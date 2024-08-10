use anyhow::Context;
use camino::Utf8PathBuf;
use crate::AResult;
use crate::provider::Provider;
use serde_derive::Deserialize;
use std::fs::File;
use std::io::BufReader;
use std::io::Read;
use std::path::Path;
use std::sync::OnceLock;

static CONFIG_PATH: OnceLock<Utf8PathBuf> = OnceLock::new();

pub fn store_config_path(path: Utf8PathBuf) {
    CONFIG_PATH.get_or_init(|| path);
}

pub fn get_config_path() -> Option<&'static Utf8PathBuf> {
    CONFIG_PATH.get()
}

pub fn get_config_dir() -> Option<&'static camino::Utf8Path> {
    if let Some(path) = get_config_path() {
        path.parent()
    } else {
        None
    }
}


/// the main config struct
#[derive(Debug)]
pub struct Config {
    pub cache_dir: Utf8PathBuf,
    pub cache_retention: std::time::Duration,
    pub db_file: Utf8PathBuf,
    pub providers: Vec<Provider>,
    pub mount: MountConfig,
}

#[derive(Debug)]
pub struct MountConfig {
    pub use_default_target: bool,
    pub default_target: Option<Utf8PathBuf>,
    pub mounts: Vec<(String, Utf8PathBuf)>,
}

#[derive(Debug)]
pub enum MountPoint {
    Specified(Utf8PathBuf),
    Default(Utf8PathBuf),
    DefaultDisabled,
    Invalid {
        name: String,
    }
}

#[derive(Debug, Deserialize)]
pub struct ConfigToml {
    database: String,
    use_default_target: bool,
    providers: toml::value::Table,
    mount: toml::value::Table,
    cache: CacheToml,
}

#[derive(Debug, Deserialize)]
pub struct CacheToml {
    dir: String,

    retention: String,

    #[serde(default = "bool::default")]
    auto_clear: bool,
}

impl Config {

    pub fn from_reader<R: Read>(mut read: R) -> AResult<Config> {

        let toml = {
            let mut contents = String::new();
            read.read_to_string(&mut contents)?;
            toml::from_str::<ConfigToml>(&contents).context("failed to parse config")?
        };

        let cache_dir = path_replace(toml.cache.dir)?.into_path();
        let db_file = path_replace(toml.database)?.into_path();
        let cache_retention = humantime::parse_duration(&toml.cache.retention).context("invalid cache retention")?;

        let mut mounts = Vec::new();
        for mount in toml.mount.into_iter() {
            match mount {
                (name, toml::Value::String(ref val)) => {
                    let path = path_replace(val)?;
                    if !path.path().exists() {
                        eprintln!("warning: mount '{}', path '{}' does not exist", name, path.path());
                    } else {
                        //let canon = path.canonicalize().expect("failed to canonicalize path");
                        //println!("canon {:?}", canon);
                    }
                    mounts.push((name, path.into_path()));
                },
                (name, _) => {
                    return Err(anyhow::anyhow!("invalid mount {}", name));
                }
            }
        }

        let mut mount = MountConfig {
            use_default_target: toml.use_default_target,
            default_target: None,
            mounts,
        };
        // extract the TARGET mount if it was defined
        if let Some(idx) = mount.mounts.iter().position(|e| e.0 == "TARGET") {
            let (_name, path) = mount.mounts.remove(idx);
            mount.default_target = Some(path);
        }

        let mut providers: Vec<Provider> = Vec::new();
        for p in toml.providers.into_iter() {
            match p {
                (name, toml::Value::String(ref uri)) => {
                    let uri = provider_replace(uri)?;
                    let provider = Provider::new(name, uri, &cache_dir)?;
                    providers.push(provider);
                }
                (name, toml::Value::Array(vals)) => {
                    for val in vals {
                        let uri = val.as_str();
                        if uri.is_none() {
                            return Err(anyhow::anyhow!("invalid provider {}", name));
                        }
                        let uri = uri.unwrap();
                        let uri = provider_replace(uri)?;
                        let provider = Provider::new(name.clone(), uri, &cache_dir)?;
                        providers.push(provider);
                    }
                }
                (name, _) => {
                    return Err(anyhow::anyhow!("invalid provider {}", name));
                }
            }
        }

        let config = Config {
            cache_dir,
            db_file,
            providers,
            mount,
            cache_retention,
        };

        //dbg!(&config);

        Ok(config)
    }

    pub fn from_path<P: AsRef<Path>>(path: P) -> AResult<Config> {
        let read = BufReader::new(File::open(path.as_ref())?);
        Self::from_reader(read)
    }

    pub fn get_mountpoint(&self, name: Option<&str>) -> MountPoint {
        match name {
            None => {
                if !self.mount.use_default_target {
                    MountPoint::DefaultDisabled
                } else {
                    match &self.mount.default_target {
                        Some(path) => MountPoint::Default(path.clone()),
                        None => MountPoint::Invalid{ name: "TARGET".into() },
                    }
                }
            },
            Some(name) => {
                if name == "TARGET" {
                    if !self.mount.use_default_target {
                        MountPoint::DefaultDisabled
                    } else {
                        match &self.mount.default_target {
                            Some(path) => MountPoint::Specified(path.clone()),
                            None => MountPoint::Invalid{ name: "TARGET".into() },
                        }
                    }
                } else {
                    for (n, path) in &self.mount.mounts {
                        if name == n {
                            return MountPoint::Specified(path.clone());
                        }
                    }
                    MountPoint::Invalid{name: name.to_string()}
                }
            }
        }
    }
}

#[derive(Debug)]
enum PathType {
    Relative{
        path: Utf8PathBuf,
        relative_to: Utf8PathBuf,
    },
    Absolute(Utf8PathBuf),
}

impl PathType {
    fn path(&self) -> &Utf8PathBuf {
        match self {
            Self::Relative{path, ..} => path,
            Self::Absolute(path) => path,
        }
    }
    fn into_path(self) -> Utf8PathBuf {
        match self {
            Self::Relative{path, ..} => path,
            Self::Absolute(path) => path,
        }
    }
}


/// replace variables within a path
/// - `${BPM}` => dir of bpm binary, must be at the start of the path
/// - `${THIS}` => dir of the config file, must be at the start of the path
/// - and anything replaced by config_replace()
fn path_replace<S: Into<String>>(path: S) -> anyhow::Result<PathType> {

    let mut path = path.into();
    let mut relative = None;

    // ${BPM} must be the first thing in the string
    if path.starts_with("${BPM}") {
        let cur_exe = std::env::current_exe().expect("failed to get current exe path");
        let exe_dir = Utf8PathBuf::from(cur_exe.parent().unwrap().to_str().context("invalid path, not utf8")?);
        path = path.replacen("${BPM}", exe_dir.as_str(), 1);
        relative = Some(exe_dir);
    }

    if path.contains("${BPM}") {
        // more ${BPM} found in the middle of the path
        anyhow::bail!("Invalid ${{BPM}} substitution: '{path}'");
    }

    // ${THIS} must be the first thing in the string
    if path.starts_with("${THIS}") {
        if let Some(dir) = get_config_dir() && dir.is_absolute() {
            path = path.replacen("${THIS}", dir.as_str(), 1);
            relative = Some(dir.to_path_buf());
        } else {
            anyhow::bail!("Cannot substitute ${{THIS}} when config path is unknown");
        }
    }

    if path.contains("${THIS}") {
        // more ${THIS} found in the middle of the path
        anyhow::bail!("Invalid ${{THIS}} substitution: '{path}'");
    }

    config_replace(&mut path);

    let path = Utf8PathBuf::from(path);
    if path.is_relative() {
        anyhow::bail!("Invalid naked relative path '{path}' in config file");
    }

    if let Some(relative) = relative {
        Ok(PathType::Relative{
            path: path.into(),
            relative_to: relative,
        })
    } else {
        Ok(PathType::Absolute(path.into()))
    }
}

/// replace variables within a path
/// - `${OS}` => "linux", "windows", "darwin", "wasm", "unix", "unknown"
/// - `${ARCH3264}` => "64" or "32"
/// - `${POINTER_WIDTH}` => "64" or "32"
/// - `${ARCHX8664}` => "x86" or "x86_64"
fn config_replace(path: &mut String) {

    if path.contains("${OS}") {

        let os = if cfg!(target_os="windows") {
            "windows"
        } else if cfg!(target_os="linux") {
            "linux"
        } else if cfg!(target_os="macos") {
            "darwin"
        } else if cfg!(target_family="wasm") {
            "wasm"
        } else if cfg!(unix) {
            "unix"
        } else {
            "unknown"
        };

        *path = path.replace("${OS}", os);
    }

    if path.contains("${ARCH3264}") {

        let arch = if cfg!(target_pointer_width="32") {
            "32"
        } else if cfg!(target_pointer_width="64") {
            "64"
        } else {
            panic!("unhandled architecture");
        };

        *path = path.replace("${ARCH3264}", arch);
    }

    if path.contains("${POINTER_WIDTH}") {

        let arch = if cfg!(target_pointer_width="32") {
            "32"
        } else if cfg!(target_pointer_width="64") {
            "64"
        } else {
            panic!("unhandled pointer width (not 32 or 64)");
        };

        *path = path.replace("${POINTER_WIDTH}", arch);
    }

    if path.contains("${ARCHX8664}") {

        let arch = if cfg!(target_arch="x86") {
            "x86"
        } else if cfg!(target_arch="x86_64") {
            "x86_64"
        } else if cfg!(target_arch="arm") {
            "arm"
        } else if cfg!(target_arch="aarch64") {
            "aarch64"
        } else {
            panic!("unhandled architecture");
        };

        *path = path.replace("${ARCHX8664}", arch);
    }
}

fn provider_replace(uri: &str) -> anyhow::Result<String> {

    if uri.starts_with("file://") {

        let path = uri.strip_prefix("file://").unwrap();
        let path = path_replace(path)?.into_path();
        return Ok(format!("file://{path}"));

    } else if uri.starts_with("http://") || uri.starts_with("https://") {

        let mut uri = String::from(uri);
        config_replace(&mut uri);
        return Ok(uri);

    } else {

        eprintln!("warning: unrecognized provider type");
        let mut uri = String::from(uri);
        config_replace(&mut uri);
        return Ok(uri);
    }
}
