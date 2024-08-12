use anyhow::Context;
use camino::Utf8PathBuf;
use crate::AResult;
use crate::provider::Provider;
use serde::{Serialize, Deserialize};
use std::borrow::Borrow;
use std::collections::HashMap;
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

    /// name of default mount
    pub default_target: Option<String>,

    /// [name, path]
    pub mounts: Vec<(String, PathType)>,
}

#[derive(Debug)]
pub enum MountPoint {
    Specified(PathType),
    Default(PathType),
    DefaultDisabled,
    Invalid {
        name: String,
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RelativeToAnchor {
    Bpm,
    Config,
}

impl RelativeToAnchor {
    pub fn path(&self) -> Option<Utf8PathBuf> {
        match self {
            Self::Bpm => {
                std::env::current_exe()
                    .map(|p| p.parent().map(Path::to_path_buf))
                    .ok()
                    .flatten()
                    .and_then(|p| Utf8PathBuf::from_path_buf(p).ok())

            },
            Self::Config => {
                get_config_dir().map(ToOwned::to_owned)
            }
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum PathType {
    Relative{
        path: Utf8PathBuf,
        relative_to: RelativeToAnchor,
    },
    #[serde(untagged)]
    Absolute(Utf8PathBuf),
}

impl PathType {
    pub fn full_path(&self) -> anyhow::Result<Utf8PathBuf> {
        match self {
            Self::Relative{path, relative_to} => {
                if let Some(rel) = relative_to.path() {
                    Ok(crate::join_path_utf8!(rel, path))
                } else {
                    anyhow::bail!("No relative path anchor")
                }
            }
            Self::Absolute(path) => Ok(path.to_owned())
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct ConfigToml {
    database: String,
    providers: toml::value::Table,
    mount: HashMap<String, MountToml>,
    cache: CacheToml,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum MountToml {
    JustPath(String),
    Table {
        default: bool,
        path: String,
    },
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

        let cache_dir = path_replace(toml.cache.dir)?.full_path()?;
        let db_file = path_replace(toml.database)?.full_path()?;
        let cache_retention = humantime::parse_duration(&toml.cache.retention).context("invalid cache retention")?;

        let mut mounts = Vec::new();
        let mut default_target = None;
        for (name, mount) in toml.mount {

            let (path, is_default) = match mount {
                MountToml::JustPath(path) => (path, false),
                MountToml::Table { default, path } => (path, default),
            };

            let path = path_replace(path)?;

            if is_default {
                if default_target.is_some() {
                    anyhow::bail!("mutliple default mount points");
                }
                default_target = Some(name.clone());
            }

            let full_path = path.full_path()?;
            if !full_path.exists() {
                eprintln!("warning: mount '{}', path '{}' does not exist", name, full_path);
            }

            mounts.push((name, path));
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
            cache_retention,
            db_file,
            providers,
            mount : MountConfig {
                default_target,
                mounts,
            },
        };

        //dbg!(&config);

        Ok(config)
    }

    pub fn from_path<P: AsRef<Path>>(path: P) -> AResult<Config> {
        let read = BufReader::new(File::open(path.as_ref())?);
        Self::from_reader(read)
    }

    /// Given a name (from a package definition) find
    /// and return the mount point from the config
    pub fn get_mountpoint(&self, name: Option<&str>) -> MountPoint {
        match name {
            None => {
                if let Some(name) = &self.mount.default_target {
                    for (n, path) in &self.mount.mounts {
                        if name == n {
                            return MountPoint::Default(path.clone());
                        }
                    }
                }
                MountPoint::DefaultDisabled
            },
            Some(name) => {
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

/// replace variables within a path
/// - `${BPM}` => dir of bpm binary, must be at the start of the path
/// - `${THIS}` => dir of the config file, must be at the start of the path
/// - and anything replaced by config_replace()
fn path_replace<S: Into<String>>(path: S) -> anyhow::Result<PathType> {

    let mut path : String = path.into();
    let mut relative = None;

    // ${BPM} must be the first thing in the string
    if path.starts_with("${BPM}/") {
        let cur_exe = std::env::current_exe().expect("failed to get current exe path");
        let exe_dir = Utf8PathBuf::from(cur_exe.parent().unwrap().to_str().context("invalid path, not utf8")?);
        path = path.strip_prefix("${BPM}/").unwrap().into();
        relative = Some(RelativeToAnchor::Bpm);
    }

    if path.contains("${BPM}") {
        // more ${BPM} found in the middle of the path
        anyhow::bail!("Invalid ${{BPM}} substitution: '{path}'");
    }

    // ${THIS} must be the first thing in the string
    if path.starts_with("${THIS}/") {
        if let Some(dir) = get_config_dir() && dir.is_absolute() {
            path = path.strip_prefix("${THIS}/").unwrap().into();
            relative = Some(RelativeToAnchor::Config);
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
    if relative.is_none() && path.is_relative() {
        anyhow::bail!("Invalid naked relative path '{path}' in config file");
    }

    if let Some(relative) = relative {
        Ok(PathType::Relative{
            path,
            relative_to: relative,
        })
    } else {
        Ok(PathType::Absolute(path))
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
        let path = path_replace(path)?.full_path()?;
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
