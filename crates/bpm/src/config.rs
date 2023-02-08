use anyhow::Context;
use crate::AResult;
use crate::provider::Provider;
use serde_derive::Deserialize;
use std::io::Read;
use std::path::{PathBuf, Path};
use std::fs::File;
use std::io::BufReader;

/// the main config struct
#[derive(Debug)]
pub struct Config {
    pub cache_dir: PathBuf,
    pub db_file: PathBuf,
    pub providers: Vec<Provider>,
    pub mount: MountConfig,
}

#[derive(Debug)]
pub struct MountConfig {
    pub use_default_target: bool,
    pub default_target: Option<PathBuf>,
    pub mounts: Vec<(String, PathBuf)>,
}

#[derive(Debug)]
pub enum MountPoint {
    Specified(PathBuf),
    Default(PathBuf),
    DefaultDisabled,
    Invalid {
        name: String,
    }
}

#[derive(Debug, Deserialize)]
pub struct ConfigToml {
    cache_dir: String,
    database: String,
    use_default_target: bool,
    providers: toml::value::Table,
    mount: toml::value::Table,
}

impl Config {

    pub fn from_reader<R: Read>(mut read: R) -> AResult<Config> {

        let toml = {
            let mut contents = String::new();
            read.read_to_string(&mut contents)?;
            toml::from_str::<ConfigToml>(&contents).context("failed to parse config.toml")?
        };

        let cache_dir = PathBuf::from(path_replace(toml.cache_dir));
        let db_file = PathBuf::from(path_replace(toml.database));
        //let use_default_target = toml.

        let mut mounts = Vec::new();
        for mount in toml.mount.into_iter() {
            match mount {
                (name, toml::Value::String(ref val)) => {
                    let path = PathBuf::from(path_replace(val));
                    if !path.exists() {
                        eprintln!("warning: mount '{}', path '{}' does not exist", name, path.display());
                    } else {
                        //let canon = path.canonicalize().expect("failed to canonicalize path");
                        //println!("canon {:?}", canon);
                    }
                    mounts.push((name, path));
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
                    let uri = path_replace(uri);
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
                        let uri = path_replace(uri);
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

/// replace variables within a path
/// - `${BPM}` => dir of bpm binary
fn path_replace<S: Into<String>>(path: S) -> String {

    let mut path = path.into();

    if path.contains("${BPM}") {
        let cur_exe = std::env::current_exe().expect("failed to get current exe path");
        let exe_dir = cur_exe.parent().unwrap().to_str().unwrap();
        path = path.replace("${BPM}", exe_dir);
    }

    if path.contains("${OS}") {

        let os = if cfg!(target_os="windows") {
            "windows"
        } else if cfg!(target_os="linux") {
            "linux"
        } else if cfg!(unix) {
            "unix"
        } else if cfg!(target_family="wasm") {
            "wasm"
        } else {
            "unknown"
        };

        path = path.replace("${OS}", os);
    }

    if path.contains("${ARCH3264}") {

        let arch = if cfg!(target_arch="x86") {
            "32"
        } else if cfg!(target_arch="x86_64") {
            "64"
        } else {
            panic!("unhandled architecture");
        };

        path = path.replace("${ARCH3264}", arch);
    }

    if path.contains("${ARCHX8664}") {

        let arch = if cfg!(target_arch="x86") {
            "x86"
        } else if cfg!(target_arch="x86_64") {
            "x86_64"
        } else {
            panic!("unhandled architecture");
        };

        path = path.replace("${ARCHX8664}", arch);
    }

    path

    //PathBuf::from(path)
}
