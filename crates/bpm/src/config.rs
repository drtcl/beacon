use anyhow::Context;
use crate::AResult;
use crate::provider::Provide;
use crate::source;
use serde_derive::Deserialize;
use serde_derive::Serialize;
use std::io::Read;
use std::path::{PathBuf, Path};

#[derive(Debug, Serialize)]
pub enum Provider {
    #[serde(serialize_with = "serialize_filesystem")]
    FileSystem(source::filesystem::FileSystem),
    //#[serde(serialize_with = "serialize_http")]
    Http(source::http::Http),
}

#[derive(Debug, Serialize)]
pub struct Config {
    pub cache_dir: PathBuf,
    pub db_file: PathBuf,
    pub use_default_target: bool,
    pub providers: Vec<(String, Provider)>,
    pub mount: Vec<(String, PathBuf)>,
}

impl Config {
    pub fn get_mountpoint(&self, name: &str) -> Option<PathBuf> {
        for (n, p) in &self.mount {
            if name == n {
                return Some(p.clone());
            }
        }
        return None;
    }
}

fn serialize_filesystem<S: serde::Serializer>(
    _fs: &source::filesystem::FileSystem,
    s: S,
) -> Result<S::Ok, S::Error> {
    s.serialize_str("")
}

pub fn write_config<W: std::io::Write>(config: &Config, to: &mut W) -> AResult<()> {
    write!(to, "{}", toml::to_string(&config)?)?;
    Ok(())
}

impl Provider {
    fn from_uri(uri: &str) -> AResult<Self> {
        const FS_PRE: &str = "fs://";
        const HTTP_PRE: &str = "http://";
        //const HTTPS_PRE: &str = "https://";

        if uri.starts_with(FS_PRE) {
            let uri = uri.strip_prefix(FS_PRE).unwrap();
            return Ok(Provider::FileSystem(source::filesystem::FileSystem::new(
                uri,
            )));
        }

        if uri.starts_with(HTTP_PRE) {
            return Ok(Provider::Http(source::http::Http::new(uri.into())));
        }

        //if uri.starts_with(HTTPS_PRE) {
            //println!("new https source {}", &uri);
            //let uri = uri.strip_prefix(HTTPS_PRE).unwrap();
        //}

        return Err(anyhow::anyhow!("invalid provider '{}'", uri));

        //let mut split = uri.split(':');
        //let count = split.clone().count();
        //match (count, split.next()) {
        //    (1, _) => {
        //        return Err(anyhow::anyhow!("?"));
        //    }
        //    (_, Some(scheme)) => {
        //        return Err(anyhow::anyhow!("invalid provider scheme '{}'", scheme));
        //    }
        //    (_, None) => {
        //        return Err(anyhow::anyhow!("invalid provider, missing scheme. Example: fs://"));
        //    },
        //}
    }

    pub(crate) fn as_provide(&self) -> &dyn Provide {
        match self {
            Provider::FileSystem(inner) => inner,
            Provider::Http(inner) => inner,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename = "bpm")]
pub struct ConfigToml {
    cache_dir: String,
    database: String,
    use_default_target: bool,
    providers: toml::value::Table,
    mount: toml::value::Table,
}

pub fn parse_path<P: AsRef<Path>>(path: P) -> AResult<Config> {
    let file = std::fs::File::open(path.as_ref())?;
    let r = std::io::BufReader::new(file);
    parse_read(r)
}

pub fn parse_read<R: Read>(mut read: R) -> AResult<Config> {
    let mut contents = String::new();
    read.read_to_string(&mut contents)?;
    let toml = toml::from_str::<ConfigToml>(&contents).context("failed to parse config.toml")?;

    let cache_dir = path_replace(toml.cache_dir);
    let db_file = path_replace(toml.database);
    //let use_default_target = toml.

    let mut mounts = Vec::new();
    for mount in toml.mount.into_iter() {
        match mount {
            (name, toml::Value::String(ref val)) => {
                let path = path_replace(val);
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

    let mut providers: Vec<(String, Provider)> = Vec::new();
    for p in toml.providers.into_iter() {
        match p {
            (name, toml::Value::String(ref uri)) => {
                providers.push((name, Provider::from_uri(uri)?))
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
        mount: mounts,
        use_default_target: toml.use_default_target,
    };

    //dbg!(&config);

    Ok(config)
}

/// replace variables within a path
/// - `${BPM}` => dir of bpm binary
fn path_replace<S: Into<String>>(path: S) -> PathBuf {

    let mut path = path.into();

    if path.find("${BPM}").is_some() {
        let cur_exe = std::env::current_exe().expect("failed to get current exe path");
        let exe_dir = cur_exe.parent().unwrap().to_str().unwrap();
        path = path.replace("${BPM}", exe_dir);
    }

    PathBuf::from(path)
}
