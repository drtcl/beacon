use anyhow::Context;
use crate::AResult;
use crate::provider::Provide;
use crate::source;
use serde_derive::Deserialize;
use serde_derive::Serialize;
use std::io::Read;
use std::path::PathBuf;

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
    pub providers: Vec<(String, Provider)>,
    pub db_file: PathBuf,
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
    cache_dir: PathBuf,
    providers: toml::value::Table,
    database: PathBuf,
}

pub fn parse(path: &str) -> AResult<Config> {
    let mut file = std::io::BufReader::new(std::fs::File::open(path)?);
    let mut contents = String::new();
    file.read_to_string(&mut contents)?;
    let toml = toml::from_str::<ConfigToml>(&contents).context("failed to parse toml config file")?;

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
        cache_dir: toml.cache_dir,
        db_file: toml.database,
        providers,
    };

    Ok(config)
}
