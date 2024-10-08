use anyhow::Result;
use camino::{Utf8PathBuf, Utf8Path};
use crate::fetch::Fetch;
use crate::join_path_utf8;
use crate::search;
use crate::source;
use serde::Deserialize;
use serde::Serialize;
use std::fs::File;
use tracing::trace;
use std::io::Write;

pub trait Provide: scan_result::Scan + Fetch + std::fmt::Debug {}

#[derive(Debug)]
pub struct Provider {
    pub name: String,
    pub uri: String,
    pub cache_file: Utf8PathBuf,
    pub inner: Box<dyn Provide>,
}

#[derive(Serialize, Deserialize)]
pub struct ProviderFile {
    pub scan_time: chrono::DateTime<chrono::Utc>,

    #[serde(flatten)]
    pub packages: scan_result::ScanResult,
}

impl Provider {
    pub fn new(name: String, uri: String, cache_dir: &Utf8Path) -> Result<Self> {

        trace!(name, uri, "Provider::new");

        let mut cache_file = join_path_utf8!(cache_dir, "provider", &name);
        cache_file.set_extension("json");

        const FILE_PRE: &str = "file://";
        const HTTP_PRE: &str = "http://";
        const HTTPS_PRE: &str = "https://";

        let inner: Box<dyn Provide>;

        if uri.starts_with(FILE_PRE) {
            let uri = uri.strip_prefix(FILE_PRE).unwrap();
            inner = Box::new(source::filesystem::FileSystem::new(uri));
        } else if uri.starts_with(HTTP_PRE) || uri.starts_with(HTTPS_PRE) {
            inner = Box::new(source::http::Http::new(uri.clone()));
        } else {
            return Err(anyhow::anyhow!("invalid provider '{}'", uri))
        }

        Ok(Self {
            name,
            uri,
            cache_file,
            inner,
        })
    }

    pub fn load_file(&self) -> Result<ProviderFile> {
        let contents = std::fs::read_to_string(&self.cache_file)?;
        let data = serde_json::from_str::<ProviderFile>(&contents)?;
        Ok(data)
    }

    //pub fn save_file(&self, data: ProviderFile) -> Result<()> {
    //    let mut file = File::create(&self.cache_file)?;
    //    let json = serde_json::to_string_pretty(&data)?;
    //    file.write_all(json.as_bytes())?;
    //    Ok(())
    //}

    pub fn as_provide(&self) -> &dyn Provide {
        &*self.inner
    }
}

#[derive(Debug)]
pub struct ProviderFilter {
    include: Vec<String>,
    exclude: Vec<String>,
}

impl ProviderFilter {

    pub fn empty() -> ProviderFilter {
        ProviderFilter {
            include: vec![],
            exclude: vec![],
        }
    }

    pub fn from_names<T, I>(input_names: T) -> ProviderFilter
        where T: Iterator<Item=I>,
              I: AsRef<str>
    {
        let mut include = Vec::<String>::new();
        let mut exclude = Vec::<String>::new();
        for name in input_names {
            let name = name.as_ref();
            if name.starts_with('!') {
                let name = name.strip_prefix('!').unwrap();
                include.retain(|n| n != name);
                exclude.push(name.to_string());
            } else {
                exclude.retain(|n| n != name);
                include.push(name.to_string());
            }
        }
        ProviderFilter {
            include,
            exclude,
        }
    }

    pub fn included(&self, name: &str) -> bool {
        let excluded = self.exclude.iter().any(|v| v == name);
        let explicitly_included = self.include.iter().any(|v| v == name);
        let implicitly_included = self.include.is_empty();

        !excluded && (explicitly_included|| implicitly_included)
    }

    pub fn excluded(&self, name: &str) -> bool {
        !self.included(name)
    }

    pub fn filter<'a>(&'a self, providers: &'a [Provider]) -> impl Iterator<Item=&'a Provider> {
        providers.iter().filter(|p| self.included(&p.name))
    }
}
