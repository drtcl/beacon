use anyhow::Result;
use crate::fetch::Fetch;
use crate::search::Search;
use crate::join_path;
use crate::source;
use tracing::trace;
use std::path::PathBuf;
use std::path::Path;
use crate::search;

pub trait Provide: Search + Fetch + std::fmt::Debug {}

#[derive(Debug)]
pub struct Provider {
    pub name: String,
    pub uri: String,
    pub cache_file: PathBuf,
    pub inner: Box<dyn Provide>,
}

impl Provider {
    pub fn new(name: String, uri: String, cache_dir: &Path) -> Result<Self> {

        trace!(name, uri, "Provider::new");

        let mut cache_file = join_path!(cache_dir, "provider", &name);
        cache_file.set_extension("json");

        const FS_PRE: &str = "fs://";
        const HTTP_PRE: &str = "http://";
        //const HTTPS_PRE: &str = "https://";

        let inner: Box<dyn Provide>;

        if uri.starts_with(FS_PRE) {

            let uri = uri.strip_prefix(FS_PRE).unwrap();
            inner = Box::new(source::filesystem::FileSystem::new(uri));

        } else if uri.starts_with(HTTP_PRE) {

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

    pub fn load_cache(&self) -> Result<search::PackageList> {

        let contents = std::fs::read_to_string(&self.cache_file)?;
        let cached_results = serde_json::from_str::<search::PackageList>(&contents)?;
        Ok(cached_results)
    }

    pub fn as_provide(&self) -> &dyn Provide {
        &*self.inner
    }
}

//impl Fetch for Provider {
//    fn fetch(&self, write: &mut dyn Write, pkg: &PackageID, url: &str) -> Result<u64> {
//        self.inner.fetch(write, pkg, url)
//    }
//}
//
//impl Search for Provider {
//    fn search(&self, name: &str) -> Result<PackageList> {
//        self.inner.search(name)
//    }
//    fn scan(&self) -> Result<PackageList> {
//        self.inner.scan()
//    }
//}
//
//impl Provide for Provider {}
