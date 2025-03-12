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
    pub inner: Box<dyn Provide + Sync + Send>,
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

        let inner: Box<dyn Provide + Sync + Send>;

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

///
/// `!foo` can be used to exclude "foo"
/// `*` can be used to mention all providers that aren't named elsewhere
/// `:ordered` can be included to use the ordering from the input rather than the config file
///
///  example:
///     `:ordered,foo,*,bar,!baz" => All providers except baz. Use foo first, then any others not expclicited named, then bar last.
///
#[derive(Debug)]
pub struct ProviderFilter {
    include: Vec<String>,
    exclude: Vec<String>,
    ordered: bool,
    rest: Option<usize>,
}

impl ProviderFilter {

    pub fn empty() -> ProviderFilter {
        ProviderFilter {
            include: vec![],
            exclude: vec![],
            ordered: false,
            rest: None,
        }
    }

    pub fn from_names<T, I>(input_names: T) -> ProviderFilter
        where T: Iterator<Item=I>,
              I: AsRef<str>
    {
        let mut include = Vec::<String>::new();
        let mut exclude = Vec::<String>::new();
        let mut ordered = false;
        let mut rest = None;
        let mut add_count = 0;
        for name in input_names {
            let name = name.as_ref();
            if name == ":ordered" {
                ordered = true
            } else if name == "*" {
                if rest.is_none() {
                    rest = Some(add_count);
                }
            } else if name.starts_with('!') {
                let name = name.strip_prefix('!').unwrap();
                include.retain(|n| n != name);
                exclude.push(name.to_string());
            } else {
                add_count += 1;
                exclude.retain(|n| n != name);
                include.push(name.to_string());
            }
        }
        ProviderFilter {
            include,
            exclude,
            ordered,
            rest,
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

    //// this is non-ordered filtering
    //pub fn filter<'a>(&'a self, providers: &'a [Provider]) -> impl Iterator<Item=&'a Provider> {
        //providers.iter().filter(|p| self.included(&p.name))
    //}

    pub fn filter<'a>(&'a self, providers: &'a [Provider]) -> impl Iterator<Item=&'a Provider> {
        self.get_list(providers).into_iter()
    }

    pub fn get_list<'a>(&self, providers: &'a [Provider]) -> Vec<&'a Provider> {

        let mut ret = Vec::new();
        let all = self.rest.is_some() || self.include.is_empty();

        if !self.ordered {

            // not ordered
            // simply add all while filtering out excluded ones

            for p in providers {
                if self.exclude.contains(&p.name) {
                    // excluded
                } else if all || self.include.contains(&p.name) {
                    ret.push(p);
                }
            }

        } else {

            // ordered
            // run through the ordering, adding what we find
            // and insert the "rest" afterward

            for name in &self.include {
                for p in providers {
                    if p.name == *name {
                        if ret.iter().any(|v| v.name == p.name) {
                            // already added
                        } else {
                            ret.push(p);
                        }
                    }
                }
            }

            if let Some(mut idx) = self.rest {
                for p in providers {
                    if self.exclude.contains(&p.name) {
                        // excluded
                    } else if ret.iter().any(|v| v.name == p.name) {
                        // already added
                    } else {
                        ret.insert(idx, p);
                        idx += 1;
                    }
                }
            }
        }

        ret
    }
}

#[cfg(test)]
mod test {

    use super::*;

    #[derive(Debug)]
    struct FakeProvider {}
    impl Provide for FakeProvider {}
    impl Fetch for FakeProvider {
        fn fetch(&self, write: &mut dyn Write, pkg: &package::PackageID, url: &str, bars: Option<&indicatif::MultiProgress>) -> Result<u64> {
            unreachable!()
        }
    }
    impl scan_result::Scan for FakeProvider {
        fn scan(&self, _arch: Option<&[&str]>) -> anyhow::Result<scan_result::ScanResult> {
            unreachable!()
        }
    }
    impl FakeProvider {
        fn make(name: &str) -> Provider {
            Provider {
                name: name.into(),
                uri: "".into(),
                cache_file: "".into(),
                inner: Box::new(FakeProvider{}),
            }
        }
    }

    #[test]
    fn provider_list() {
        let t1 = FakeProvider::make("test1");
        let t2 = FakeProvider::make("test2");
        let t3 = FakeProvider::make("test3");
        let t4 = FakeProvider::make("test4");
        let t5 = FakeProvider::make("test5");
        let providers = vec![t1, t2, t3, t4, t5];


        let input = "test3";
        let filter = ProviderFilter::from_names(input.split(','));
        let list = filter.get_list(&providers);
        let mut iter = list.iter().map(|v| v.name.as_str());
        assert_eq!(iter.next(), Some("test3"));
        assert_eq!(iter.next(), None);

        let input = "!test2";
        let filter = ProviderFilter::from_names(input.split(','));
        let list = filter.get_list(&providers);
        let mut iter = list.iter().map(|v| v.name.as_str());
        assert_eq!(iter.next(), Some("test1"));
        assert_eq!(iter.next(), Some("test3"));
        assert_eq!(iter.next(), Some("test4"));
        assert_eq!(iter.next(), Some("test5"));
        assert_eq!(iter.next(), None);

        let input = "test3,test1,!test2";
        let filter = ProviderFilter::from_names(input.split(','));
        let list = filter.get_list(&providers);
        let mut iter = list.iter().map(|v| v.name.as_str());
        assert_eq!(iter.next(), Some("test1"));
        assert_eq!(iter.next(), Some("test3"));
        assert_eq!(iter.next(), None);

        let input = "test3,*,test1,!test2";
        let filter = ProviderFilter::from_names(input.split(','));
        let list = filter.get_list(&providers);
        let mut iter = list.iter().map(|v| v.name.as_str());
        assert_eq!(iter.next(), Some("test1"));
        assert_eq!(iter.next(), Some("test3"));
        assert_eq!(iter.next(), Some("test4"));
        assert_eq!(iter.next(), Some("test5"));
        assert_eq!(iter.next(), None);

        let input = ":ordered,test3,test1,!test2";
        let filter = ProviderFilter::from_names(input.split(','));
        let list = filter.get_list(&providers);
        let mut iter = list.iter().map(|v| v.name.as_str());
        assert_eq!(iter.next(), Some("test3"));
        assert_eq!(iter.next(), Some("test1"));
        assert_eq!(iter.next(), None);

        // :ordered can be in the middle
        let input = "test3,:ordered,test1,!test2";
        let filter = ProviderFilter::from_names(input.split(','));
        let list = filter.get_list(&providers);
        let mut iter = list.iter().map(|v| v.name.as_str());
        assert_eq!(iter.next(), Some("test3"));
        assert_eq!(iter.next(), Some("test1"));
        assert_eq!(iter.next(), None);

        // :ordered can be at the end
        let input = "test3,test1,!test2,:ordered";
        let filter = ProviderFilter::from_names(input.split(','));
        let list = filter.get_list(&providers);
        let mut iter = list.iter().map(|v| v.name.as_str());
        assert_eq!(iter.next(), Some("test3"));
        assert_eq!(iter.next(), Some("test1"));
        assert_eq!(iter.next(), None);

        let input = ":ordered,test3,*,test1,!test2";
        let filter = ProviderFilter::from_names(input.split(','));
        let list = filter.get_list(&providers);
        let mut iter = list.iter().map(|v| v.name.as_str());
        assert_eq!(iter.next(), Some("test3"));
        assert_eq!(iter.next(), Some("test4"));
        assert_eq!(iter.next(), Some("test5"));
        assert_eq!(iter.next(), Some("test1"));
        assert_eq!(iter.next(), None);

        let input = ":ordered,!test2,!test5,test3,*,test1";
        let filter = ProviderFilter::from_names(input.split(','));
        let list = filter.get_list(&providers);
        let mut iter = list.iter().map(|v| v.name.as_str());
        assert_eq!(iter.next(), Some("test3"));
        assert_eq!(iter.next(), Some("test4"));
        assert_eq!(iter.next(), Some("test1"));
        assert_eq!(iter.next(), None);

        let input = ":ordered,!test2,!test5,test3,test1,*,!test4";
        let filter = ProviderFilter::from_names(input.split(','));
        let list = filter.get_list(&providers);
        let mut iter = list.iter().map(|v| v.name.as_str());
        assert_eq!(iter.next(), Some("test3"));
        assert_eq!(iter.next(), Some("test1"));
        assert_eq!(iter.next(), None);

        let input = "*";
        let filter = ProviderFilter::from_names(input.split(','));
        let list = filter.get_list(&providers);
        let mut iter = list.iter().map(|v| v.name.as_str());
        assert_eq!(iter.next(), Some("test1"));
        assert_eq!(iter.next(), Some("test2"));
        assert_eq!(iter.next(), Some("test3"));
        assert_eq!(iter.next(), Some("test4"));
        assert_eq!(iter.next(), Some("test5"));
        assert_eq!(iter.next(), None);

        // additional mentions of "*" are ignored
        let input = ":ordered,test3,*,test1,*";
        let filter = ProviderFilter::from_names(input.split(','));
        let list = filter.get_list(&providers);
        let mut iter = list.iter().map(|v| v.name.as_str());
        assert_eq!(iter.next(), Some("test3"));
        assert_eq!(iter.next(), Some("test2"));
        assert_eq!(iter.next(), Some("test4"));
        assert_eq!(iter.next(), Some("test5"));
        assert_eq!(iter.next(), Some("test1"));
        assert_eq!(iter.next(), None);

        let input = ":ordered,test2,test2";
        let filter = ProviderFilter::from_names(input.split(','));
        let list = filter.get_list(&providers);
        let mut iter = list.iter().map(|v| v.name.as_str());
        assert_eq!(iter.next(), Some("test2"));
        assert_eq!(iter.next(), None);

        let input = "test2,test2";
        let filter = ProviderFilter::from_names(input.split(','));
        let list = filter.get_list(&providers);
        let mut iter = list.iter().map(|v| v.name.as_str());
        assert_eq!(iter.next(), Some("test2"));
        assert_eq!(iter.next(), None);

    }
}
