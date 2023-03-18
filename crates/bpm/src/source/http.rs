use crate::AResult;
use crate::fetch::*;
use crate::provider::Provide;
use crate::search::*;
use httpsearch;
use package::PackageID;
use serde_derive::Serialize;
use std::io::Write;

#[derive(Debug, Serialize)]
pub struct Http {
    url: String,
}

impl Http {
    pub fn new(url: String) -> Self {
        Http { url }
    }
}

//impl Search for Http {
//    fn search(&self, needle: &str) -> SearchResults {
//        tracing::debug!("-- [Search] Http::search '{needle}' -- ");
//
//        let client = httpsearch::Client::new();
//        let names = httpsearch::get_package_names_all(&client, &self.url);
//
//        let mut results = SearchResults::new();
//
//        if let Ok(names) = names {
//            for name in names {
//                if name.contains(needle) {
//                    let versions = httpsearch::get_package_versions_all(&client, &self.url, &name);
//                    if let Ok(versions) = versions {
//
//                        let prefix = format!("{name}-");
//                        let suffix = format!(".{}", package::PKG_FILE_EXTENSION);
//
//                        let versions: Vec<_> = versions.iter().map(|v| {
//                            let mut v = v.as_str();
//                            if v.starts_with(&prefix) {
//                                v = &v[prefix.len()..];
//                            }
//                            if v.ends_with(&suffix) {
//                                v = v.strip_suffix(&suffix).unwrap();
//                            }
//                            v.to_string()
//                        }).collect();
//
//                        results.inner.insert(
//                            (&name).into(),
//                            SearchResult {
//                                name: (&name).into(),
//                                versions: versions
//                                    .iter()
//                                    .filter_map(|s| semver::Version::parse(s).ok())
//                                    .collect(),
//                            },
//                        );
//                    }
//                }
//            }
//        }
//
//        //dbg!(&results);
//        results
//    }
//}

impl From<httpsearch::VersionInfo> for VersionInfo {
    fn from(value: httpsearch::VersionInfo) -> Self {
        Self {
            url: value.url,
            filename: value.filename,
            channels: value.channels,
        }
    }
}

impl Search for Http {
    fn search(&self, needle: &str) -> AResult<PackageList> {
        tracing::debug!("[Search] Http::search '{needle}'");

        let mut pkgs = httpsearch::full_scan(None, &self.url, None)?;

        pkgs.retain(|name, _versions| {
            name.contains(needle)
        });

        let mut ret = PackageList::new();
        for (name, versions) in pkgs {
            for (version, urlfilename) in versions {
                ret.entry(name.to_string())
                    .or_default()
                    .insert(version, VersionInfo::from(urlfilename));
            }
        }

        Ok(ret)
    }

    fn scan(&self) -> AResult<PackageList> {

        let pkgs = httpsearch::full_scan(None, &self.url, None)?;

        let mut ret = PackageList::new();
        for (name, versions) in pkgs {
            for (version, urlfilename) in versions {
                ret.entry(name.to_string())
                    .or_default()
                    .insert(version, VersionInfo::from(urlfilename));
            }
        }
        Ok(ret)
    }
}

impl Fetch for Http {
    fn fetch(&self, write: &mut dyn Write, pkg: &PackageID, url: &str) -> AResult<u64> {

        tracing::trace!(pkg=?pkg, url, "Http::fetch()");

        //let client = httpsearch::Client::new();
        let n = httpsearch::download(None, url, write)?;
        Ok(n)
    }
}

impl Provide for Http {}
