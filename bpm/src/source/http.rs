use crate::AResult;
use crate::fetch::*;
use crate::package;
use crate::PackageID;
use crate::provider::Provide;
use crate::search::*;
use httpsearch;
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

impl Search for Http {
    fn search(&self, pkg_name: &str) -> SearchResults {
        println!("-- [Search] Http::search '{pkg_name}' -- ");

        let client = httpsearch::Client::new();
        let names = httpsearch::get_package_names_all(&client, &self.url);

        let mut results = SearchResults::new();

        if let Ok(names) = names {
            for name in names {
                if name.contains(pkg_name) {
                    let versions = httpsearch::get_package_versions_all(&client, &self.url, &name);
                    if let Ok(versions) = versions {
                        results.inner.insert(
                            (&name).into(),
                            SearchResult {
                                name: (&name).into(),
                                versions: versions
                                    .iter()
                                    .filter_map(|s| semver::Version::parse(s).ok())
                                    .collect(),
                            },
                        );
                    }
                }
            }
        }

        results
    }
}

impl Fetch for Http {
    fn fetch(&self, pkg: &PackageID, write: &mut dyn Write) -> AResult<u64> {
        let filename = package::to_filename(&pkg.name, &pkg.version);
        let url = format!(
            "{}/{}/{}.{}.{}/{}",
            &self.url, &pkg.name, pkg.version.major, pkg.version.minor, pkg.version.patch, filename
        );
        dbg!(&url);

        let client = httpsearch::Client::new();
        let n = httpsearch::download(&client, &url, write)?;
        Ok(n)
    }
}

impl Provide for Http {}
