use crate::AResult;
use crate::fetch::*;
use crate::provider::Provide;
use crate::search::*;
use itertools::Itertools;
use std::io::Write;
use std::path::{Path, PathBuf};

use package::PackageID;

#[derive(Debug)]
pub struct FileSystem {
    root: PathBuf,
}

#[derive(Debug)]
struct FileSystemPackage {
    name: String,
    versions: Vec<semver::Version>,
    //path: PathBuf,
}

impl FileSystem {
    pub fn new<P: AsRef<Path>>(path: P) -> Self {
        FileSystem {
            root: PathBuf::from(path.as_ref()),
        }
    }

    fn get_versions(&self, pkg_name: &str) -> AResult<Vec<semver::Version>> {
        let mut pkg_dir = PathBuf::new();
        pkg_dir.push(&self.root);
        pkg_dir.push(pkg_name);

        let versions = std::fs::read_dir(&pkg_dir)?
            .filter_map(|x| x.ok().map(|y| y.file_name()))
            .filter_map(|x| x.into_string().ok())
            .filter_map(|x| semver::Version::parse(&x).ok())
            .sorted()
            .rev()
            .collect();

        Ok(versions)
    }

    fn search(&self, name: &str) -> AResult<Vec<FileSystemPackage>> {
        //println!("FileSystem::search {}", name);

        //let mut results = SearchResults::new();
        let mut results = Vec::new();

        //println!("searching {:?} for {}", self.root, name);

        for entry in std::fs::read_dir(&self.root)? {
            //println!("entry {:?}", entry);

            if let Ok(entry) = entry {
                if let Ok(filename) = entry.file_name().into_string() {
                    //println!("-- {:?}", &filename);
                    if filename.contains(name) {
                        //results.packages.push(SearchResult{name:filename});
                        if let Ok(versions) = self.get_versions(&filename) {
                            //println!("found versions {:#?}", versions.iter().map(|x| format!("{}", x)).collect::<Vec<String>>());
                            results.push(FileSystemPackage {
                                name: filename,
                                versions,
                            });
                        }
                    }
                }
            }
        }

        return Ok(results);
    }
}

impl Search for FileSystem {
    fn search(&self, name: &str) -> SearchResults {
        //println!("FileSystem::[Search]::search {}", name);

        let results = self.search(name);

        if let Ok(results) = results {
            return SearchResults {
                inner: results
                    .into_iter()
                    .map(|result| {
                        (
                            result.name.clone(),
                            SearchResult {
                                name: result.name,
                                versions: result.versions.into_iter().collect(),
                                //uri: String::new(),
                            },
                        )
                    })
                    .collect(),
            };
        }

        return SearchResults::new();
    }
}

impl Fetch for FileSystem {

    fn fetch(&self, _pkg: &PackageID, _write: &mut dyn Write) -> AResult<u64> {
        todo!("filesystem fetch");
    }
}

impl Provide for FileSystem {}
