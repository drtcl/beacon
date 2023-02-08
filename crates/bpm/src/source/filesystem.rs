use crate::AResult;
use crate::fetch::*;
use crate::provider::Provide;
use crate::search::*;
use std::io::Write;
use std::path::{Path, PathBuf};

use package::PackageID;

#[derive(Debug)]
pub struct FileSystem {
    root: PathBuf,
}

//#[derive(Debug)]
//struct FileSystemPackage {
//    name: String,
//    version: String,
//    filename: String,
//    url: String,
//}

impl FileSystem {
    pub fn new<P: AsRef<Path>>(path: P) -> Self {
        FileSystem {
            root: PathBuf::from(path.as_ref()),
        }
    }

//    fn get_versions(&self, pkg_name: &str) -> AResult<Vec<semver::Version>> {
//        let mut pkg_dir = PathBuf::new();
//        pkg_dir.push(&self.root);
//        pkg_dir.push(pkg_name);
//
//        let versions = std::fs::read_dir(&pkg_dir)?
//            .filter_map(|x| x.ok().map(|y| y.file_name()))
//            .filter_map(|x| x.into_string().ok())
//            .filter_map(|x| semver::Version::parse(&x).ok())
//            .sorted()
//            .rev()
//            .collect();
//
//        Ok(versions)
//    }

//    fn search<F>(&self, filter: F) -> AResult<Vec<FileSystemPackage>>
//        where F: Fn(&FileSystemPackage) -> bool,
//    {
//        let mut results = Vec::new();
//
//        for entry in std::fs::read_dir(&self.root)? {
//            if let Ok(entry) = entry {
//                if let Ok(filename) = entry.file_name().into_string() {
//
//                    if !filename.ends_with(package::PKG_FILE_EXTENSION) {
//                        continue;
//                    }
//
//                    if let Some((pkg_name, version)) = package::split_parts(&filename) {
//
//                        let mut url = entry.path().to_string_lossy().to_string();
//                        url.insert_str(0, "file://");
//
//                        let pkg = FileSystemPackage {
//                            name: pkg_name.to_string(),
//                            version: version.to_string(),
//                            filename,
//                            url,
//                        };
//
//                        if filter(&pkg) {
//                            results.push(pkg);
//                        }
//                    }
//                }
//            }
//        }
//
//        return Ok(results);
//    }
}

impl From<fssearch::VersionInfo> for VersionInfo {
    fn from(value: fssearch::VersionInfo) -> Self {
        Self {
            url: value.url,
            filename: value.filename,
            channels: value.channels,
        }
    }
}

impl Search for FileSystem {
//    fn search(&self, name: &str) -> AResult<PackageList> {
//        tracing::trace!("FileSystem::search {}", name);
//        let results = self.search(|p| p.name.contains(name))?;
//        dbg!(&results);
//        let mut pkgs = PackageList::new();
//        for ent in results.into_iter() {
//            let vl = pkgs.entry(ent.name).or_insert(VersionList::new());
//            vl.insert(ent.version, UrlFilename {
//                url: ent.url,
//                filename: "".into(),
//            });
//        }
//        //dbg!(&pkgs);
//        Ok(pkgs)
//    }
    fn search(&self, needle: &str) -> AResult<PackageList> {
        tracing::debug!(needle, root=?&self.root, "[Search] FileSystem::search");

        let mut pkgs = fssearch::full_scan(&self.root)?;

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
        Ok(self.search("").ok().unwrap_or(PackageList::new()))
        //let results = self.search(|_| true);
        //dbg!(&results);
        //let pkgs = PackageList::new();
        //pkgs
    }
}

//impl Search for FileSystem {
//    fn search(&self, name: &str) -> SearchResults {
//        //println!("FileSystem::[Search]::search {}", name);
//
//        let results = self.search(name);
//
//        if let Ok(results) = results {
//            return SearchResults {
//                inner: results
//                    .into_iter()
//                    .map(|result| {
//                        (
//                            result.name.clone(),
//                            SearchResult {
//                                name: result.name,
//                                versions: result.versions.into_iter().collect(),
//                                //uri: String::new(),
//                            },
//                        )
//                    })
//                    .collect(),
//            };
//        }
//
//        return SearchResults::new();
//    }
//}

impl Fetch for FileSystem {

    fn fetch(&self, write: &mut dyn Write, pkg: &PackageID, url: &str) -> AResult<u64> {

        tracing::trace!(pkg=?pkg, url, "FileSystem::fetch()");

        let mut file = std::fs::File::open(url)?;
        Ok(std::io::copy(&mut file, write)?)
    }
}

impl Provide for FileSystem {}
