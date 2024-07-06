use std::collections::BTreeMap;

use anyhow::Result;

use std::rc::Rc;
use serde::{Serialize, Deserialize};

pub type PackageName = String;
use version::{Version, VersionString};
pub type Url = String;
pub type Filename = String;

/// info for a single version
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VersionInfo {
    pub url: Url,
    pub filename: Filename,
    pub channels: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct SingleListing {
    pub pkg_name: Rc<str>,
    pub version: Version,
    pub filename: Filename,
    pub url: Url,
    pub channels: Vec<String>,
}

pub type VersionList = BTreeMap<VersionString, VersionInfo>;
pub type PackageList = BTreeMap<PackageName, VersionList>;

pub trait Search {
    fn search(&self, name: &str) -> Result<PackageList>;
    fn scan(&self) -> Result<PackageList>;
}

pub fn flatten(list: PackageList) -> Vec<SingleListing> {
    let mut ret = Vec::new();
    for (pkg_name, versions) in list.into_iter() {
        let pkg_name: Rc<str> = Rc::from(pkg_name);
        for (version, urlfn) in versions {
            ret.push(SingleListing {
                pkg_name: Rc::clone(&pkg_name),
                version: version.into(),
                filename: urlfn.filename,
                url: urlfn.url,
                channels: urlfn.channels,
            });
        }
    }
    ret
}

pub fn merge_package_lists(mut a: PackageList, b: PackageList) -> PackageList {

    // extend a with b, but do not overwrite values, extend the values

    for (name, versions) in b.into_iter() {
        let vl = a.entry(name).or_default();
        for (version, url) in versions {
            vl.insert(version, url);
        }
    }

    a
}
