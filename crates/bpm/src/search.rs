use std::collections::BTreeMap;

use anyhow::Result;

use std::rc::Rc;
use serde::{Serialize, Deserialize};

pub type PackageName = String;
use version::{Version, VersionString};
pub type Url = String;
pub type Filename = String;

#[derive(Debug, Clone)]
pub struct SingleListing {
    pub pkg_name: Rc<str>,
    pub version: Version,
    pub filename: Filename,
    pub url: Url,
    pub channels: Vec<String>,
}
