use std::rc::Rc;
use version::Version;

//pub type PackageName = String;
pub type Url = String;
pub type Filename = String;

#[derive(Debug, Clone)]
pub struct SingleListing {
    pub pkg_name: Rc<str>,
    pub version: Version,
    pub filename: Filename,
    pub url: Url,
    pub channels: Vec<String>,
    pub arch: Option<String>,
}
