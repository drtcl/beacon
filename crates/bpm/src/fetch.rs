use anyhow::Result;
use package::PackageID;
use std::io::Write;

pub trait Fetch {
    fn fetch(&self, write: &mut dyn Write, pkg: &PackageID, url: &str) -> Result<u64>;
}
