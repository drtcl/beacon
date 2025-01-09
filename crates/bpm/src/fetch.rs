use anyhow::Result;
use package::PackageID;
use std::io::Write;
use indicatif::MultiProgress;

pub trait Fetch {
    fn fetch(&self, write: &mut dyn Write, pkg: &PackageID, url: &str, bars: Option<&MultiProgress>) -> Result<u64>;
}
