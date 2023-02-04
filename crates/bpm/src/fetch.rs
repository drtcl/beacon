use crate::AResult;

use crate::package::PackageID;
use std::io::Write;

pub trait Fetch {
    fn fetch(&self, pkg: &PackageID, write: &mut dyn Write) -> AResult<u64>;
}
