use crate::fetch::Fetch;
use crate::search::Search;

pub(crate) trait Provide: Search + Fetch {}
