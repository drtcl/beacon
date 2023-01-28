use std::cmp::Ordering;
use std::collections::btree_map::Entry;
use std::collections::BTreeMap;
use std::collections::BTreeSet;

pub trait Search {
    fn search(&self, name: &str) -> SearchResults;
}

type PackageName = String;

#[derive(Debug)]
pub struct SearchResult {
    pub name: String,
    pub versions: BTreeSet<semver::Version>,
    //pub uri: String,
}

impl PartialEq for SearchResult {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
    }
}

impl PartialOrd for SearchResult {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.name.partial_cmp(&other.name)
    }
}

impl Eq for SearchResult {}

impl Ord for SearchResult {
    fn cmp(&self, other: &Self) -> Ordering {
        self.name.cmp(&other.name)
    }
}

#[derive(Debug)]
pub struct SearchResults {
    pub inner: BTreeMap<PackageName, SearchResult>,
}

impl SearchResults {
    pub fn new() -> Self {
        SearchResults {
            inner: BTreeMap::new(),
        }
    }

    pub fn extend(&mut self, other: SearchResults) {
        for pkg in other.inner.into_iter() {
            match self.inner.entry(pkg.0) {
                Entry::Vacant(o) => {
                    o.insert(pkg.1);
                }
                Entry::Occupied(mut o) => {
                    o.get_mut().versions.extend(pkg.1.versions);
                }
            }
        }
    }
}
