
use semver::Version as SemVer;

#[derive(Debug, Clone)]
pub struct Version {
    raw: String,
    semver: Option<SemVer>,
}

impl Version {
    pub fn new(v: &str) -> Self {
        Self {
            raw: v.to_string(),
            semver: SemVer::parse(v).ok()
        }
    }
    pub fn as_str(&self) -> &str {
        self.raw.as_str()
    }

    pub fn is_beta(&self) -> bool {
        false
    }

    pub fn is_semver(&self) -> bool {
        self.semver.is_some()
    }
}

impl std::ops::Deref for Version {
    type Target = str;
    fn deref(&self) -> &Self::Target {
        self.as_str()
    }
}

impl PartialEq for Version {
    fn eq(&self, rhs: &Version) -> bool {
        match (&self.semver, &rhs.semver) {
            (Some(a), Some(b)) => {
                a == b
            },
            _ => {
                self.raw == rhs.raw
            }
        }
    }
}

impl Eq for Version {}

impl PartialOrd for Version {
    fn partial_cmp(&self, rhs: &Version) -> Option<std::cmp::Ordering> {
        Some(self.cmp(rhs))
    }
}

impl Ord for Version {
    fn cmp(&self, rhs: &Version) -> std::cmp::Ordering {
        match (&self.semver, &rhs.semver) {
            (Some(a), Some(b)) => {
                a.cmp(&b)
            },
            _ => {
                self.raw.cmp(&rhs.raw)
            }
        }
    }
}

impl std::fmt::Display for Version {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.raw)
    }
}
