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

    pub fn is_pre(&self) -> bool {
        self.pre().is_some()
    }

    pub fn pre(&self) -> Option<&str> {
        self.semver.as_ref().and_then(|v| {
            match v.pre.as_str() {
                "" => None,
                p => Some(p),
            }
        })
    }

    pub fn has_buildmeta(&self) -> bool {
        self.buildmeta().is_some()
    }

    pub fn buildmeta(&self) -> Option<&str> {
        self.semver.as_ref().and_then(|v| {
            match v.build.as_str() {
                "" => None,
                p => Some(p),
            }
        })
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
                a.cmp(b)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compare_semver() {
        let v1 = Version::new("0.1.0");
        let v2 = Version::new("0.2.0");
        assert!(v1.is_semver());
        assert!(v2.is_semver());
        assert!(v1 < v2);
    }

    #[test]
    fn compare_nonsemver() {
        let v1 = Version::new("0.2");
        let v2 = Version::new("0.2.0.1");
        assert!(!v1.is_semver());
        assert!(!v2.is_semver());
        assert!(v1 < v2);
    }

    #[test]
    fn semver() {
        let v = Version::new("3.1.4");
        assert!(v.is_semver());
        assert!(v.as_str() == "3.1.4");
        assert!(!v.is_pre());
        assert!(!v.has_buildmeta());
        assert_eq!(v.pre(), None);
        assert_eq!(v.buildmeta(), None);
    }

    #[test]
    fn semver_pre() {
        let v = Version::new("3.1.4-beta");
        assert!(v.is_semver());
        assert!(v.as_str() == "3.1.4-beta");
        assert!(v.is_pre());
        assert!(!v.has_buildmeta());
        assert_eq!(v.pre(), Some("beta"));
        assert_eq!(v.buildmeta(), None);
    }

    #[test]
    fn semver_build() {
        let v = Version::new("3.1.4+deprecated");
        assert!(v.is_semver());
        assert!(v.as_str() == "3.1.4+deprecated");
        assert!(!v.is_pre());
        assert!(v.has_buildmeta());
        assert_eq!(v.pre(), None);
        assert_eq!(v.buildmeta(), Some("deprecated"));
    }

    #[test]
    fn semver_pre_build() {
        let v = Version::new("3.1.4-beta+linux");
        assert!(v.is_semver());
        assert!(v.as_str() == "3.1.4-beta+linux");
        assert!(v.is_pre());
        assert!(v.has_buildmeta());
        assert_eq!(v.pre(), Some("beta"));
        assert_eq!(v.buildmeta(), Some("linux"));
    }
}

