use serde::{Serialize, Deserialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Version {
    raw: VersionString,
    //semver: Option<semver::Version>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(transparent)]
pub struct VersionString(pub String);

impl VersionString {
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

impl PartialOrd for VersionString {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for VersionString {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        let left = bpm_version_compare::VersionRef::new(&self.0);
        let right = bpm_version_compare::VersionRef::new(&other.0);
        left.cmp(&right)
    }
}

impl PartialEq for VersionString {
    fn eq(&self, other: &VersionString) -> bool {
        let left = bpm_version_compare::VersionRef::new(&self.0);
        let right = bpm_version_compare::VersionRef::new(&other.0);
        left.eq(&right)
    }
}

impl Eq for VersionString {}

impl From<VersionString> for Version {
    fn from(v: VersionString) -> Self {
        Version::new(v.as_str())
    }
}

impl From<&str> for Version {
    fn from(s: &str) -> Self {
        Version::new(s)
    }
}

impl From<String> for VersionString {
    fn from(s: String) -> Self {
        Self(s)
    }
}

impl From<&str> for VersionString {
    fn from(s: &str) -> Self {
        Self(s.to_string())
    }
}

impl Version {

    pub fn new(v: &str) -> Self {
        Self {
            raw: v.into(),
            //semver: semver::Version::parse(v).ok()
        }
    }

    pub fn as_str(&self) -> &str {
        self.raw.as_str()
    }

    pub fn is_semver(&self) -> bool {
        semver::Version::parse(&self.raw).is_ok()
    }

//    pub fn is_pre(&self) -> bool {
//        self.pre().is_some()
//    }
//
//    pub fn pre(&self) -> Option<&str> {
//        self.semver.as_ref().and_then(|v| {
//            match v.pre.as_str() {
//                "" => None,
//                p => Some(p),
//            }
//        })
//    }
//
//    pub fn has_buildmeta(&self) -> bool {
//        self.buildmeta().is_some()
//    }
//
//    pub fn buildmeta(&self) -> Option<&str> {
//        self.semver.as_ref().and_then(|v| {
//            match v.build.as_str() {
//                "" => None,
//                p => Some(p),
//            }
//        })
//    }

}

impl std::ops::Deref for Version {
    type Target = str;
    fn deref(&self) -> &Self::Target {
        self.as_str()
    }
}

impl std::ops::Deref for VersionString {
    type Target = str;
    fn deref(&self) -> &Self::Target {
        self.0.as_str()
    }
}

//impl std::convert::AsRef<str> for VersionString {
//    fn as_ref(&self) -> &str {
//        self.0.as_str()
//    }
//}
//
//impl std::convert::AsRef<str> for Version {
//    fn as_ref(&self) -> &str {
//        self.raw.as_ref()
//    }
//}

impl PartialEq for Version {
    fn eq(&self, other: &Version) -> bool {
        self.raw.eq(&other.raw)
    }
}

impl Eq for Version {}

impl PartialOrd for Version {
    fn partial_cmp(&self, other: &Version) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Version {
    fn cmp(&self, other: &Version) -> std::cmp::Ordering {
        self.raw.cmp(&other.raw)
    }
}

impl std::fmt::Display for Version {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.raw)
    }
}

impl std::fmt::Display for VersionString {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn part_ordering() {
        let s1 = "1.2.3-7.1.2";
        let s2 = "1.2.3-7.1.10";

        let v1 = Version::new(s1);
        let v2 = Version::new(s2);
        assert!(v1 < v2);
    }

    #[test]
    fn subpart_ordering() {
        let s1 = "1.2.3-7-2-a";
        let s2 = "1.2.3-7-10-a";

        let v1 = Version::new(s1);
        let v2 = Version::new(s2);
        assert!(v1 < v2);
    }

    #[test]
    fn ascii_ordering() {
        let s1 = "1.2.3-A-2";
        let s2 = "1.2.3-a-2";

        let v1 = Version::new(s1);
        let v2 = Version::new(s2);
        assert!(v1 < v2);
    }

    #[test]
    fn cant_use_vc_crate() {

        // we used to use the version-compare crate
        // until these oddities were found

        // version-compare crate considers these equal
        assert!(
            version_compare::Version::from("1").unwrap()
            ==
            version_compare::Version::from("1.0").unwrap()
        );

        // version-compare crate does no-case string compares
        assert!(
            version_compare::Version::from("1.2.3.4-master").unwrap()
            <
            version_compare::Version::from("1.2.3.4-trial").unwrap()
        );
        assert!(
            version_compare::Version::from("1.2.3.4-master").unwrap()
            <
            version_compare::Version::from("1.2.3.4-TRIAL").unwrap()
        );

        // verison-compare crate gives strange ordering here, can't use it
        assert!(
            version_compare::Version::from("1.2.3").unwrap()
            <
            version_compare::Version::from("1.2.3.4").unwrap()
        );
        assert!(
            version_compare::Version::from("1.2.3.4-rc1").unwrap()
            <
            version_compare::Version::from("1.2.3-rc1").unwrap()
        );

    }

}

