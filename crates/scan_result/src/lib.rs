
use std::collections::{BTreeMap, HashMap};
use serde::{Serialize, Deserialize};

use version::VersionString;

type PackageName = String;
type ChannelName = String;
type ArchName    = String;

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct VersionInfo {
    pub uri: String,
    pub filename: String,
    pub channels: Vec<ChannelName>,
    pub arch: Option<ArchName>,
}

impl VersionInfo {
    fn add_channel(&mut self, channel: Option<&str>) {
        if let Some(channel) = channel {
            if !self.channels.iter().any(|v| v == channel) {
                self.channels.push(channel.into());
            }
        }
    }
}

pub type Kv = HashMap<String, String>;

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct PackageInfo {

    // version -> [version_info]
    pub versions: BTreeMap<VersionString, Vec<VersionInfo>>,

    #[serde(default)]
    #[serde(skip_serializing_if="Option::is_none")]
    pub kv: Option<Kv>,
}

// package_name -> version -> [version_info]
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ScanResult {
    pub packages: BTreeMap<PackageName, PackageInfo>,
}

impl ScanResult {

    /// add a name, version, arch, ?channel?
    pub fn add_version<S: Into<String>>(
        &mut self,
        pkg_name: &str,
        version: &str,
        arch: Option<&str>,
        channel: Option<&str>,
        filename: S,
        uri: S
    ) {

        let version = VersionString::from(version);

        let pinfo = self.packages.entry(pkg_name.into()).or_default();
        let vlist = pinfo.versions.entry(version).or_default();
        if let Some(existing) = vlist.iter_mut().find(|ent| ent.arch.as_deref() == arch) {
            existing.add_channel(channel);
        } else {
            let mut vinfo = VersionInfo{
                uri: uri.into(),
                filename: filename.into(),
                channels: vec![],
                arch: arch.map(String::from),
            };
            vinfo.add_channel(channel);
            vlist.push(vinfo);
        }
    }

    /// apply a channel to all entries for that version
    pub fn insert_channel(&mut self, pkg_name: &str, version: &str, channel: &str) {
        if let Some(pinfo) = self.packages.get_mut(pkg_name) {
            if let Some(vlist) = pinfo.versions.get_mut(&version.into()) {
                for ent in vlist.iter_mut() {
                    ent.add_channel(Some(channel));
                }
            }
        }
    }

    /// remove package entries that don't match any of the arch filters
    pub fn filter_arch(&mut self, archs: &[&str]) {

        if archs.is_empty() {
            return;
        }

        let filters : Vec<_> = archs.iter().map(|f| package::ArchMatcher::from(*f)).collect();

        for pinfo in self.packages.values_mut() {
            for vlist in pinfo.versions.values_mut() {
                vlist.retain(|ent| {
                    filters.iter().any(|f| f.matches(ent.arch.as_deref()))
                });
            }
            pinfo.versions.retain(|_version, vlist| !vlist.is_empty());
        }
        self.packages.retain(|_pname, pinfo| !pinfo.versions.is_empty());
    }

    pub fn filter_arch_fn<F: Fn(Option<&str>) -> bool>(&mut self, f: F) {
        for pinfo in self.packages.values_mut() {
            for vlist in pinfo.versions.values_mut() {
                vlist.retain(|ent| {
                    f(ent.arch.as_deref())
                });
            }
            pinfo.versions.retain(|_version, vlist| !vlist.is_empty());
        }
        self.packages.retain(|_pname, pinfo| !pinfo.versions.is_empty());
    }

    /// remove package entries that don't match one of the named packages
    pub fn filter_package(&mut self, pkgs: &[&str]) {
        if !pkgs.is_empty() {
            self.filter_package_fn(|name: &str| pkgs.contains(&name));
        }
    }

    pub fn filter_package_fn<F: Fn(&str) -> bool>(&mut self, f: F) {
        self.packages.retain(|pname, _pinfo| f(pname))
    }

    /// remove package entires that don't match the given channels
    pub fn filter_channel(&mut self, channels: &[&str]) {
        if !channels.is_empty() {
            for pinfo in self.packages.values_mut() {
                for vlist in pinfo.versions.values_mut() {
                    vlist.retain(|ent| {
                        ent.channels.iter().any(|c| channels.contains(&c.as_str()))
                    });
                }
                pinfo.versions.retain(|_version, vlist| !vlist.is_empty());
            }
            self.packages.retain(|_pname, pinfo| !pinfo.versions.is_empty());
        }
    }

    pub fn filter_channel_fn<F: Fn(&[String]) -> bool>(&mut self, f: F) {
        for pinfo in self.packages.values_mut() {
            for vlist in pinfo.versions.values_mut() {
                vlist.retain(|ent| f(ent.channels.as_slice()));
            }
            pinfo.versions.retain(|_version, vlist| !vlist.is_empty());
        }
        self.packages.retain(|_pname, pinfo| !pinfo.versions.is_empty());
    }

    /// count of packages
    pub fn package_count(&self) -> usize {
        self.packages.len()
    }

    /// count of all versions for all packages
    pub fn version_count(&self) -> usize {
        self.packages.values().map(|pinfo| pinfo.versions.len()).sum()
    }

    /// count of all versions+archs for all packages
    pub fn unique_count(&self) -> usize {
        self.packages.iter()
            .flat_map(|(_pname, pinfo)| pinfo.versions.iter())
            .map(|(_version, vlist)| vlist.len())
            .sum()
    }

    /// store a Kv for a given package name
    pub fn add_kv(&mut self, pkg_name: &str, kv: Kv) {
        if let Some(pkg_info) = self.packages.get_mut(pkg_name) {
            pkg_info.kv = Some(kv);
        }
    }

    /// merge packge info from multiple scan results (different providers)
    pub fn merge(&mut self, other: Self) {
        for (pname, right_pinfo) in other.packages {
            let left_pinfo = self.packages.entry(pname).or_default();
            left_pinfo.merge(right_pinfo);
        }
    }

    pub fn print(&self) {
        for (name, pkg_info) in &self.packages {
            println!("{}", name);
            if let Some(kv) = &pkg_info.kv {
                println!("  kv {}", serde_json::to_string_pretty(kv).unwrap());
            }
            for (version, vlist) in pkg_info.versions.iter().rev() {
                for info in vlist {
                    let arch = info.arch.as_deref().unwrap_or("noarch");
                    print!("  {version} ({arch})");
                    for chan in &info.channels {
                        print!(" {chan}");
                    }
                    print!(" {}", info.uri);
                    println!();
                }
            }
        }
    }
}

impl PackageInfo {

    pub fn has_version(&self, v: &str) -> bool {
        self.versions.iter().any(|(version, _vlist)| version.as_str() == v)
    }

    pub fn has_channel(&self, c: &str) -> bool {
        self.versions.iter()
            .flat_map(|(_version, vlist)| vlist.iter())
            .flat_map(|ent| ent.channels.iter())
            .any(|chan| chan == c)
    }

    fn merge(&mut self, other: Self) {

        for (r_key, r_val) in other.versions {
            let l_val = self.versions.entry(r_key).or_default();
            l_val.extend(r_val);
            //l_val.merge(r_val);
        }

        if self.kv.is_none() && other.kv.is_some() {
            self.kv = other.kv;
        }
    }
}

pub trait Scan {
    fn scan(&self, arch_filter: Option<&[&str]>) -> anyhow::Result<ScanResult>;
}

#[cfg(test)]
mod test {
    use super::*;

    #[allow(dead_code)]
    fn print(lbl: &str, r: &ScanResult) {
        println!("-- {} --", lbl);
        r.print();
    }

    #[test]
    fn merge() -> anyhow::Result<()> {

        let a : ScanResult = serde_json::from_value(serde_json::json! {{
            "packages": {
                "A": {
                    "kv": null,
                    "versions": {
                        "1" : [
                            {
                                "uri": "from_a",
                                "filename": "from_a",
                                "channels": ["a1", "a2"],
                                "arch": null,
                            },
                            {
                                "uri": "from_a",
                                "filename": "from_a",
                                "channels": ["a1", "a2"],
                                "arch": "linux",
                            }
                        ]
                    }
                },
                "AB": {
                    "kv": null,
                    "versions": {
                        "1" : [
                            {
                                "uri": "from_a",
                                "filename": "from_a",
                                "channels": ["a"],
                                "arch": null,
                            }
                        ],
                        "2" : [
                            {
                                "uri": "from_a",
                                "filename": "from_a",
                                "channels": ["a"],
                                "arch": null,
                            }
                        ]
                    }
                },
            }
        }})?;

        let b : ScanResult = serde_json::from_value(serde_json::json! {{
            "packages": {
                "B": {
                    "kv": null,
                    "versions": {
                        "1" : [
                            {
                                "uri": "from_b",
                                "filename": "from_b",
                                "channels": ["b"],
                                "arch": "linux",
                            }
                        ]
                    }
                },
                "AB": {
                    "kv": {
                        "b": "b"
                    },
                    "versions": {
                        "2" : [
                            {
                                "uri": "from_b",
                                "filename": "from_b",
                                "channels": ["b"],
                                "arch": "linux",
                            },
                            {
                                "uri": "from_b",
                                "filename": "from_b",
                                "channels": ["b"],
                                "arch": "windows",
                            }
                        ],
                        "3" : [
                            {
                                "uri": "from_b",
                                "filename": "from_b",
                                "channels": ["b"],
                                "arch": "linux",
                            }
                        ]
                    }
                }
            }
        }})?;

        let c : ScanResult = serde_json::from_value(serde_json::json! {{
            "packages": {
                "A": {
                    "kv": null,
                    "versions": {
                        "1" : [
                            {
                                "uri": "from_a",
                                "filename": "from_a",
                                "channels": ["a1", "a2"],
                            },
                            {
                                "uri": "from_a",
                                "filename": "from_a",
                                "channels": ["a1", "a2"],
                                "arch": "linux",
                            }
                        ]
                    }
                },
                "B": {
                    "kv": null,
                    "versions": {
                        "1" : [
                            {
                                "uri": "from_b",
                                "filename": "from_b",
                                "channels": ["b"],
                                "arch": "linux",

                            },
                        ]
                    }
                },
                "AB": {
                    "kv": {
                        "b": "b"
                    },
                    "versions": {
                        "1" : [
                            {
                                "uri": "from_a",
                                "filename": "from_a",
                                "channels": ["a"],
                                "arch": null,
                            },
                        ],
                        "2" : [
                            {
                                "uri": "from_a",
                                "filename": "from_a",
                                "channels": ["a"],
                                "arch": null,
                            },
                            {
                                "uri": "from_b",
                                "filename": "from_b",
                                "channels": ["b"],
                                "arch": "linux",
                            },
                            {
                                "uri": "from_b",
                                "filename": "from_b",
                                "channels": ["b"],
                                "arch": "windows",
                            },
                        ],
                        "3" : [
                            {
                                "uri": "from_b",
                                "filename": "from_b",
                                "channels": ["b"],
                                "arch": "linux",
                            },
                        ]
                    }
                }
            }
        }})?;

        let mut sink = ScanResult::default();
        sink.merge(a.clone());
        assert_eq!(sink, a);

        sink.merge(b.clone());
        //print("a", &a);
        //print("b", &b);
        //print("c", &c);
        //print("sink",&sink);
        assert_eq!(sink, c);

        sink.filter_arch(&["", "linux"]);
        print("filter", &sink);
        assert_eq!(sink.package_count(), 3);
        assert_eq!(sink.version_count(), 5);
        assert_eq!(sink.unique_count(), 7);

        sink.filter_package(&["A", "AB"]);
        print("package filter", &sink);
        assert_eq!(sink.package_count(), 2);
        assert_eq!(sink.version_count(), 4);
        assert_eq!(sink.unique_count(), 6);

        sink.filter_channel(&["a", "b"]);
        print("channel filter", &sink);
        assert_eq!(sink.package_count(), 1);
        assert_eq!(sink.version_count(), 3);
        assert_eq!(sink.unique_count(), 4);

        assert!(sink.packages.iter().next().unwrap().1.has_version("1"));
        assert!(!sink.packages.iter().next().unwrap().1.has_version("9"));
        assert!(sink.packages.iter().next().unwrap().1.has_channel("a"));
        assert!(!sink.packages.iter().next().unwrap().1.has_channel("X"));

        Ok(())
    }
}

