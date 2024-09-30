
use std::collections::{BTreeMap, HashMap};
use serde::{Serialize, Deserialize};

use version::VersionString;

type ChannelName = String;
type PackageName = String;

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct VersionInfo {
    pub uri: String,
    pub filename: String,
    pub channels: Vec<ChannelName>,
}

pub type Kv = HashMap<String, String>;

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct PackageInfo {

    pub versions: BTreeMap<VersionString, VersionInfo>,

    #[serde(default)]
    #[serde(skip_serializing_if="Option::is_none")]
    pub kv: Option<Kv>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ScanResult {
    pub packages: BTreeMap<PackageName, PackageInfo>,
}

impl ScanResult {

    pub fn add_version(&mut self, pkg_name: &str, version: &str, info: VersionInfo) {
        let version = VersionString::from(version);

        let pkg_info = self.packages.entry(pkg_name.to_string())
            .or_default();

        match pkg_info.versions.get_mut(&version) {
            None => {
                pkg_info.versions.insert(version, info);
            }
            Some(ref mut entry) => {
                for channel in info.channels {
                    if !entry.channels.iter().any(|c| c == &channel) {
                        entry.channels.push(channel.to_string());
                    }
                }
            }
        }
    }

    pub fn add_channel_version(&mut self, pkg_name: &str, channel: &str, version: &str) {
        if let Some(pkg_info) = self.packages.get_mut(pkg_name) {
            if let Some(vinfo) = pkg_info.versions.get_mut(&VersionString::from(version)) {
                if !vinfo.channels.iter().any(|v| v == channel) {
                    vinfo.channels.push(channel.to_string());
                }
            }
        }
    }

    pub fn add_kv(&mut self, pkg_name: &str, kv: Kv) {
        if let Some(pkg_info) = self.packages.get_mut(pkg_name) {
            pkg_info.kv = Some(kv);
        }
    }

    /// merge packge info from multiple scan results (different providers)
    pub fn merge(&mut self, other: Self) {
        for (pname, right_pinfo) in other.packages {
            let left_pinfo = self.packages.entry(pname).or_default();
            for (version, vinfo) in right_pinfo.versions {
                match left_pinfo.versions.entry(version) {
                    std::collections::btree_map::Entry::Vacant(v) => {
                        v.insert(vinfo);
                    }
                    std::collections::btree_map::Entry::Occupied(o) => {
                        o.into_mut().merge(vinfo);
                    }
                }
            }
            if left_pinfo.kv.is_none() && right_pinfo.kv.is_some() {
                left_pinfo.kv = right_pinfo.kv;
            }
        }
    }
}

impl VersionInfo {
    /// only merge channels, does not update uri or filename
    fn merge(&mut self, other: Self) {
        for c in other.channels {
            if !self.channels.contains(&c) {
                self.channels.push(c);
            }
        }
        self.channels.sort();
    }
}

pub trait Scan {
    fn scan(&self) -> anyhow::Result<ScanResult>;
}

#[cfg(test)]
mod test {
    use super::*;

    #[allow(dead_code)]
    fn print(lbl: &str, r: &ScanResult) {
        println!("-- {} --", lbl);
        for (name, pkg_info) in &r.packages {
            //println!("{} {:?}", name, pkg_info.keys());
            println!("{}", name);
            if let Some(kv) = &pkg_info.kv {
                println!("  kv {}", serde_json::to_string_pretty(kv).unwrap());
            }
            for (version, info) in &pkg_info.versions {
                print!("  {}", version);
                for chan in &info.channels {
                    print!(" {}", chan);
                }
                println!();
            }
        }
    }

    #[test]
    //fn merge() -> Result<(), impl std::error::Error> {
    fn merge() -> anyhow::Result<()> {

        let a : ScanResult = serde_json::from_value(serde_json::json! {{
            "packages": {
                "A": {
                    "kv": null,
                    "versions": {
                        "1" : {
                            "uri": "ignore",
                            "filename": "ignore",
                            "channels": ["a"]
                        }
                    }
                },
                "AB": {
                    "kv": null,
                    "versions": {
                        "1" : {
                            "uri": "ignore",
                            "filename": "ignore",
                            "channels": ["a"]
                        },
                        "2" : {
                            "uri": "ignore",
                            "filename": "ignore",
                            "channels": ["a"]
                        }
                    }
                },
            }
        }})?;

        let b : ScanResult = serde_json::from_value(serde_json::json! {{
            "packages": {
                "B": {
                    "kv": null,
                    "versions": {
                        "1" : {
                            "uri": "ignore",
                            "filename": "ignore",
                            "channels": ["b"]
                        }
                    }
                },
                "AB": {
                    "kv": {
                        "b": "b"
                    },
                    "versions": {
                        "2" : {
                            "uri": "ignore",
                            "filename": "ignore",
                            "channels": ["b"]
                        },
                        "3" : {
                            "uri": "ignore",
                            "filename": "ignore",
                            "channels": ["b"]
                        }
                    }
                }
            }
        }})?;

        let c : ScanResult = serde_json::from_value(serde_json::json! {{
            "packages": {
                "A": {
                    "kv": null,
                    "versions": {
                        "1" : {
                            "uri": "ignore",
                            "filename": "ignore",
                            "channels": ["a"]
                        }
                    }
                },
                "B": {
                    "kv": null,
                    "versions": {
                        "1" : {
                            "uri": "ignore",
                            "filename": "ignore",
                            "channels": ["b"]
                        }
                    }
                },
                "AB": {
                    "kv": {
                        "b": "b"
                    },
                    "versions": {
                        "1" : {
                            "uri": "ignore",
                            "filename": "ignore",
                            "channels": ["a"]
                        },
                        "2" : {
                            "uri": "ignore",
                            "filename": "ignore",
                            "channels": ["a", "b"]
                        },
                        "3" : {
                            "uri": "ignore",
                            "filename": "ignore",
                            "channels": ["b"]
                        },
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

        Ok(())
    }
}

