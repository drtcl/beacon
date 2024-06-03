use std::collections::BTreeSet;
use std::collections::BTreeMap;
use super::App;
use anyhow::Result;
use crate::*;

impl App {

    pub fn list_cmd(&mut self, matches: &clap::ArgMatches) -> Result<()> {
        match matches.subcommand() {
            Some(("available", sub_matches)) => {
                let exact = sub_matches.get_flag("exact");
                let json = sub_matches.get_flag("json");
                let oneline = sub_matches.get_flag("oneline");
                let name = sub_matches.get_one::<String>("pkg");
                let channels = args::pull_many_opt(sub_matches, "channels");
                let limit = *sub_matches.get_one::<u32>("limit").unwrap();

                self.provider_filter = args::parse_providers(sub_matches);
                self.list_available_cmd(name, exact, oneline, json, channels, limit)?;
            }
            Some(("channels", sub_matches)) => {
                self.provider_filter = args::parse_providers(sub_matches);
                self.list_channels_cmd()?;
            }
            Some(("installed", _sub_matches)) => {
                self.list_installed()?;
            }
            Some(_) => unreachable!(),
            None => {
                self.list_installed()?;
            }
        }

        Ok(())
    }

    /// `bpm list`
    /// list installed packages
    pub fn list_installed(&mut self) -> Result<()> {

        // if the db file doesn't exist, dont' attempt to load it, return 0 packages
        if !self.db_file_exists() {
            return Ok(());
        }

        self.load_db()?;

        let mut tw = tabwriter::TabWriter::new(std::io::stdout());
        //write!(&mut tw, "{}\t{}\t{}\n", "name", "version", "channel")?;
        for ent in self.db.installed.iter() {
            let channel = ent.versioning.channel.as_deref().unwrap_or("");
            let pinned = tern!(ent.versioning.pinned_to_version, "=", "^");
            writeln!(&mut tw, "{}\t{}{}\t{}", ent.metadata.name, pinned, ent.metadata.version, channel)?;
        }
        tw.flush()?;
        Ok(())
    }

    /// `bpm list channels`
    /// list channels for a given package, or all packages
    pub fn list_channels_cmd(&mut self) -> Result<()> {

        let mut combined = search::PackageList::new();
        for provider in self.filtered_providers() {
            if let Ok(data) = provider.load_file() {
                combined = search::merge_package_lists(combined, data.packages);
            }
        }

        combined.retain(|_, versions| versions.iter().any(|(_, vi)| !vi.channels.is_empty()));
        let mut m = BTreeMap::new();
        for (pkg_name, versions) in combined {
            let entry = m.entry(pkg_name).or_insert(BTreeSet::new());
            for (_version, info) in versions {
                for channel in info.channels {
                    entry.insert(channel);
                }
            }
        }

        let mut stdout = std::io::stdout();
        for (name, channels) in m {
            write!(&mut stdout, "{}", name)?;
            for channel in channels {
                write!(&mut stdout, " {}", channel)?;
            }
            writeln!(&mut stdout)?;
        }
        Ok(())
    }

    /// `bpm list available` or `bpm list avail`
    /// list all versions of all packages that are available
    pub fn list_available_cmd(&mut self,
        needle: Option<&String>,
        exact: bool,
        oneline: bool,
        json: bool,
        channels: Option<Vec<&String>>,
        limit: u32
    ) -> Result<()> {

        let mut combined = search::PackageList::new();
        for provider in self.filtered_providers() {
            if let Ok(data) = provider.load_file() {
                combined = search::merge_package_lists(combined, data.packages);
            }
        }

        // limit to package names containing the search term (or exact matches)
        if let Some(needle) = needle {
            combined.retain(|pkg_name, _versions| {
                if exact {
                    pkg_name == needle
                } else {
                    pkg_name.contains(needle)
                }
            });
        }

        // limit to only the specified channels
        if let Some(channels) = channels {
            combined.retain(|_pkg_name, versions| {
                versions.retain(|version, info| {
                    if !info.channels.is_empty() {
                        for c in &info.channels {
                            if channels.contains(&c) {
                                return true;
                            }
                        }
                    }
                    false
                });
                !versions.is_empty()
            });
        }

        for (name, versions) in &combined {
            let mut sorted = Vec::new();
            versions.iter().map(|(v, i)| (version::Version::new(v), i)).collect_into(&mut sorted);
            sorted.sort_by(|a, b| a.0.cmp(&b.0));

            // limit to N number of versions per package
            let take = sorted.iter().rev().take(if limit > 0 { limit as usize } else { sorted.len() });

            if oneline {

                // foo 1.0.0 1.0.1-test +beta

                print!("{name}");
                for (version, info) in take {
                    print!(" {}", version.as_str());
                    for c in &info.channels {
                        print!(" +{c}");
                    }
                }
                println!();
            } else if json {

                // {"package":"foo","versions":[["1.0.0",[]],["1.0.1-test",["beta"]]]}

                let mut json_versions = Vec::new();
                for (version, info) in take {
                    let mut json_channels = Vec::new();
                    for channel in &info.channels {
                        json_channels.push(channel.to_string());
                    }

                    json_versions.push(serde_json::json!((version.to_string(), json_channels)));
                }
                println!("{}", serde_json::json!({
                    "package": name,
                    "versions": json_versions,
                }));
            } else {

                // foo
                //   1.0.0
                //   1.0.1-test beta

                println!("{name}");
                for (version, info) in take {
                    print!("  {}", version.as_str());
                    for c in &info.channels {
                        print!(" {c}");
                    }
                    println!();
                }
            }
        }

        Ok(())
    }
}
