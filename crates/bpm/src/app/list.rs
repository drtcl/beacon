use std::collections::BTreeSet;
use std::collections::BTreeMap;
use std::io::IsTerminal;
use super::App;
use anyhow::Result;
use crate::*;
use package::ArchMatcher;

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

                let show_arch = sub_matches.get_flag("show-arch");

                let arch = args::pull_many_opt(sub_matches, "arch");
                self.setup_arch_filter(arch);

                self.provider_filter = args::parse_providers(sub_matches);
                self.list_available_cmd(name, exact, limit, oneline, json, channels, show_arch)?;
            }
            Some(("channels", sub_matches)) => {
                let name = sub_matches.get_one::<String>("pkg");
                let exact = sub_matches.get_flag("exact");
                let json = sub_matches.get_flag("json");

                let arch = args::pull_many_opt(sub_matches, "arch");
                self.setup_arch_filter(arch);

                self.provider_filter = args::parse_providers(sub_matches);

                self.list_channels_cmd(name, exact, json)?;
            }
            Some(("installed", sub_matches)) => {
                let show_arch = sub_matches.get_flag("show-arch");
                self.list_installed(show_arch)?;
            }
            Some(_) => unreachable!(),
            None => {
                self.list_installed(false)?;
            }
        }

        Ok(())
    }

    /// `bpm list installed` OR `bpm list`
    /// list installed packages
    pub fn list_installed(&mut self, show_arch: bool) -> Result<()> {

        self.shared_lock()?;

        // if the db file doesn't exist, dont' attempt to load it, return 0 packages
        if !self.db_file_exists() {
            return Ok(());
        }

        self.load_db()?;

        let is_term = std::io::stdout().is_terminal();
        let mut tw = tabwriter::TabWriter::new(std::io::stdout());
        if is_term {
            if show_arch {
                writeln!(&mut tw, "name\tversion\tchannel\tarch")?;
            } else {
                writeln!(&mut tw, "name\tversion\tchannel")?;
            }
        }
        for ent in self.db.installed.iter() {
            let channel = ent.versioning.channel.as_deref().unwrap_or("");
            let pinned = tern!(ent.versioning.pinned_to_version, "=", "^");
            if show_arch {
                let arch = ent.metadata.arch.as_deref().unwrap_or("noarch");
                writeln!(&mut tw, "{}\t{}{}\t{}\t{}", ent.metadata.name, pinned, ent.metadata.version, channel, arch)?;
            } else {
                writeln!(&mut tw, "{}\t{}{}\t{}", ent.metadata.name, pinned, ent.metadata.version, channel)?;
            }
        }
        tw.flush()?;
        Ok(())
    }

    /// `bpm list channels`
    /// list channels for a given package, or all packages
    pub fn list_channels_cmd(&mut self, needle: Option<&String>, exact: bool, json: bool) -> Result<()> {

        self.shared_lock()?;

        let mut combined = scan_result::ScanResult::default();
        for provider in self.filtered_providers() {
            if let Ok(data) = provider.load_file() {
                combined.merge(data.packages);
            }
        }

        // limit to package names containing the search term (or exact matches)
        if let Some(needle) = needle {
            combined.packages.retain(|pkg_name, _| {
                if exact {
                    pkg_name == needle
                } else {
                    pkg_name.contains(needle)
                }
            });
        }

        // remove any that don't match the arch filters
        if !self.arch_filter.is_empty() {
            let arch : Vec<ArchMatcher> = self.arch_filter.iter().map(|v| ArchMatcher::from(v.as_str())).collect();
            combined.filter_arch_fn(|pkg_arch: Option<&str>| {
                arch.iter().any(|m| m.matches(pkg_arch))
            });
        }

        // remove any packages that don't have a channel at all
        combined.packages.retain(|_pkg_name, pkg_info| pkg_info.versions.iter().any(|(_version, vlist)| vlist.iter().any(|info| !info.channels.is_empty())));

        // pkg_name -> set of channel_name
        let mut m : BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
        for (pkg_name, pkg_info) in combined.packages {
            let entry = m.entry(pkg_name).or_default();
            for (_version, vlist) in pkg_info.versions {
                for info in vlist {
                    for channel in info.channels {
                        entry.insert(channel);
                    }
                }
            }
        }

        let mut stdout = std::io::stdout();
        for (name, channels) in m {

            if json {
                //println!("{}", serde_json::json!{{
                //    "package": name,
                //    "channels": channels,
                //}});
                // just to get the key order to be: package, channels
                println!("{{\"package\":\"{name}\",\"channels\":{}}}", serde_json::json!(channels));
            } else {
                write!(&mut stdout, "{}", name)?;
                for channel in channels {
                    write!(&mut stdout, " {}", channel)?;
                }
                writeln!(&mut stdout)?;
            }
        }
        Ok(())
    }

    /// `bpm list available` or `bpm list avail`
    /// list all versions of all packages that are available
    pub fn list_available_cmd(&mut self,
        needle: Option<&String>,
        exact: bool,
        limit: u32,
        oneline: bool,
        json: bool,
        channels: Option<Vec<&String>>,
        show_arch: bool,
    ) -> Result<()> {

        self.shared_lock()?;

        let mut combined = scan_result::ScanResult::default();
        for provider in self.filtered_providers() {
            if let Ok(data) = provider.load_file() {
                combined.merge(data.packages);
            }
        }

        // limit to package names containing the search term (or exact matches)
        if let Some(needle) = needle {
            combined.filter_package_fn(|pkg_name: &str| {
                if exact {
                    pkg_name == needle
                } else {
                    pkg_name.contains(needle)
                }
            });
        }

        // limit to only the specified channels
        if let Some(channels) = channels {
            combined.filter_channel_fn(|pkg_chans: &[String]| {
                pkg_chans.iter().any(|ref pkg_chan| channels.contains(pkg_chan))
            });
        }

        if !self.arch_filter.is_empty() {
            let arch : Vec<ArchMatcher> = self.arch_filter.iter().map(|v| ArchMatcher::from(v.as_str())).collect();
            combined.filter_arch_fn(|pkg_arch: Option<&str>| {
                arch.iter().any(|m| m.matches(pkg_arch))
            });
        }

        // recycled storage for lists of channels and archs while we iterate each package/version
        let mut chans : Vec<&str> = Vec::new();
        let mut archs : Vec<Option<&str>> = Vec::new();

        for (name, pkg_info) in &combined.packages {

            let mut sorted = Vec::new();
            pkg_info.versions.iter().map(|(v, i)| (version::Version::new(v), i)).collect_into(&mut sorted);
            sorted.sort_by(|a, b| a.0.cmp(&b.0));

            // limit to N number of versions per package
            let take = sorted.iter().rev().take(if limit > 0 { limit as usize } else { sorted.len() });

            if oneline {

                // foo 1.0.0 +stable 1.0.1-test +beta
                //
                // or with --show-arch:
                // foo 1.0.0 @noarch +stable 1.0.1-test @linux_x86_64 +beta
                //

                print!("{name}");
                for (version, vlist) in take {

                    if show_arch {
                        archs.clear();
                        for arch in vlist.iter().map(|info| info.arch.as_deref()) {
                            bsearch_insert(&mut archs, arch);
                        }

                        for arch in archs.iter() {

                            chans.clear();
                            for chan in vlist.iter().filter(|ent| ent.arch.as_deref() == *arch).flat_map(|ent| ent.channels.iter()).map(|c| c.as_str()) {
                                bsearch_insert(&mut chans, chan);
                            }

                            print!(" {version} @{}", arch.unwrap_or("noarch"));
                            if !chans.is_empty() {
                                print!(" +{}", chans.join(" +"));
                            }
                        }
                    } else {

                        chans.clear();
                        for info in vlist.iter() {
                            for chan in info.channels.iter() {
                                bsearch_insert(&mut chans, chan);
                            }
                        }

                        print!(" {version}");
                        if !chans.is_empty() {
                            print!(" +{}", chans.join(" +"));
                        }
                    }
                }
                println!();

            } else if json {

                // {"package":"foo","versions":[["1.0.0",["stable"]],["1.0.1-test",["beta"]]]}
                // or with --show-arch:
                // {"package":"foo","versions":[["1.0.0",["stable"],null],["1.0.1-test",["beta"],"linux_x86_64"]]}

                let mut json_versions = Vec::new();
                for (version, vlist) in take {

                    if show_arch {
                        archs.clear();
                        for arch in vlist.iter().map(|info| info.arch.as_deref()) {
                            bsearch_insert(&mut archs, arch);
                        }

                        for arch in archs.iter() {
                            chans.clear();
                            for chan in vlist.iter().filter(|ent| ent.arch.as_deref() == *arch).flat_map(|ent| ent.channels.iter()).map(|c| c.as_str()) {
                                bsearch_insert(&mut chans, chan);
                            }
                            json_versions.push(serde_json::json!([version.as_str(), chans, arch]));
                        }
                    } else {

                        chans.clear();
                        for info in vlist.iter() {
                            for chan in info.channels.iter() {
                                bsearch_insert(&mut chans, chan);
                            }
                        }
                        json_versions.push(serde_json::json!([version.to_string(), chans]));
                    }

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
                for (version, vlist) in take {

                    if show_arch {
                        archs.clear();
                        for arch in vlist.iter().map(|info| info.arch.as_deref()) {
                            bsearch_insert(&mut archs, arch);
                        }

                        for arch in archs.iter() {
                            print!("  {version} ({})", arch.unwrap_or("noarch"));
                            chans.clear();
                            for chan in vlist.iter().filter(|ent| ent.arch.as_deref() == *arch).flat_map(|ent| ent.channels.iter()).map(|c| c.as_str()) {
                                bsearch_insert(&mut chans, chan);
                            }
                            println!(" {}", chans.join(" "));
                        }
                    } else {

                        print!("  {version}");
                        chans.clear();
                        for info in vlist.iter() {
                            for chan in info.channels.iter() {
                                bsearch_insert(&mut chans, chan);
                            }
                        }
                        println!(" {}", chans.join(" "));
                    }


                }
            }
        }

        Ok(())
    }
}

/// insert into vec, maintain sorted ordering, no duplicates
///
/// binary search for inseration index, then insert there if not found
fn bsearch_insert<T: Ord>(vec: &mut Vec<T>, val: T) {
    if let Err(idx) = vec.binary_search(&val) {
        vec.insert(idx, val);
    }
}
