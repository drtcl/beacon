use std::collections::BTreeSet;
use std::collections::BTreeMap;
use super::App;
use anyhow::Result;
use crate::*;

impl App {

    pub fn list_cmd(&mut self, matches: &clap::ArgMatches) -> Result<()> {
        match matches.subcommand() {
            Some(("available", sub_matches)) => {
                self.list_available_cmd()?;
            }
            Some(("channels", sub_matches)) => {
                self.list_channels_cmd()?;
            }
            Some(("installed", sub_matches)) => {
                self.list_installed()?;
            }
            Some(_) => unreachable!(),
            None => {
                self.list_installed()?;
            }
        }

        Ok(())
    }

    /// list installed packages
    pub fn list_installed(&mut self) -> Result<()> {

        // if the db file doesn't exist, dont' attempt to load it, return 0 packages
        if !self.config.db_file.exists() {
            return Ok(());
        }

        self.load_db()?;

        let mut tw = tabwriter::TabWriter::new(std::io::stdout());
        //write!(&mut tw, "{}\t{}\t{}\n", "name", "version", "channel")?;
        for ent in self.db.installed.iter() {
            let channel = ent.versioning.channel.as_deref().unwrap_or("");
            let pinned = tern!(ent.versioning.pinned_to_version, "=", "^");
            writeln!(&mut tw, "{}\t{pinned}{}\t{}", ent.metadata.name, ent.metadata.version, channel)?;
        }
        tw.flush()?;
        Ok(())
    }

    /// list channels for a given package, or all packages
    pub fn list_channels_cmd(&mut self) -> Result<()> {
        let mut combined = search::PackageList::new();
        for provider in &self.config.providers {
            if let Ok(cache) = provider.load_cache() {
                combined = search::merge_package_lists(combined, cache);
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
            write!(&mut stdout, "\n")?;
        }
        Ok(())
    }

    /// list all versions of all packages that are available
    pub fn list_available_cmd(&mut self) -> Result<()> {

        let mut combined = search::PackageList::new();
        for provider in &self.config.providers {
            if let Ok(cache) = provider.load_cache() {
                combined = search::merge_package_lists(combined, cache);
            }
        }

        let mut stdout = std::io::stdout();

        let mut sorted = Vec::new();

        for (name, versions) in &combined {

            versions.keys().map(|v| version_compare::Version::from(v).unwrap()).collect_into(&mut sorted);
            sorted.sort_by(|a, b| a.compare(b).ord().unwrap_or(a.as_str().cmp(b.as_str())));

            write!(&mut stdout, "{}", name)?;
            for version in &sorted {
                if let Some(version_info) = versions.get(version.as_str()) {
                    write!(&mut stdout, " {}", version.as_str())?;
                    for channel in &version_info.channels {
                        write!(&mut stdout, " +{}", channel)?;
                    }
                }
            }
            write!(&mut stdout, "\n")?;
            sorted.clear();
        }

        //for (name, versions) in &combined {
        //    write!(&mut stdout, "{}", name)?;
        //    for (version, version_info) in versions {
        //        write!(&mut stdout, " {}", version)?;
        //        for channel in &version_info.channels {
        //            write!(&mut stdout, " +{}", channel)?;
        //        }
        //    }
        //    write!(&mut stdout, "\n")?;
        //}

        Ok(())
    }
}
