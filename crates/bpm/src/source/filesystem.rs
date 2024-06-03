use anyhow::Context;
use crate::AResult;
use crate::fetch::*;
use crate::provider::Provide;
use crate::search::*;
use package::PackageID;
use std::io::Write;
use std::path::{Path, PathBuf};

#[cfg(unix)]
use std::os::unix::fs::MetadataExt;

#[cfg(windows)]
use std::os::windows::fs::MetadataExt;

#[derive(Debug)]
pub struct FileSystem {
    root: PathBuf,
}

impl FileSystem {
    pub fn new<P: AsRef<Path>>(path: P) -> Self {
        FileSystem {
            root: PathBuf::from(path.as_ref()),
        }
    }
}

impl From<fssearch::VersionInfo> for VersionInfo {
    fn from(value: fssearch::VersionInfo) -> Self {
        Self {
            url: value.url,
            filename: value.filename,
            channels: value.channels,
        }
    }
}

impl Search for FileSystem {
    fn search(&self, needle: &str) -> AResult<PackageList> {
        tracing::debug!(needle, root=?&self.root, "[Search] FileSystem::search");

        let mut pkgs = fssearch::full_scan(&self.root, None)?;

        pkgs.retain(|name, _versions| {
            name.contains(needle)
        });

        let mut ret = PackageList::new();
        for (name, versions) in pkgs {
            for (version, urlfilename) in versions {
                ret.entry(name.to_string())
                    .or_default()
                    .insert(version, VersionInfo::from(urlfilename));
            }
        }
        Ok(ret)
    }

    fn scan(&self) -> AResult<PackageList> {
        Ok(self.search("").ok().unwrap_or_default())
    }
}

impl Fetch for FileSystem {

    fn fetch(&self, write: &mut dyn Write, pkg: &PackageID, url: &str) -> AResult<u64> {

        tracing::trace!(pkg=?pkg, url, "FileSystem::fetch()");

        let mut file = std::fs::File::open(url).context("open file for reading")?;

        #[cfg(unix)]
        let file_size = file.metadata().ok().map(|m| m.size());

        #[cfg(windows)]
        let file_size = file.metadata().ok().map(|m| m.file_size());

        let bar = indicatif::ProgressBar::new(0);
        bar.set_message(pkg.name.clone());

        if let Some(size) = file_size {
            bar.set_style(indicatif::ProgressStyle::with_template(
                " {spinner:.green} downloading {msg} - {bytes_per_sec} {wide_bar:.green} {bytes}/{total_bytes} - [{eta}] "
            ).unwrap());
            bar.set_length(size);
        } else {
            bar.set_style(indicatif::ProgressStyle::with_template(
                " {spinner:.green} downloading {msg} {bytes_per_sec} {bytes} "
            ).unwrap());
        }

        let mut write = bar.wrap_write(write);
        //let mut write = bpmutil::SlowWriter::new(&mut write, std::time::Duration::from_millis(1));

        std::io::copy(&mut file, &mut write).context("copy file")
    }
}

impl Provide for FileSystem {}
