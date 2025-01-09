use anyhow::Context;
use crate::AResult;
use crate::fetch::*;
use crate::provider::Provide;
use crate::search::*;
use package::PackageID;
use std::io::Write;
use std::path::{Path, PathBuf};
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};

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

impl scan_result::Scan for FileSystem {
    fn scan(&self) -> anyhow::Result<scan_result::ScanResult> {
        fssearch::full_scan(&self.root, None)
    }
}

impl Fetch for FileSystem {

    fn fetch(&self, write: &mut dyn Write, pkg: &PackageID, url: &str, bars: Option<&MultiProgress>) -> AResult<u64> {

        tracing::trace!(pkg=?pkg, url, "FileSystem::fetch()");

        let mut file = std::fs::File::open(url).context("open file for reading")?;

        #[cfg(unix)]
        let file_size = file.metadata().ok().map(|m| m.size());

        #[cfg(windows)]
        let file_size = file.metadata().ok().map(|m| m.file_size());

        let msg = format!("{} {}", pkg.name, pkg.version);
        let bar = if let Some(size) = file_size {
            ProgressBar::new(size)
                .with_message(msg)
                .with_prefix("✓")
                .with_style(ProgressStyle::with_template(
                    #[allow(clippy::literal_string_with_formatting_args)]
                    " {spinner:.green} copying {msg:.cyan} {bytes_per_sec} {wide_bar:.green} {bytes}/{total_bytes} - {eta} "
                ).unwrap())
        } else {
            ProgressBar::no_length()
                .with_message(msg)
                .with_prefix("✓")
                .with_style(ProgressStyle::with_template(
                    #[allow(clippy::literal_string_with_formatting_args)]
                    " {spinner:.green} copying {msg:.cyan} {bytes_per_sec} {bytes} "
                ).unwrap())
        };

        let bar = if let Some(bars) = bars {
            bars.add(bar)
        } else {
            bar
        };

        let mut write = bar.wrap_write(write);

        // [debug] slow
        //let mut write = bpmutil::SlowWriter::new(&mut write, std::time::Duration::from_millis(1));

        let ret = std::io::copy(&mut file, &mut write).context("copy file");

        let bar = if let Some(bars) = bars {
            bars.remove(&bar);
            bars.insert(0, bar)
        } else {
            bar
        };

        bar.set_style(ProgressStyle::with_template(
            #[allow(clippy::literal_string_with_formatting_args)]
            " {prefix:.green} copied  {msg:.cyan} {total_bytes} in {elapsed}").unwrap()
        );
        bar.finish();

        ret
    }
}

impl Provide for FileSystem {}
