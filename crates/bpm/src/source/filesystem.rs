use anyhow::Context;
use crate::AResult;
use crate::fetch::*;
use crate::provider::Provide;
use indicatif::ProgressStyle;
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

impl scan_result::Scan for FileSystem {
    fn scan(&self, arch_filter: Option<&[&str]>) -> anyhow::Result<scan_result::ScanResult> {
        fssearch::full_scan(&self.root, None, arch_filter)
    }
}

impl Fetch for FileSystem {

    fn fetch(&self, write: &mut dyn Write, pkg: &PackageID, url: &str) -> AResult<u64> {

        tracing::trace!(pkg=?pkg, url, "FileSystem::fetch()");

        let mut file = std::fs::File::open(url).context("open file for reading")?;

        #[cfg(unix)]
        let file_size = file.metadata().ok().map(|m| m.size());

        let msg = format!("{} {}", pkg.name, pkg.version);

        #[cfg(windows)]
        let file_size = file.metadata().ok().map(|m| m.file_size());

        let status_mgr = bpmutil::status::global();
        let mut bar = status_mgr.add_task(Some("download"), Some(pkg.name.as_str()), file_size);
        bar.set_message(msg);
        bar.set_prefix("✓");

        if file_size.is_some() {
            bar.set_style(ProgressStyle::with_template(
                    #[allow(clippy::literal_string_with_formatting_args)]
                    " {spinner:.green} downloading {msg:.cyan} {bytes_per_sec} {wide_bar:.green} {bytes}/{total_bytes} - {eta} "
            ).unwrap());
        } else {
            bar.set_style(ProgressStyle::with_template(
                    #[allow(clippy::literal_string_with_formatting_args)]
                    " {spinner:.green} downloading {msg:.cyan} {bytes_per_sec} {bytes} "
            ).unwrap());
        }

        let mut write = bar.wrap_write(write);

        // [debug] slow
        #[cfg(feature="dev-debug-slow")]
        let mut write = bpmutil::SlowWriter::new(&mut write, std::time::Duration::from_micros(900));

        let ret = std::io::copy(&mut file, &mut write).context("copy file");

        status_mgr.remove(&bar);
        status_mgr.insert(0, &mut bar);

        if ret.is_ok() {
            bar.set_style(ProgressStyle::with_template(
                #[allow(clippy::literal_string_with_formatting_args)]
                " {prefix:.green} downloaded  {msg:.cyan} {total_bytes} in {elapsed}").unwrap()
            );
        } else {
            bar.set_style(ProgressStyle::with_template(
                #[allow(clippy::literal_string_with_formatting_args)]
                " {prefix:.red} error       {msg:.cyan} fetch failed").unwrap()
            );
        }
        bar.finish();

        ret
    }
}

impl Provide for FileSystem {}
