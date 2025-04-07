use crate::AResult;
use crate::fetch::*;
use crate::provider::Provide;
use httpsearch;
use indicatif::ProgressStyle;
use package::PackageID;
use serde_derive::Serialize;
use std::io::Write;

#[derive(Debug, Serialize)]
pub struct Http {
    url: String,
}

impl Http {
    pub fn new(url: String) -> Self {
        Http { url }
    }
}

impl scan_result::Scan for Http {
    fn scan(&self, arch_filter: Option<&[&str]>) -> anyhow::Result<scan_result::ScanResult> {
        httpsearch::full_scan(None, &self.url, None, arch_filter)
    }
}

impl Fetch for Http {
    fn fetch(&self, write: &mut dyn Write, pkg: &PackageID, url: &str) -> AResult<u64> {

        tracing::trace!(pkg=?pkg, url, "Http::fetch()");

        let msg = format!("{} {}", pkg.name, pkg.version);
        let size = httpsearch::get_size(None, url).ok();

        let status_mgr = bpmutil::status::global();
        let mut bar = status_mgr.add_task(Some("download"), Some(pkg.name.as_str()), size);
        bar.set_message(msg);
        bar.set_prefix("✓");
        if size.is_some() {
            bar.set_style(ProgressStyle::with_template(
                #[allow(clippy::literal_string_with_formatting_args)]
                " {spinner:.green} downloading {msg:.cyan} {wide_bar:.green} {bytes_per_sec} - {bytes}/{total_bytes} - {eta} "
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
        let mut write = bpmutil::SlowWriter::new(&mut write, std::time::Duration::from_millis(1));

        let ret = httpsearch::download(None, url, &mut write);

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

impl Provide for Http {}
