use crate::AResult;
use crate::fetch::*;
use crate::provider::Provide;
use crate::search::*;
use httpsearch;
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
    fn scan(&self) -> anyhow::Result<scan_result::ScanResult> {
        httpsearch::full_scan(None, &self.url, None)
    }
}

impl Fetch for Http {
    fn fetch(&self, write: &mut dyn Write, pkg: &PackageID, url: &str) -> AResult<u64> {

        tracing::trace!(pkg=?pkg, url, "Http::fetch()");

        let bar = indicatif::ProgressBar::new(0);
        bar.set_style(
            indicatif::ProgressStyle::with_template(
                " {spinner:.green} downloading {msg} {bytes_per_sec} {bytes} "
            ).unwrap()
        );

        bar.set_message(pkg.name.clone());

        let mut write = bar.wrap_write(write);
        //let mut write = bpmutil::SlowWriter::new(&mut write, std::time::Duration::from_millis(1));

        //let client = httpsearch::Client::new();
        let n = httpsearch::download(None, url, &mut write)?;

        bar.finish_and_clear();

        Ok(n)
    }
}

impl Provide for Http {}
