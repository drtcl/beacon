use crate::AResult;
use crate::fetch::*;
use crate::provider::Provide;
use crate::search::*;
use httpsearch;
use package::PackageID;
use serde_derive::Serialize;
use std::io::Write;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};

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
    fn fetch(&self, write: &mut dyn Write, pkg: &PackageID, url: &str, bars: Option<&MultiProgress>) -> AResult<u64> {

        tracing::trace!(pkg=?pkg, url, "Http::fetch()");

        //let client = httpsearch::Client::new();

        let msg = format!("{} {}", pkg.name, pkg.version);
        let bar = if let Ok(size) = httpsearch::get_size(None, url) {
            ProgressBar::new(size)
                .with_message(msg)
                .with_prefix("✓")
                .with_style(ProgressStyle::with_template(
                    #[allow(clippy::literal_string_with_formatting_args)]
                    " {spinner:.green} downloading {msg:.cyan} {wide_bar:.green} {bytes_per_sec} - {bytes}/{total_bytes} - {eta} "
                ).unwrap())
        } else {
            ProgressBar::no_length()
                .with_message(msg)
                .with_prefix("✓")
                .with_style(ProgressStyle::with_template(
                    #[allow(clippy::literal_string_with_formatting_args)]
                    " {spinner:.green} downloading {msg:.cyan} {bytes_per_sec} {bytes} "
                ).unwrap())
        };

        let bar = if let Some(bars) = bars {
            bars.insert_from_back(1, bar)
        } else {
            bar
        };

        let mut write = bar.wrap_write(write);

        // [debug] slow
        //let mut write = bpmutil::SlowWriter::new(&mut write, std::time::Duration::from_millis(1));

        let n = httpsearch::download(None, url, &mut write)?;

        let bar = if let Some(bars) = bars {
            bars.remove(&bar);
            bars.insert(0, bar)
        } else {
            bar
        };

        bar.set_style(ProgressStyle::with_template(
            #[allow(clippy::literal_string_with_formatting_args)]
            " {prefix:.green} downloaded  {msg:.cyan} {total_bytes} in {elapsed}").unwrap()
        );
        bar.finish();

        Ok(n)
    }
}

impl Provide for Http {}
