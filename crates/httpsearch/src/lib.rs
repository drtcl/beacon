//! Scan an http server for packages
//! ```ignore
//! Accepted directory structures:
//! 1) flat -- packages listed at root
//!     pkg/
//!         foo-1.0.0.bpm
//!         foo-2.0.0.bpm
//!         bar-0.1.0.bpm
//!
//! 2) organized -- packages in directories of package name (allows for adding channels)
//!     pkg/
//!         foo/
//!             foo-1.0.0.bpm
//!             foo-2.0.0.bpm
//!             channels.json      (optional)
//!             channel_stable/    (optional)
//!                 foo-3.0.0.bpm
//!         bar/
//!             bar-0.1.0.bpm
//! ```
//!
//! env vars:
//!   BPM_HTTP_THREADS = T
//!   BPM_HTTP_JOBS = J
//!
//!   T threads running J concurrent http requests

#![feature(extract_if)]

pub mod masync;

#[cfg(all(feature="rustls", feature="nativessl"))]
std::compile_error!("Use either rustls or nativessl feature, not both.");

use anyhow::Context;
use anyhow::Result;
use reqwest::blocking::Client;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Semaphore;
use tracing::trace;
use url::Url;
use version::VersionString;

const DEFAULT_TIMEOUT: u64 = 5;
const CHANNEL_DIR_PREFIX : &str = "channel_";
const CHANNELS_FILE : &str = "channels.json";
const KV_FILE : &str = "kv.json";

type LinkText = String;
type LinkUrlStr = String;

type PackageVersion = VersionString;
type ChannelName = String;

// ChannelName -> [Version]
type ChannelList = HashMap<ChannelName, Vec<PackageVersion>>;

#[derive(Debug)]
pub struct VersionInfo<'a> {
    pub version: &'a str,
    pub url: &'a str,
    pub filename: &'a str,
    pub channel: Option<&'a str>,
    pub arch: Option<&'a str>,
}

#[derive(Debug)]
struct Link {
    text: LinkText,
    url: LinkUrlStr
}

impl Link {
    /// split out info parts of a link
    fn version_info(&self) -> Option<VersionInfo> {
        if let Some((_name, version, arch)) = package::split_parts(&self.text) {
            return Some(VersionInfo {
                version,
                url: &self.url,
                filename: &self.text,
                channel: None,
                arch,
            });
        }
        None
    }
}

/// return true for links that look like package file names
fn is_pkg_link(link: &Link) -> bool {
    package::is_packagefile_name(&link.text)
}

// return true for links to directories that follow the naming of channel dirs
fn is_channel_dir(link: &Link) -> bool {
    link.url.ends_with('/') && link.text.starts_with(CHANNEL_DIR_PREFIX)
}

/// return true for links to a CHANNELS_FILE file
fn is_channels_json(link: &Link) -> bool {
    // test if link is named "channels.json" and that the url ends in "/channels.json"
    link.text == CHANNELS_FILE &&
        link.url.strip_suffix(CHANNELS_FILE).and_then(|s| s.strip_suffix('/')).is_some()
}

/// return true for links to a CHANNELS_FILE file
fn is_kv_json(link: &Link) -> bool {
    // test if link is named "kv.json" and that the url ends in "/kv.json"
    link.text == KV_FILE &&
        link.url.strip_suffix(KV_FILE).and_then(|s| s.strip_suffix('/')).is_some()
}

/// remove any trailing "/" from a &str, returning a &str of the same lifetime
fn strip_slash(s: &str) -> &str {
    match s.strip_suffix('/') {
        Some(s) => s,
        None => s,
    }
}

/// example: "channel_stable/" -> "stable"
fn channel_dir_to_name(dir: &str) -> Option<&str> {
    strip_slash(dir).strip_prefix(CHANNEL_DIR_PREFIX)
}

/// split (or filter or partition) links into two Vec<Link> (X, Y)
/// where X contains the ones that the filter function returns true for, and Y the ones that do not
fn split_links<F>(mut links: Vec<Link>, mut f: F) -> (Vec<Link>, Vec<Link>)
    where F: FnMut(&Link) -> bool
{
    let excluded = links.extract_if(.., |v| !f(v)).collect();
    (links, excluded)
}

/// return all links from a page
fn scrape_links(origin_url: &Url, body: &str) -> Vec<Link> {

    let doc = scraper::Html::parse_document(body);
    let a = scraper::Selector::parse("a").unwrap();

    let mut links = Vec::new();
    for element in doc.select(&a) {
        let v = element.value();
        if let Some(href) = v.attr("href") {
            let link_text = element.inner_html();

            // make the URL absolute
            if let Ok(url) = origin_url.join(href) {
                links.push(Link {
                    text: link_text,
                    url: url.to_string(),
                })
            }
        }
    }
    links
}

pub fn full_scan(timeout: Option<u64>, root_url: &str, pkg_name: Option<&str>, archs: Option<&[&str]>)-> Result<scan_result::ScanResult> {

    let threads = std::env::var("BPM_HTTP_THREADS")
        .ok()
        .and_then(|s| s.parse::<u32>().ok())
        .filter(|&n| n != 0)
        .unwrap_or(1) as usize;

    let jobs = std::env::var("BPM_HTTP_JOBS")
        .ok()
        .and_then(|s| s.parse::<u32>().ok())
        .filter(|&n| n != 0)
        .unwrap_or(1) as usize;

    tracing::trace!("http scan with {threads} threads, {jobs} jobs");

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(threads)
        .thread_name("http-scan")
        .enable_time()
        .enable_io()
        .build()
        .unwrap();

    let semaphore = Arc::new(Semaphore::new(jobs));

    let archs : Arc<Vec<String>> = Arc::new(
        match archs {
            None => Vec::new(),
            Some(s) => s.iter().map(|s| s.to_string()).collect(),
        }
    );

    runtime.block_on(masync::full_scan(semaphore, timeout, root_url, pkg_name, archs))
}

pub fn get_size(timeout: Option<u64>, url: &str) -> Result<u64> {

    let timeout = timeout.unwrap_or(DEFAULT_TIMEOUT);

    let url = Url::parse(url)?;

    let client = reqwest::blocking::Client::builder()
        .connect_timeout(std::time::Duration::from_secs(timeout))
        .build()?;

    let resp = client.head(url).send()?;

    let size = resp.headers()
        .get("content-length")
        .and_then(|s| s.to_str().ok())
        .context("no content-length")?;

    Ok(size.parse::<u64>()?)
}

pub fn download(timeout: Option<u64>, url: &str, write: &mut dyn std::io::Write) -> Result<u64> {

    let timeout = timeout.unwrap_or(DEFAULT_TIMEOUT);

    let url = Url::parse(url)?;

    let client = reqwest::blocking::Client::builder()
        .connect_timeout(std::time::Duration::from_secs(timeout))
        .build()?;

    client_download(&client, &url, write)
}

pub fn client_download(client: &Client, url: &Url, write: &mut dyn std::io::Write) -> Result<u64> {

    tracing::trace!("downloading {url}");

    let resp = client.get(url.as_str()).send()?;
    match resp.error_for_status() {
        Err(e) => {
            anyhow::bail!(e)
        }
        Ok(mut resp) => {
            Ok(resp.copy_to(write)?)
        }
    }
}
