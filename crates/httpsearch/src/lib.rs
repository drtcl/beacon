//#![allow(dead_code)]
#![allow(unused_variables)]

#![feature(extract_if)]
#![feature(let_chains)]

#[cfg(all(feature="rustls", feature="nativessl"))]
std::compile_error!("Use either rustls or nativessl feature, not both.");

use std::collections::HashMap;
use anyhow::Result;
use tracing::trace;
use url::Url;
use std::collections::BTreeMap;
use version::VersionString;

pub use reqwest::blocking::Client;
use anyhow::Context;

const DEFAULT_TIMEOUT: u64 = 5;
const CHANNEL_DIR_PREFIX : &str = "channel_";
const CHANNELS_FILE : &str = "channels.json";
const KV_FILE : &str = "kv.json";

type LinkText = String;
type LinkUrlStr = String;

type PackageName = String;
type PackageVersion = VersionString;
type PackagefileName = String;
type ChannelName = String;

// ChannelName -> [Version]
type ChannelList = HashMap<ChannelName, Vec<PackageVersion>>;

#[derive(Debug)]
pub struct VersionInfo {
    pub url: LinkUrlStr,
    pub filename: PackagefileName,
    pub channels: Vec<String>,
}

type VersionList = BTreeMap<PackageVersion, VersionInfo>;
type PackageList = BTreeMap<PackageName, VersionList>;

#[derive(Debug)]
struct Link {
    text: LinkText,
    url: LinkUrlStr
}

impl Link {
    fn version_info(&self) -> Option<(PackageVersion, scan_result::VersionInfo)> {
        if let Some((name, version)) = package::split_parts(&self.text) {
            return Some((
                version.into(),
                scan_result::VersionInfo {
                    uri: self.url.clone(),
                    filename: self.text.clone(),
                    channels: Vec::new(),
                }
            ));
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
    // test if link is named "channels.json" and that the url ends in "/channels.json"
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

fn fetch_page(client: &Client, url: &Url) -> Result<String> {

    //let head = client.head(url.as_str()).send()?;
    //println!("head {:?}", head);
    //println!("[head] headers {:?}", head.headers());

    let resp = client.get(url.as_str()).send()?;
    //println!("[get] headers {:?}", resp.headers());
    let body = resp.text()?;
    tracing::trace!(url=url.as_str(), "fetch, body size {}", body.len());
    Ok(body)
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
                    //url: href.to_string(),
                    url: url.to_string(),
                })
            }
        }
    }
    links
}

/// wrapper around fetch_page() and scrape_links()
fn scrape_links_from(client: &Client, url: &Url) -> Result<Vec<Link>> {
    let body = fetch_page(client, url)?;
    Ok(scrape_links(url, &body))
}

/// Scan an http server for packages
/// Accepted directory structures:
/// ```ignore
/// 1) flat -- packages listed at root
///     pkg/
///         foo-1.0.0.bpm
///         foo-2.0.0.bpm
///         bar-0.1.0.bpm
///
/// 2) organized -- packages in directories of package name (allows for adding channels)
///     pkg/
///         foo/
///             foo-1.0.0.bpm
///             foo-2.0.0.bpm
///             channels.json      (optional)
///             channel_stable/    (optional)
///                 foo-3.0.0.bpm
///         bar/
///             bar-0.1.0.bpm
/// ```
pub fn full_scan(timeout: Option<u64>, root_url: &str, pkg_name: Option<&str>)-> Result<scan_result::ScanResult> {

    let timeout = timeout.unwrap_or(DEFAULT_TIMEOUT);

    // re-usable client connection
    let client = reqwest::blocking::Client::builder()
        .connect_timeout(std::time::Duration::from_secs(timeout))
        .build()?;

    client_full_scan(&client, root_url, pkg_name)
}

/// see [full_scan]
pub fn client_full_scan(client: &Client, root_url: &str, filter_name: Option<&str>) -> Result<scan_result::ScanResult> {

    let mut report = scan_result::ScanResult::default();

    let mut root_url = std::borrow::Cow::Borrowed(root_url);
    if !root_url.ends_with('/') {
        root_url.to_mut().push('/');
    }
    let root_url = Url::parse(&root_url)?;

    trace!(url=?root_url, "full_scan");

    let links = scrape_links_from(client, &root_url)?;

    // split off any packages that are at the toplevel
    let (flat_package_links, links) = split_links(links, is_pkg_link);

    for link in flat_package_links {
        tracing::debug!("found at toplevel: {}", &link.text);
        if let Some((pkg_name, version)) = package::split_parts(&link.text) {
            if let Some((_version, info)) = link.version_info() {

                let mut add = true;

                if let Some(filter_name) = filter_name {
                    if filter_name != pkg_name {
                        add = false;
                    }
                }

                if add {
                    report.add_version(pkg_name, version, info);
                }
            }
        }
    }

    // scan again, but only scan items that don't have a '.' in the name, as
    // package names cannot contain a '.' and the ones that look like a
    // directory (have a trailing slash)
    // i.e. scan just directories that are package names
    let (pkg_dir_links, _) = split_links(links, |link| {
        !link.text.contains('.') && link.url.ends_with('/')
    });

    for link in pkg_dir_links {
        let pkg_name = strip_slash(&link.text);
        let mut scan = true;
        if let Some(filter_name) = filter_name {
            if filter_name != pkg_name {
                scan = false;
            }
        }

        if scan {
            scan_package_dir(client, &mut report, pkg_name, &link);
        }
    }

    Ok(report)
}

fn scan_package_dir(client: &Client, report: &mut scan_result::ScanResult, pkg_name: &str, dir_link: &Link) {

    let ret = PackageList::new();

    trace!(url=dir_link.url, "scanning package dir {}", pkg_name);

    // gather all links from this package dir
    let mut links = Vec::new();
    if let Ok(url) = Url::parse(&dir_link.url) {
        match scrape_links_from(client, &url) {
            Ok(l) => { links = l; },
            Err(_) => { return; }
        }
    }

    // (1) extract links to package files
    let (pkg_files, links) = split_links(links, is_pkg_link);

    // add all found packages to the report
    for pkg_link in pkg_files {
        if let Some((name, version)) = package::split_parts(&pkg_link.text) {
            if name != pkg_name {
                // this file doesn't belong in this dir
                // this is a package file for a package with a different name
                tracing::warn!("ignored package in wrong dir: {}", pkg_link.url);
            } else if let Some((version, info)) = pkg_link.version_info() {
                report.add_version(pkg_name, &version, info);
            }
        }
    }

    // (2) extract links to channel dirs
    let (channel_dirs, links) = split_links(links, is_channel_dir);

    // (3) extract link to channels.json
    let (channels_json, links) = split_links(links, is_channels_json);

    // (4) extract link to channels.json
    let (kv_json, links) = split_links(links, is_kv_json);

    // for each channel dir, scan for package files
    for channel in channel_dirs {
        if let Some(channel_name) = channel_dir_to_name(&channel.text) {
            tracing::trace!(channel=channel_name, url=channel.url, "scanning channel dir");
            if let Ok(url) = Url::parse(&channel.url) {
                let links = scrape_links_from(client, &url).unwrap_or_default();

                // add the package files found in this dir to the overall packages list
                let (channel_pkg_files, _) = split_links(links, is_pkg_link);
                for link in channel_pkg_files {
                    if let Some((link_pkg_name, version)) = package::split_parts(&link.text) {
                        if pkg_name == link_pkg_name {
                            if let Some((version, info)) = link.version_info() {
                                report.add_version(pkg_name, &version, info);
                                report.add_channel_version(pkg_name, channel_name, &version);
                            }
                        } else {
                            // this file doesn't belong in this dir
                            // this is a package file for a package with a different name
                            tracing::warn!("found package in wrong dir: {}", link.url);
                        }
                    }
                }
            }
        }
    }

    // parse the channels.json file and apply any channels to versions as it specifies
    if let Some(channels_json) = channels_json.first() {
        if let Ok(url) = Url::parse(&channels_json.url) {
            if let Ok(body) = fetch_page(client, &url) {
                match serde_json::from_str::<ChannelList>(&body) {
                    Ok(channels) => {
                        //println!("channels json: {:?}", channels);
                        for (chan_name, versions) in channels {
                            for v in versions {
                                report.add_channel_version(pkg_name, &chan_name, &v);
                            }
                        }
                    },
                    Err(_) => {
                        tracing::debug!("invalid json at {}", url.as_str());
                    }
                }
            }
        }
    }

    // parse the kv.json file and save the result
    if let Some(kv_url) = kv_json.first() {
        if let Ok(url) = Url::parse(&kv_url.url) {
            if let Ok(body) = fetch_page(client, &url) {
                if let Ok(kv) = serde_json::from_str(&body) {
                    report.add_kv(pkg_name, kv);
                }
            }
        }
    }
}

pub fn get_size(timeout: Option<u64>, url: &str) -> Result<u64> {

    let timeout = timeout.unwrap_or(DEFAULT_TIMEOUT);

    // re-usable client connection
    let client = reqwest::blocking::Client::builder()
        .connect_timeout(std::time::Duration::from_secs(timeout))
        .build()?;

    let url = Url::parse(url)?;

    let resp = client.head(url).send()?;

    let size = resp.headers()
        .get("content-length")
        .and_then(|s| s.to_str().ok())
        .context("no content-length")?;

    Ok(size.parse::<u64>()?)
}

pub fn download(timeout: Option<u64>, url: &str, write: &mut dyn std::io::Write) -> Result<u64> {

    let timeout = timeout.unwrap_or(DEFAULT_TIMEOUT);

    // re-usable client connection
    let client = reqwest::blocking::Client::builder()
        .connect_timeout(std::time::Duration::from_secs(timeout))
        .build()?;

    let url = Url::parse(url)?;

    client_download(&client, &url, write)
}

pub fn client_download(client: &Client, url: &Url, write: &mut dyn std::io::Write) -> Result<u64> {

    tracing::trace!("downloading {url}");

    // TODO probably want to find a better way to download,
    // this has the entire file in memory before writing to file
    let mut resp = client.get(url.as_str()).send()?;
    let err = resp.error_for_status_ref();
    if err.is_ok() {
        return Ok(resp.copy_to(write)?);
    }
    resp.error_for_status()?;
    Ok(0)
}
