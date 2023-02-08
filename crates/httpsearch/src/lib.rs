//#![allow(dead_code)]
#![allow(unused_variables)]

#![feature(extract_if)]
#![feature(let_chains)]

use std::collections::HashMap;
use anyhow::Result;
use tracing::trace;
use url::Url;
use std::collections::BTreeMap;

pub use reqwest::blocking::Client;

const DEFAULT_TIMEOUT: u64 = 5;

type LinkText = String;
type LinkUrlStr = String;

type PackageName = String;
type PackageVersion = String;
type PackagefileName = String;
type ChannelName = String;

// ChannelName -> [Version]
type ChannelList = HashMap<ChannelName, Vec<PackageVersion>>;

/// PkgName -> ChannelList
type AllPackagesChannelList = HashMap<PackageName, ChannelList>;

#[derive(Debug)]
pub struct VersionInfo {
    pub url: LinkUrlStr,
    pub filename: PackagefileName,
    pub channels: Vec<String>,
    //pub channels: Vec<u16>,
}

type VersionList = BTreeMap<PackageVersion, VersionInfo>;
type PackageList = BTreeMap<PackageName, VersionList>;

pub fn strip_slash(s: &str) -> &str {
    match s.strip_suffix('/') {
        Some(s) => s,
        None => s,
    }
}

pub fn fetch_page(client: &Client, url: &Url) -> Result<String> {
    let body = client.get(url.as_str()).send()?.text()?;
    tracing::trace!(url=url.as_str(), "fetch, body size {}", body.len());
    Ok(body)
}

/// return all links from a page [(text, url), ...]
pub fn scrape_links(body: &str) -> Vec<(LinkText, LinkUrlStr)> {

    let doc = scraper::Html::parse_document(body);
    let a = scraper::Selector::parse("a").unwrap();

    let mut links = Vec::new();
    for element in doc.select(&a) {
        let v = element.value();
        if let Some(href) = v.attr("href") {
            let link_text = element.inner_html();
            links.push((link_text.to_string(), href.to_string()));
        }
    }
    links
}

/// Given urls to channel.json for each package, parse the files and build a list of versions for
/// channels for each package
pub fn scrape_channels(client: &Client, name_urls: Vec<(String, String)>) -> Result<AllPackagesChannelList> {

    let mut channels = AllPackagesChannelList::new();

    for (pkg_name, url) in name_urls {
        let body = fetch_page(client, &Url::parse(&url)?)?;
        let pkg_channels : ChannelList = serde_json::from_str(&body)?;
        channels.insert(pkg_name, pkg_channels);
    }

    Ok(channels)
}

//fn build_channel_map(channels: &AllPackagesChannelList) -> HashMap<&str, u16> {
//
//    let mut n = 0;
//    let mut channel_map = HashMap::new();
//    for (_pkg, chans) in channels {
//        for (chan_name, _versions) in chans {
//            channel_map.entry(chan_name.as_str()).or_insert_with(|| {
//                let val = n;
//                n += 1;
//                val
//            });
//        }
//    }
//    channel_map
//}

fn apply_channels(
    //channel_map: &HashMap<&str, u16>,
    channels: &AllPackagesChannelList, 
    packages: &mut PackageList) {

    for (pkg_name, chans) in channels {
        if let Some(map) = packages.get_mut(pkg_name) {
            for (chan_name, versions) in chans {
                //if let Some(channel_idx) = channel_map.get(chan_name.as_str()) {
                    for version in versions {
                        if let Some(packages) = map.get_mut(version) {
                            if !packages.channels.contains(chan_name) {
                                packages.channels.push(chan_name.clone());
                            }
                            //if !packages.channels.contains(channel_idx) {
                                //packages.channels.push(*channel_idx);
                            //}
                        }
                    }
                //}
            }
        }
    }
}

/// turn relative urls into absolute urls
fn munge_links(root_url: &Url, links: &mut Vec<(LinkText, LinkUrlStr)>) {
    for (_txt, url) in links {
        if let Ok(joined) = root_url.join(url) {
            *url = joined.as_str().to_string();
        }
    }
}

/// Scan an http server for packages
/// Accepted directory structures:
/// ```
/// 1) flat -- packages listed at root
///     pkg/
///         foo-1.0.0.bpm
///         foo-2.0.0.bpm
///         bar-0.1.0.bpm
/// 2) organized -- packages in directories of package name
///     pkg/
///         foo/
///             foo-1.0.0.bpm
///             foo-2.0.0.bpm
///             channels.json
///         bar/
///             bar-0.1.0.bpm
/// 3) organized-versioned -- packages in named and versioned directories
///     pkg/
///         foo/
///             1.0.0/
///                 foo-1.0.0.bpm
///             2.0.0/
///                 foo-2.0.0.bpm
///         bar/
///             0.1.0/
///                 bar-0.1.0.bpm
/// ```
pub fn full_scan(timeout: Option<u64>, root_url: &str)-> Result<PackageList> {

    let timeout = timeout.unwrap_or(DEFAULT_TIMEOUT);

    // re-usable client connection
    let client = reqwest::blocking::Client::builder()
        .connect_timeout(std::time::Duration::from_secs(timeout))
        .build()?;

    client_full_scan(&client, root_url)
}

/// see [full_scan]
pub fn client_full_scan(client: &Client, root_url: &str) -> Result<PackageList> {

    let mut root_url = std::borrow::Cow::Borrowed(root_url);
    if !root_url.ends_with('/') {
        root_url.to_mut().push('/');
    }

    trace!(url=?root_url, "full_scan");

    let root_url = Url::parse(&root_url)?;

    let body = fetch_page(client, &root_url)?;
    let mut links = scrape_links(&body);
    munge_links(&root_url, &mut links);

    let flat_package_links : Vec<(LinkText, LinkUrlStr)> = links
        .extract_if(|(text, _url)| package::is_packagefile_name(text))
        .collect();

    for (txt, _url) in &flat_package_links {
        tracing::debug!("found {}", txt);
    }

    // scan again, but only scan items that don't have a '.' in the name, as
    // package names cannot contain a '.' and the ones that look like a
    // directory (have a trailing slash)
    links.retain(|(text, _url)| {
        !text.contains('.') && text.ends_with('/')
    });

    let mut pkgs_with_channels = Vec::new();

    let name_links = links;
    let mut links = Vec::new();
    for (name, url) in name_links {

        tracing::trace!("scanning versions of package {}", name);

        if let Ok(url) = Url::parse(&url) {
            if let Ok(body) = fetch_page(client, &url) {

                let mut _links = scrape_links(&body);
                munge_links(&url, &mut _links);

                let name = strip_slash(&name);

                // remove the channels.json entry if it exists, save it for later
                if let Some(channels_idx) = _links.iter().position(|(name, _url)| name == "channels.json") {
                    let channels = _links.remove(channels_idx);
                    pkgs_with_channels.push((name.to_string(), channels.1));
                    tracing::trace!("{name} has channels");
                }

                // remove anything that looks like a package name but the package is named
                // something other than what this directory is named
                // example:
                //      foo/
                //          foo-1.0.0.bpm  (keep this)
                //          bar-1.0.0.bpm  (remove this)
                //          1.0.0/         (keep this)
                //          something_else (remove this)
                _links.retain(|(text, _url)| {
                    if package::is_packagefile_name(text) {
                        text.starts_with(strip_slash(name))
                    } else {
                        package::is_version_string(text)
                    }
                });

                links.extend(
                    _links
                    .into_iter()
                    .map(|(text, url)| {
                        (name.to_string(), text, url)
                    })
                );
            }
        }
    }

    // extract the ones that look like packages names
    let named_package_links : Vec<(LinkText, LinkUrlStr)> = links
        .extract_if(|(_name, text, _url)| package::is_packagefile_name(text))
        .map(|(_name, text, url)| (text, url))
        .collect();

    for (txt, _url) in &named_package_links {
        tracing::debug!("found {}", txt);
    }

    let version_links = links;
    let mut links = Vec::new();
    for (name, version, url) in version_links {
        if let Ok(url) = Url::parse(&url) {
            if let Ok(body) = fetch_page(client, &url) {
                let mut _links = scrape_links(&body);
                munge_links(&url, &mut _links);

                let version = strip_slash(&version);

                // remove anything that looks like a package name but the package is named
                // something other than what this directory is named
                // example:
                //      foo/
                //          1.0.0/
                //              foo-1.0.0.bpm  (keep this)
                //              foo-1.2.0.bpm  (remove this, bad version)
                //              bar-1.0.0.bpm  (remove this, bad name)
                //              something_else (remove this, doesn't look like a packagefile)
                _links.retain(|(text, _url)| {
                    if package::is_packagefile_name(text) {
                        if let Some((name_part, version_part)) = package::split_parts(text) {
                            if name_part.starts_with(&name) && version_part.starts_with(version) {
                                return true;
                            }
                        }
                    }
                    false
                });

                links.extend(_links);
            }
        }
    }

    // once again, save the ones that look like packages
    let versioned_package_links : Vec<(LinkText, LinkUrlStr)> =
        links.extract_if(|(text, _url)| {
            package::is_packagefile_name(text)
        })
        .collect();

    for (txt, _url) in &versioned_package_links {
        tracing::debug!("found {}", txt);
    }

    let mut packages = flat_package_links;
    packages.extend(named_package_links);
    packages.extend(versioned_package_links);

    let mut ret: PackageList = BTreeMap::new();
    for (filename, url) in packages {
        if let Some((pkgname, version)) = package::split_parts(&filename) {
            //println!("inserting {} {} {} {}", pkgname, version, filename, url);
            ret.entry(pkgname.to_string()).or_default()
                .insert(version.to_string(), VersionInfo{
                url,
                filename,
                channels: Vec::new(),
            });
        }
    }

    //dbg!(&pkgs_with_channels);
    let channels = scrape_channels(client, pkgs_with_channels)?;
    //dbg!(&channels);
    //println!("channels {:?}", channels);

    //let channel_map = build_channel_map(&channels);
    //dbg!(&channel_map);

    //apply_channels(&channel_map, &channels, &mut ret);
    apply_channels(&channels, &mut ret);

    Ok(ret)
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

    // TODO probably want to find a better way to download
    // this has the entire file in memory before writing to file
    let mut resp = client.get(url.as_str()).send()?;
    let err = resp.error_for_status_ref();
    if err.is_ok() {
        return Ok(resp.copy_to(write)?);
    }
    resp.error_for_status()?;
    Ok(0)
}
