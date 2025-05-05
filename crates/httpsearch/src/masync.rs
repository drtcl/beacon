use crate::*;
pub use reqwest::Client;
use std::sync::Arc;
use std::sync::Mutex;
use tokio::sync::Semaphore;

async fn fetch_page(semaphore: &Semaphore, client: &Arc<Client>, url: &Url) -> Result<String> {

    let _permit = semaphore.acquire().await.unwrap();

    let resp = client.get(url.as_str()).send().await?;
    //println!("[get] headers {:?}", resp.headers());
    let body = resp.text().await?;
    //println!("fetch_page body: {}", body);
    //tracing::trace!(url=url.as_str(), "fetch, body size {}", body.len());
    Ok(body)
}

async fn scrape_links_from(semaphore: &Semaphore, client: &Arc<Client>, url: &Url) -> Result<Vec<Link>> {

    let body = fetch_page(semaphore, client, url).await?;
    let links = scrape_links(url, &body);
    Ok(links)
}

async fn scan_package_dir(
    semaphore: Arc<Semaphore>,
    client: Arc<Client>,
    report: Arc<Mutex<scan_result::ScanResult>>,
    pkg_name: String,
    archs: Arc<Vec<String>>,
    dir_link: Link
) {

    trace!(url=dir_link.url, "scanning package dir {}", pkg_name);

    // gather all links from this package dir
    let mut links = Vec::new();
    if let Ok(url) = Url::parse(&dir_link.url) {
        match scrape_links_from(&semaphore, &client, &url).await {
            Ok(l) => { links = l; },
            Err(_) => { return; }
        }
    }

    // (1) extract links to package files
    let (pkg_files, links) = split_links(links, is_pkg_link);

    // add all found packages to the report
    for pkg_link in pkg_files {
        if let Some((name, _version, _arch)) = package::split_parts(&pkg_link.text) {
            if name != pkg_name {
                // this file doesn't belong in this dir
                // this is a package file for a package with a different name
                tracing::warn!("ignored package in wrong dir: {}", pkg_link.url);
            } else if let Some(info) = pkg_link.version_info() {
                if arch_filter(info.arch, &archs) {
                    let mut report = report.lock().unwrap();
                    report.add_version(&pkg_name, info.version, info.arch, info.channel, info.filename, info.url);
                }
            }
        }
    }

    // (2) extract links to channel dirs
    let (channel_dirs, links) = split_links(links, is_channel_dir);

    // (3) extract link to channels.json
    let (channels_json, links) = split_links(links, is_channels_json);

    // (4) extract link to kv.json
    let (kv_json, _links) = split_links(links, is_kv_json);

    let mut joinset = tokio::task::JoinSet::new();

    // for each channel dir, scan for package files
    for channel in channel_dirs {
        if let Some(channel_name) = channel_dir_to_name(&channel.text) {
            tracing::trace!(channel=channel_name, url=channel.url, "scanning channel dir");
            if let Ok(url) = Url::parse(&channel.url) {
                let semaphore = Arc::clone(&semaphore);
                let client = Arc::clone(&client);
                let report = Arc::clone(&report);
                let pkg_name = pkg_name.clone();
                let channel_name = channel_name.to_string();
                let archs = Arc::clone(&archs);
                joinset.spawn(async move {
                    let links = scrape_links_from(&semaphore, &client, &url).await.unwrap_or_default();

                    // add the package files found in this dir to the overall packages list
                    let (channel_pkg_files, _) = split_links(links, is_pkg_link);
                    let mut report = report.lock().unwrap();
                    for link in channel_pkg_files {
                        if let Some((link_pkg_name, _version, _arch)) = package::split_parts(&link.text) {
                            if pkg_name == link_pkg_name {
                                if let Some(info) = link.version_info() {
                                    if arch_filter(info.arch, &archs) {
                                        report.add_version(&pkg_name, info.version, info.arch, Some(channel_name.as_str()), info.filename, info.url);
                                    }
                                }
                            } else {
                                // this file doesn't belong in this dir
                                // this is a package file for a package with a different name
                                tracing::warn!("found package in wrong dir: {}", link.url);
                            }
                        }
                    }
                });
            }
        }
    }

    let kv = Arc::new(Mutex::new(None));
    let channels = Arc::new(Mutex::new(None));

    // parse the kv.json file and save the result
    if let Some(kv_url) = kv_json.first() {
        if let Ok(url) = Url::parse(&kv_url.url) {
            let semaphore = Arc::clone(&semaphore);
            let client = Arc::clone(&client);
            let kv = Arc::clone(&kv);
            joinset.spawn(async move {
                if let Ok(body) = fetch_page(&semaphore, &client, &url).await {
                    match serde_json::from_str(&body) {
                        Ok(val) => {
                            *kv.lock().unwrap() = Some(val);
                        }
                        Err(_) => {
                            tracing::debug!("kv file is invalid json");
                        }
                    }
                }
            });
        }
    }

    // parse the channels.json file saving it for later
    if let Some(channels_json) = channels_json.first() {
        if let Ok(url) = Url::parse(&channels_json.url) {
            let channels = Arc::clone(&channels);
            joinset.spawn(async move {
                if let Ok(body) = fetch_page(&semaphore, &client, &url).await {
                    match serde_json::from_str::<ChannelList>(&body) {
                        Ok(val) => {
                            //println!("channels json: {:?}", val);
                            *channels.lock().unwrap() = Some(val);
                        },
                        Err(_) => {
                            tracing::debug!("channels file is invalid json");
                        }
                    }
                }
            });
        }
    }

    // wait for everything to complete
    joinset.join_all().await;

    // apply explicit channels to versions as listed in the channels.json file.
    // We wait to do this until all versions have been added.
    if let Some(channels) = &mut *channels.lock().unwrap() {
        let channels = std::mem::take(channels);
        let mut report = report.lock().unwrap();
        for (chan_name, versions) in channels {
            for v in versions {
                report.insert_channel(&pkg_name, &v, &chan_name);
            }
        }
    }

    // store the kv in the package results. We wait to do this until after all verions are handled
    // because we only save the kv if the pacakge exists, and it will only exist if at least one
    // version exists.
    if let Some(kv) = &mut *kv.lock().unwrap() {
        let kv = std::mem::take(kv);
        let mut report = report.lock().unwrap();
        report.add_kv(&pkg_name, kv);
    };

}

/// return true if name passes the name filter
fn name_filter(name: &str, filter: Option<&str>) -> bool {
    if let Some(filter) = filter {
        name == filter
    } else {
        true
    }
}

/// return true if arch passes the arch filters
fn arch_filter(arch: Option<&str>, filters: &Vec<String>) -> bool {

    if filters.is_empty() {
        return true;
    }

    for f in filters {
        if package::ArchMatcher::from(f.as_str()).matches(arch) {
            return true
        }
    }

    false
}

pub async fn full_scan(
    semaphore: Arc<Semaphore>,
    _timeout: Option<u64>,
    root_url: &str,
    filter_name: Option<&str>,
    archs: Arc<Vec<String>>,
) -> Result<scan_result::ScanResult> {

    let client = reqwest::Client::new();
    let client = Arc::new(client);

    let mut report = scan_result::ScanResult::default();

    let mut root_url = std::borrow::Cow::Borrowed(root_url);
    if !root_url.ends_with('/') {
        root_url.to_mut().push('/');
    }
    let root_url = Url::parse(&root_url)?;

    trace!(url=?root_url, "full_scan");

    let links = scrape_links_from(&semaphore, &client, &root_url).await?;

    // split off any packages that are at the toplevel
    let (flat_package_links, links) = split_links(links, is_pkg_link);

    for link in flat_package_links {
        tracing::debug!("found at toplevel: {}", &link.text);
        if let Some((pkg_name, version, arch)) = package::split_parts(&link.text) {
            if let Some(info) = link.version_info() {
                if name_filter(pkg_name, filter_name) && arch_filter(arch, &archs) {
                    report.add_version(pkg_name, version, info.arch, None, info.filename, info.url);
                }
            }
        }
    }

    // scan again, but only scan items that don't have a '.' in the name, as
    // package names cannot contain a '.' and the ones that look like a
    // directory (have a trailing slash)
    // i.e. scan just directories that are package names
    let (mut pkg_dir_links, _) = split_links(links, |link| {
        !link.text.contains('.') && link.url.ends_with('/')
    });

    let report = Arc::new(Mutex::new(report));
    let mut joinset = tokio::task::JoinSet::new();

    // if filtering, remove non-matching package dirs
    if let Some(filter_name) = filter_name {
        pkg_dir_links.retain(|link| name_filter(strip_slash(&link.text), Some(filter_name)));
    }

    for link in pkg_dir_links {

        let pkg_name = strip_slash(&link.text).to_string();
        let client = Arc::clone(&client);
        let report = Arc::clone(&report);
        let semaphore = Arc::clone(&semaphore);
        let archs = Arc::clone(&archs);
        let _handle = joinset.spawn(async move {
            scan_package_dir(semaphore, client, report, pkg_name, archs, link).await
        });
    }

    // wait for everything to complete
    joinset.join_all().await;

    let report = Arc::into_inner(report).unwrap().into_inner()?;
    Ok(report)
}
