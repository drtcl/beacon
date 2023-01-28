use anyhow::Result;

pub use reqwest::blocking::Client;

pub fn strip_slash(s: &str) -> &str {
    match s.strip_suffix('/') {
        Some(s) => s,
        None => s,
    }
}

pub fn fetch_page(client: &Client, url: &str) -> Result<String> {
    let body = client.get(url).send()?.text()?;
    Ok(body)
}

pub fn scrape_links(body: &str) -> Vec<(String, String)> {
    let mut links = Vec::new();

    let doc = scraper::Html::parse_document(body);
    let a = scraper::Selector::parse("a").unwrap();

    for element in doc.select(&a) {
        let v = element.value();
        let link_url = strip_slash(v.attr("href").unwrap());
        let ihtml = element.inner_html();
        let link_text = strip_slash(&ihtml);

        links.push((link_url.to_string(), link_text.to_string()));
    }
    links
}

pub fn get_package_names_all(client: &Client, url: &str) -> Result<Vec<String>> {
    //println!("get_package_names_all {url}");

    let body = fetch_page(client, url)?;
    let links = scrape_links(&body);

    let pkgs = links
        .into_iter()
        .filter_map(|(u, t)| if u == t { Some(u) } else { None })
        .collect();

    Ok(pkgs)
}

pub fn get_package_versions_all(client: &Client, url: &str, pkg_name: &str) -> Result<Vec<String>> {
    //println!("get_package_versions_all {url} {pkg_name}");

    let mut url = String::from(url);
    url.push('/');
    url.push_str(pkg_name);

    //let body = fetch_page(&url)?;
    let body = fetch_page(client, &url)?;
    let links = scrape_links(&body);

    let versions = links
        .into_iter()
        .filter_map(|(u, t)| if u == t { Some(u) } else { None })
        .collect();

    Ok(versions)
}

pub fn download(client: &Client, url: &str, write: &mut dyn std::io::Write) -> Result<u64> {
    let mut resp = client.get(url).send()?;
    dbg!(&resp);

    let err = resp.error_for_status_ref();
    if err.is_ok() {
        return Ok(resp.copy_to(write)?);
    }
    resp.error_for_status()?;
    Ok(0)
}
