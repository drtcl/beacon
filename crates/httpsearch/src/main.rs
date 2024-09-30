use anyhow::Result;
use httpsearch::*;

fn main() -> Result<()> {

    let subscriber = tracing_subscriber::FmtSubscriber::builder()
        .without_time()
        .with_max_level(tracing::Level::INFO)
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .finish();

    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    let mut args = std::env::args().skip(1);
    let url = args.next().expect("expected url");
    let pkg_name = args.next();
    let pkg_name = pkg_name.as_deref();

    let mut url = String::from(&url);
    if !url.starts_with("http") {
        url.insert_str(0, "http://");
    }

    let packages = full_scan(None, &url, pkg_name)?;
    for (name, pkg_info) in &packages.packages {
        //println!("{} {:?}", name, pkg_info.keys());
        println!("{}", name);
        if let Some(kv) = &pkg_info.kv {
            println!("  kv {}", serde_json::to_string_pretty(kv)?);
        }
        for (version, info) in &pkg_info.versions {
            print!("  {}", version);
            for chan in &info.channels {
                print!(" {}", chan);
            }
            println!();
        }
    }

    Ok(())
}

#[cfg(test)]
mod test {

    // make sure we can talk to https
    #[cfg(any(feature="rustls", feature="nativessl", feature="nativessl-vendored"))]
    #[test]
    fn https() {
        let client = reqwest::blocking::Client::new();
        let resp = client.get("https://google.com").send().unwrap();
        dbg!(&resp);
        let body = resp.bytes().unwrap();
        dbg!(&body);
    }

}
