use anyhow::Result;
use httpsearch::*;
use clap::arg;

fn main() -> Result<()> {

    let subscriber = tracing_subscriber::FmtSubscriber::builder()
        .without_time()
        .with_max_level(tracing::Level::INFO)
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .finish();

    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    let matches = clap::Command::new("httpsearch")
        .arg(arg!(<url> "base url"))
        .arg(arg!(-p --pkg <pkg> "Search for a single package"))
        .arg(arg!(-a --arch <arch> "Search for a single architecture"))
        .get_matches();

    let url = matches.get_one::<String>("url").unwrap();
    let pkg = matches.get_one::<String>("pkg").map(|s| s.as_str());
    let arch = matches.get_one::<String>("arch").map(|s| s.as_str());

    let mut url = String::from(url);
    if !url.starts_with("http") {
        url.insert_str(0, "http://");
    }

    let arch = arch.as_ref().map(std::slice::from_ref);
    if let Some(a) = arch {
        for a in a {
            if *a != "*" && !package::is_valid_arch(Some(a)) {
                println!("warning: {} is not a valid arch string", a);
            }
        }
    }

    let packages = full_scan(None, &url, pkg, arch)?;
    packages.print();

    Ok(())
}

#[cfg(test)]
mod test {

    // make sure we can talk to https
    #[cfg(any(feature="rustls", feature="nativessl", feature="nativessl-vendored"))]
    #[test]
    fn https() {
        let client = reqwest::blocking::Client::new();
        let resp = client.get("https://github.com").send().unwrap();
        dbg!(&resp);
        let body = resp.bytes().unwrap();
        dbg!(&body);
    }

}
