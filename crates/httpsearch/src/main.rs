use anyhow::Result;
use httpsearch::*;

fn main() -> Result<()> {

    let subscriber = tracing_subscriber::FmtSubscriber::builder()
        .without_time()
        .with_max_level(tracing::Level::INFO)
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .finish();

    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    let url = std::env::args().nth(1).expect("expected url");

    let mut url = String::from(&url);
    if !url.starts_with("http") {
        url.insert_str(0, "http://");
    }

    let packages = full_scan(None, &url)?;

    for (name, version_map) in &packages {
        //println!("{} {}", name, version_map.keys().last().unwrap());
        println!("{} {:?}", name, version_map.keys());
    }

    Ok(())
}

#[cfg(test)]
mod test {
    use super::*;

    fn join_url(parts: &[&str]) -> String {
        parts.iter().map(|&e| String::from(e)).collect::<Vec<_>>().as_slice().join("/")
    }

    /// NOTE: must be serving on localhost:8000 and must have the foo-1.0.0 package
    #[test]
    fn download_foo() -> Result<()> {
        let port = std::option_env!("PKG_PORT").or(Some("8000")).unwrap();
        let root_url = format!("http://localhost:{port}/pkg");

        let filename = "foo-1.0.0.bpm.tar";

        let mut file = std::fs::File::create(filename)?;
        let url = join_url(&[&root_url, "foo", filename].as_slice());
        download(None, &url, &mut file)?;

        let exists = std::path::Path::new(&filename).try_exists()?;
        assert!(exists);

        // cleanup
        std::fs::remove_file(&filename)?;

        Ok(())
    }

}
