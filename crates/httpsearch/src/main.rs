use anyhow::Result;
use httpsearch::*;

fn join_url(parts: &[&str]) -> String {
    parts.iter().map(|&e| String::from(e)).collect::<Vec<_>>().as_slice().join("/")
}

fn main() -> Result<()> {
    let mut args = std::env::args().skip(1);
    let url = args.next();
    if url.is_none() {
        anyhow::bail!("expected url");
    }

    let mut root_url = String::from(strip_slash(&url.unwrap()));
    if !root_url.starts_with("http") {
        root_url.insert_str(0, "http://");
    }
    root_url.push_str("/pkg");

    // re-usable client connection
    let client = reqwest::blocking::Client::builder()
        .connect_timeout(std::time::Duration::from_secs(10))
        .build()?;

    let packages = get_package_names_all(&client, &root_url)?;

    for name in &packages {
        let versions = get_package_versions_all(&client, &root_url, name)?;
        println!("{name:-10} {versions:?}");
    }

    Ok(())
}

#[cfg(test)]
mod test {
    use super::*;

    /// NOTE: must be serving on localhost:8000 and must have the foo-1.0.0 package
    #[test]
    fn download_foo() -> Result<()> {

        let root_url = "http://localhost:8000/pkg";

        let client = reqwest::blocking::Client::builder()
            .connect_timeout(std::time::Duration::from_secs(10))
            .build()?;

        let filename = "foo-1.0.0.bpm.tar";
        let mut file = std::fs::File::create(filename)?;
        let url = join_url(&[root_url, "foo", "1.0.0", filename].as_slice());
        download(&client, &url, &mut file)?;

        let exists = std::path::Path::new(&filename).try_exists()?;
        assert!(exists);

        // cleanup
        std::fs::remove_file(&filename)?;

        Ok(())
    }

}
