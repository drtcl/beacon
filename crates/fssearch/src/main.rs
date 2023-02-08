use anyhow::Result;
use fssearch::*;
use std::path::Path;

fn main() -> Result<()> {

    let subscriber = tracing_subscriber::FmtSubscriber::builder()
        .without_time()
        .with_max_level(tracing::Level::INFO)
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .finish();

    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    let dir = std::env::args().nth(1).expect("expected dir");

    let packages = full_scan(Path::new(&dir))?;

    for (name, version_map) in &packages {
        println!("{} {:?}", name, version_map.keys());
    }

    Ok(())
}
