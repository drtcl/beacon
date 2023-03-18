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

    let mut args = std::env::args().skip(1);
    let dir = args.next().expect("expected dir");
    let pkg_name = args.next();

    let packages = full_scan(Path::new(&dir), pkg_name.as_deref())?;
    for (name, version_map) in &packages {
        //println!("{} {:?}", name, version_map.keys());
        println!("{}", name);
        for (version, info) in version_map {
            print!("  {}", version);
            for chan in &info.channels {
                print!(" {}", chan);
            }
            println!();
        }
    }

    Ok(())
}
