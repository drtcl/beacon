use anyhow::Result;
use fssearch::*;
use std::path::Path;
use clap::arg;

fn main() -> Result<()> {

    let subscriber = tracing_subscriber::FmtSubscriber::builder()
        .without_time()
        .with_max_level(tracing::Level::INFO)
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .finish();

    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    let matches = clap::Command::new("fssearch")
        .arg(arg!(<dir> "base dir"))
        .arg(arg!(-p --pkg <pkg> "Search for a single package"))
        .arg(arg!(-a --arch <arch> "Search for a single architecture"))
        .get_matches();

    let dir = matches.get_one::<String>("dir").unwrap();
    let pkg = matches.get_one::<String>("pkg").map(|s| s.as_str());
    let arch = matches.get_one::<String>("arch").map(|s| s.as_str());

    let arch = arch.as_ref().map(|s| std::slice::from_ref(s));
    if let Some(a) = arch {
        for a in a {
            if !package::is_valid_arch(Some(a)) {
                println!("warning: {} is not a valid arch string", a);
            }
        }
    }

    let packages = full_scan(Path::new(&dir), pkg, arch)?;
    packages.print();

    Ok(())
}
