#![feature(let_chains)]
#![feature(iter_collect_into)]
#![feature(fs_try_exists)]

#![allow(dead_code)]
#![allow(unused_imports)]
#![allow(unused_variables)]

mod config;
mod macros;
mod db;
mod fetch;
mod provider;
mod search;
mod source;
mod app;
mod args;

use anyhow::Context;
use anyhow::Result as AResult;
use camino::{Utf8Path, Utf8PathBuf};
use serde::{Deserialize, Serialize};
use std::io::Seek;
use std::io::Write;
use std::iter::Iterator;
use std::path::{Path, PathBuf};
use version::Version;

use crate::app::*;

fn create_dir<P: AsRef<Path>>(path: P) -> AResult<()> {
    let path = path.as_ref();

    // create the directory if it doesn't exist
    // if it does exist and it isn't a dir, that's a problem
    match (path.exists(), path.is_dir()) {
        (false, _) => {
            println!("creating dir {path:?}");
            std::fs::create_dir_all(path).context("failed to create directory")?;
            Ok(())
        }
        (true, false) => Err(anyhow::anyhow!("dir path exists, but is not a directory")),
        (true, true) => {
            //println!("[ok] dir {path:?} already exists");
            Ok(())
        },
    }
}

//fn path_rewrite(mut path: String) -> String {
//    let needles = ["<ROOT>", "<PWD>"];
//    for needle in needles {
//        path = path.replace(needle, "XX");
//    }
//    path
//}

/// search for the config file
/// 1. bpm_config.toml next to the executable
/// 2.     config.toml next to the executable
/// 3. bpm_config.toml in user's config dir
/// 4. bpm_config.toml in any parent dir from executable
fn find_config_file() -> AResult<PathBuf> {

    // bpm_config.toml next to executable
    let mut path = std::env::current_exe()?.with_file_name("bpm_config.toml");
    if path.is_file() {
        return Ok(path);
    }

    // config.toml next to executable
    path.set_file_name("config.toml");
    if path.is_file() {
        return Ok(path);
    }

    // bpm_config.toml in user's config dir
    if let Some(base) = directories::BaseDirs::new() {
        let config_dir = base.config_local_dir();
        let path = config_dir.join("bpm_config.toml");
        if path.is_file() {
            return Ok(path);
        }
    }

    // any bpm_config.toml in a parent dir
    let path = std::env::current_exe().context("failed to get current exe")?;
    for dir in path.ancestors().skip(1) {
        let path = dir.join("bpm_config.toml");
        if path.is_file() {
            return Ok(path);
        }
    }

    Err(anyhow::anyhow!("cannot find config file"))
}

fn main() -> AResult<()> {

    let subscriber = tracing_subscriber::FmtSubscriber::builder()
        .without_time()
        .with_max_level(tracing::Level::TRACE)
        .with_env_filter(tracing_subscriber::EnvFilter::from_env("BPM_LOG"))
        .finish();

    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    let matches = args::get_cli().get_matches();

    // shortcut to bpm-pack, no config file needed
    #[cfg(feature = "pack")]
    match matches.subcommand() {
        Some(("pack", matches)) => {
            return bpmpack::cli_main(matches);
        }
        _ => {}
    }

    // find the config file
    let config_file = matches.get_one::<String>("config");
    let config_file = config_file.map_or_else(find_config_file, |s| Ok(PathBuf::from(s)))?;
    tracing::trace!("using config file {}", config_file.display());

    // load the config file
    let mut app = App {
        config: config::Config::from_path(config_file).context("reading config file")?,
        db: db::Db::new(),
        db_loaded: false,
    };

    match matches.subcommand() {
        Some(("cache", sub_matches)) => {
            match sub_matches.subcommand() {
                Some(("clear", _args)) => {
                    app.cache_clear()?;
                },
                Some(("touch", args)) => {

                    let filename = args.get_one::<String>("pkg").unwrap();
                    let duration = args.get_one::<String>("duration").map(|s| {
                        humantime::parse_duration(s).expect("invalid time string")
                    });

                    app.cache_touch(filename, duration)?;
                },
                _ => {
                    todo!("NYI");
                }
            }
        }
        Some(("list", sub_matches)) => {
            app.list_cmd(sub_matches)?;
        }
        Some(("scan", sub_matches)) => {

            let provider_filter = args::parse_providers(sub_matches);

            let debounce = sub_matches.get_one::<String>("debounce");
            let debounce : std::time::Duration = if let Some(s) = debounce {
                if let Ok(s) = s.parse::<u64>() {
                    std::time::Duration::from_secs(s)
                } else if let Ok(d) = humantime::parse_duration(s) {
                    d
                } else {
                    anyhow::bail!("invalid debounce time");
                }
            } else {
                std::time::Duration::from_secs(0)
            };

            app.scan_cmd(provider_filter, debounce)?;
        }
        Some(("install", sub_matches)) => {

            let no_pin = sub_matches.get_one::<bool>("no-pin").unwrap();
            let pkg_name = sub_matches.get_one::<String>("pkg").unwrap();
            app.install_cmd(pkg_name, *no_pin)?;
        }
        Some(("uninstall", sub_matches)) => {
            let pkg_name = sub_matches.get_one::<String>("pkg").unwrap();
            println!("removing package {pkg_name}");
            app.uninstall_cmd(pkg_name)?;
        }
        Some(("update", sub_matches)) => {

            let pkg_names = sub_matches
                .get_many::<String>("pkg")
                .map_or(Vec::new(), |given| given.collect());

            app.update_packages_cmd(&pkg_names)?;
        }
        Some(("verify", sub_matches)) => {

            // -q quiet option, don't print file status, just return good or bad exit code
            // --stop-on-first  option to stop on the first mismatch
            // --fail-fast?

            let verbose = *sub_matches.get_one::<bool>("verbose").unwrap();
            let restore = *sub_matches.get_one::<bool>("restore").unwrap();
            let mtime   = *sub_matches.get_one::<bool>("mtime").unwrap();

            let pkg_names = sub_matches
                .get_many::<String>("pkg")
                .map_or(Vec::new(), |given| given.collect());

            app.verify_cmd(&pkg_names, restore, verbose, mtime)?;
        }
        Some(("search", sub_matches)) => {
            let pkg_name = sub_matches.get_one::<String>("pkg").unwrap();
            let exact = *sub_matches.get_one::<bool>("exact").unwrap();
            //println!("searching for package {}", pkg_name);
            app.search_cmd(pkg_name, exact)?;
        }
        Some(("query", sub_matches)) => {
            match sub_matches.subcommand() {
                Some(("owner", sub_matches)) => {
                    let file = sub_matches.get_one::<String>("file").unwrap();
                    app.query_owner(file)?;
                }
                Some(("list-files", sub_matches)) => {
                    let pkg = sub_matches.get_one::<String>("pkg").unwrap();
                    let depth = sub_matches.get_one::<u32>("depth");
                    let show_type = *sub_matches.get_one::<bool>("show-type").unwrap();
                    let absolute  = *sub_matches.get_one::<bool>("absolute").unwrap();
                    app.query_files(pkg, depth.copied(), absolute, show_type)?;
                }
                _ => {
                    unreachable!();
                }
            }
        }
        _ => {
            unreachable!();
        }
    }

    Ok(())
}

//    //---
//    let r = humantime::parse_duration("2d30s1ms");
//    dbg!(&r);
//    let d = r?;
//    let d = std::time::Duration::from_secs(d.as_secs());
//    let d = humantime::format_duration(d);
//    println!("{}", d.to_string());
//
//    let now = chrono::Utc::now();
//    let then = now + chrono::Duration::seconds(120) + chrono::Duration::milliseconds(123);
//    let dur = then - now;
//    let d = humantime::format_duration(dur.to_std()?);
//    println!("{}", d.to_string());
//    //---
