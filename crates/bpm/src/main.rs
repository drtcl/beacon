#![feature(file_lock)]
#![feature(io_error_more)]
#![feature(iter_chain)]
#![feature(iter_collect_into)]
#![feature(let_chains)]
#![feature(thread_id_value)]

#![allow(dead_code)]
#![allow(unused_imports)]
//#![allow(unused_variables)]
//#![allow(unused_mut)]

mod app;
mod args;
mod config;
mod db;
mod fetch;
mod macros;
mod provider;
mod search;
mod source;

use anyhow::Context;
use anyhow::Result as AResult;
use camino::{Utf8Path, Utf8PathBuf};
use serde::{Deserialize, Serialize};
use std::io::Write;
use std::iter::Iterator;
use std::path::{Path, PathBuf};
use version::Version;

use crate::app::*;

/// Ensure that a dir path exists. Create dirs as needed.
fn create_dir<P: AsRef<Path>>(path: P) -> AResult<()> {
    let path = path.as_ref();

    // create the directory if it doesn't exist
    // if it does exist and it isn't a dir, that's a problem
    match (path.exists(), path.is_dir()) {
        (false, _) => {
            tracing::debug!("creating dir {path:?}");
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

/// search for the config file
/// 1. bpm_config.toml next to the executable
/// 2.     config.toml next to the executable
/// 3. bpm_config.toml in user's config dir
/// 4. bpm_config.toml in any parent dir from executable
fn find_config_file() -> AResult<Utf8PathBuf> {

    let cur_exe = Utf8PathBuf::from_path_buf(std::env::current_exe()?).map_err(|_| anyhow::anyhow!("invalid path, not utf8"))?;

    // bpm_config.toml next to executable
    let mut path = cur_exe.with_file_name("bpm_config.toml");
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
            let path = Utf8PathBuf::from_path_buf(path).map_err(|_| anyhow::anyhow!("invalid path, not utf8"))?;
            return Ok(path);
        }
    }

    // any bpm_config.toml in a parent dir
    let path = std::env::current_exe().context("failed to get current exe")?;
    for dir in path.ancestors().skip(1) {
        let path = dir.join("bpm_config.toml");
        if path.is_file() {
            let path = Utf8PathBuf::from_path_buf(path).map_err(|_| anyhow::anyhow!("invalid path, not utf8"))?;
            return Ok(path);
        }
    }

    Err(anyhow::anyhow!("cannot find config file"))
}

fn main() -> AResult<()> {

    // nice for debugging
    //let arg_log_path = std::env::current_exe().unwrap().with_file_name("bpm.log");
    //let mut arg_log = std::fs::OpenOptions::new().create(true).append(true).open(&arg_log_path).expect("open log");
    //let _ = arg_log.lock().expect("log file lock");
    //use chrono::SubsecRound;
    //let _ = write!(&mut arg_log, "{}", format!("{} {} {:?}\n",
    //    chrono::Local::now().format("%Y-%m-%d %H:%M:%S%.6f"),
    //    std::process::id(),
    //    std::env::args().collect::<Vec<_>>()
    //));

    let subscriber = tracing_subscriber::FmtSubscriber::builder()
        .without_time()
        .with_max_level(tracing::Level::TRACE)
        .with_env_filter(tracing_subscriber::EnvFilter::from_env("BPM_LOG"))
        //.with_writer(arg_log)
        .with_writer(std::io::stderr)
        .finish();

    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    let matches = args::get_cli().get_matches_from(wild::args());


    // shortcut to bpm-pack, no config file needed
    #[cfg(feature = "pack")]
    if let Some(("pack", matches)) = matches.subcommand() {
        return bpmpack::main_cli(matches);
    }

    // shortcut to swiss cmds, no config file needed
    #[cfg(feature = "swiss")]
    if let Some(("util", matches)) = matches.subcommand() {
        return swiss::main_cli(matches);
    }

    // find the config file
    let config_file = matches.get_one::<String>("config");
    let config_file = config_file.map_or_else(find_config_file, |s| Ok(Utf8PathBuf::from(s)))?;
    tracing::trace!("using config file {}", config_file);

    // load the config file
    config::store_config_path(config_file.canonicalize_utf8().context("failed to canonicalize config path")?);
    let config = config::Config::from_path(config_file).context("reading config file")?;

    // create main App struct
    let mut app = App::new(config);

    match matches.subcommand() {
        Some(("cache", sub_matches)) => {
            match sub_matches.subcommand() {
                Some(("clean", _matches)) => {
                    app.cache_clean()?;
                },
                Some(("clear", matches)) => {
                    let in_use = matches.get_flag("in-use");
                    app.cache_clear(in_use)?;
                },
                Some(("evict", matches)) => {
                    let pkg = matches.get_one::<String>("pkg").unwrap();
                    let version = matches.get_one::<String>("version");
                    let in_use = matches.get_flag("in-use");
                    app.cache_evict(pkg, version, in_use)?;
                },
                Some(("list", _matches)) => {
                    app.cache_list()?;
                },
                Some(("fetch", matches)) => {

                    let pkgs = matches.get_many::<String>("pkg")
                        .map_or(Vec::new(), |given| given.collect());

                    let arch = args::pull_many_opt(matches, "arch");
                    app.setup_arch_filter(arch);

                    app.provider_filter = args::parse_providers(matches);
                    app.cache_fetch(&pkgs)?;
                },
                Some(("touch", matches)) => {

                    let pkg = matches.get_one::<String>("pkg").unwrap();
                    let version = matches.get_one::<String>("version");
                    let duration = matches.get_one::<String>("duration").map(|s| {
                        humantime::parse_duration(s).expect("invalid time string")
                    });

                    app.cache_touch(pkg, version, duration)?;
                },
                _ => {
                    unreachable!();
                }
            }
        }
        Some(("list", sub_matches)) => {
            app.list_cmd(sub_matches)?;
        }
        Some(("scan", sub_matches)) => {

            let debounce = if let Some(d) = sub_matches.get_one::<String>("debounce") {
                Some(bpmutil::parse_duration_base(Some(d), std::time::Duration::from_secs(1))?)
            } else {
                None
            };

            let arch = args::pull_many_opt(sub_matches, "arch");
            app.setup_arch_filter(arch);

            app.provider_filter = args::parse_providers(sub_matches);

            app.scan_cmd(debounce)?;
        }
        Some(("install", sub_matches)) => {

            let no_pin = sub_matches.get_flag("no-pin");
            let pkg_name = sub_matches.get_one::<String>("pkg").unwrap();
            let update = sub_matches.get_flag("update");
            let reinstall = sub_matches.get_flag("reinstall");
            let target = sub_matches.get_one::<String>("target");

            let arch = args::pull_many_opt(sub_matches, "arch");
            app.setup_arch_filter(arch);

            app.provider_filter = args::parse_providers(sub_matches);
            app.install_cmd(pkg_name, no_pin, update, reinstall, target)?;
        }
        Some(("uninstall", sub_matches)) => {
            let pkg_name = sub_matches.get_one::<String>("pkg").unwrap();
            let verbose = sub_matches.get_flag("verbose");
            let remove_unowned = sub_matches.get_flag("remove-unowned");
            app.uninstall_cmd(pkg_name, verbose, remove_unowned)?;
        }
        Some(("update", sub_matches)) => {

            let pkg_names = sub_matches
                .get_many::<String>("pkg")
                .map_or(Vec::new(), |given| given.collect());

            app.provider_filter = args::parse_providers(sub_matches);

            app.setup_arch_filter(None);

            app.update_packages_cmd(&pkg_names)?;
        }
        Some(("pin", sub_matches)) => {
            let pkg_name = sub_matches.get_one::<String>("pkg").unwrap();
            let channel = sub_matches.get_one::<String>("channel").map(String::as_str);
            app.pin(pkg_name, channel)?;
        }
        Some(("unpin", sub_matches)) => {
            let pkg_name = sub_matches.get_one::<String>("pkg").unwrap();
            app.unpin(pkg_name)?;
        }
        Some(("verify", sub_matches)) => {

            // -q quiet option, don't print file status, just return good or bad exit code
            // --stop-on-first  option to stop on the first mismatch
            // --fail-fast?

            let verbose          = sub_matches.get_count("verbose").clamp(0, 2);
            let restore          = sub_matches.get_flag("restore");
            let restore_volatile = sub_matches.get_flag("restore-volatile");
            let fail_fast        = sub_matches.get_flag("fail-fast");
            let mtime            = sub_matches.get_flag("mtime");

            let pkg_names = sub_matches
                .get_many::<String>("pkg")
                .map_or(Vec::new(), |given| given.collect());

            app.verify_cmd(&pkg_names, restore, restore_volatile, fail_fast, verbose, mtime)?;
        }
        Some(("search", sub_matches)) => {
            let pkg_name = sub_matches.get_one::<String>("pkg").unwrap();
            let exact = sub_matches.get_flag("exact");

            let arch = args::pull_many_opt(sub_matches, "arch");
            app.setup_arch_filter(arch);

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
                    let show_type = sub_matches.get_flag("show-type");
                    let absolute  = sub_matches.get_flag("absolute");
                    app.query_files(pkg, depth.copied(), absolute, show_type)?;
                }
                Some(("kv", sub_matches)) => {

                    let names : Option<Vec<&str>> = sub_matches.get_many::<String>("pkg")
                        .map(|v| v.collect::<Vec<&String>>())
                        .map(|v| v.iter().map(|s| s.as_str()).collect());

                    let keys : Option<Vec<&str>> = sub_matches.get_many::<String>("keys")
                        .map(|v| v.collect::<Vec<&String>>())
                        .map(|v| v.iter().map(|s| s.as_str()).collect());

                    let arch = args::pull_many_opt(sub_matches, "arch");
                    app.setup_arch_filter(arch);

                    let provider = sub_matches.get_one::<String>("from-providers"); //.map(|s| s.as_str());
                    if let Some(provider) = provider {
                        if provider == "*" {
                            app.provider_filter = provider::ProviderFilter::empty();
                        } else {
                            app.provider_filter = provider::ProviderFilter::from_names(provider.split(','));
                        };
                        app.query_kv_provider(names.as_deref(), keys.as_deref())?;
                    } else {
                        app.query_kv(names.as_deref(), keys.as_deref())?;
                    }
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
