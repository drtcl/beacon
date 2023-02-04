#![feature(let_chains)]
#![allow(dead_code)]
//#![allow(unused_imports)]
//#![allow(unused_variables)]

mod config;
mod db;
mod fetch;
mod package;
mod provider;
mod search;
mod source;
mod app;

use anyhow::Context;
use anyhow::Result as AResult;
use clap::Arg;
use package::PackageID;
use semver::Version;
use serde::{Deserialize, Serialize};
use std::io::Seek;
use std::io::Write;
use std::iter::Iterator;
use std::path::{Path, PathBuf};

use crate::app::*;

const PACKAGE_EXT: &str = ".bpm.tar";

fn create_dir<P: AsRef<Path>>(path: P) -> AResult<()> {
    let path = path.as_ref();

    // create the directory if it doesn't exist
    // if it does exist and it isn't a dir, that's a problem
    match (path.exists(), path.is_dir()) {
        (false, _) => {
            println!("creating dir {path:?}");
            std::fs::create_dir(path).context("failed to create directory")?;
            Ok(())
        }
        (true, false) => Err(anyhow::anyhow!("dir path exists, but is not a directory")),
        (true, true) => {
            println!("[ok] dir {path:?} already exists");
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

fn main() -> AResult<()> {

    let matches = clap::Command::new("bpm")
        .version("0.1.0")
        .about("Bryan's Package Manager : bpm")
        .author("Bryan Splitgerber")
        .subcommand_required(true)
        .subcommand(
            clap::Command::new("search")
                .about("search for packages")
                .arg(Arg::new("pkg").action(clap::ArgAction::Set).required(true)),
        )
        .subcommand(clap::Command::new("list").about("list currently installed packages"))
        .subcommand(
            clap::Command::new("install")
                .about("install new packages")
                .arg(
                    Arg::new("pkg")
                        .help("package name or path to local package file")
                        .action(clap::ArgAction::Set)
                        .required(true),
                ),
        )
        .subcommand(
            clap::Command::new("uninstall")
                .alias("remove")
                .about("remove installed packages")
                .arg(
                    Arg::new("pkg")
                        .help("package name or path to local package file")
                        .action(clap::ArgAction::Set)
                        .required(true),
                ),
        )
        .subcommand(
            clap::Command::new("verify")
                .about("perform consistency check on package state")
                .arg(
                    Arg::new("pkg")
                        .help("package name(s)")
                        .action(clap::ArgAction::Append)
                        .required(false),
                ),
        )
        //.subcommand(
        //    clap::Command::new("query"), //.short_flag('Q')
        //)
        .get_matches();

    // load the config file
    let mut app = App {
        config: config::parse_path("config.toml")?,
        db: db::Db::new(),
    };

    match matches.subcommand() {
        Some(("list", _sub_matches)) => {
            app.list_cmd()?;
        }
        Some(("install", sub_matches)) => {
            let pkg_name = sub_matches.get_one::<String>("pkg").unwrap();
            app.install_cmd(pkg_name)?;
        }
        Some(("uninstall", sub_matches)) => {
            let pkg_name = sub_matches.get_one::<String>("pkg").unwrap();
            println!("removing package {pkg_name}");
            app.uninstall_cmd(pkg_name)?;
        }
        Some(("verify", sub_matches)) => {
            // TODO
            // -q quiet option, don't print file status, just return good or bad exit code
            // --stop-on-first  option to stop on the first mismatch

            let pkg_names = sub_matches
                .get_many::<String>("pkg")
                .map_or(Vec::new(), |given| given.collect());

            app.verify_cmd(&pkg_names)?;
        }
        Some(("search", sub_matches)) => {
            let pkg_name = sub_matches.get_one::<String>("pkg").unwrap();
            //println!("searching for package {}", pkg_name);
            app.search_cmd(pkg_name)?;
        }
        _ => {
            todo!("NYI");
        }
    }

    Ok(())
}
