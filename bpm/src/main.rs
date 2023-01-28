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

struct App {
    config: config::Config,
    db: db::Db,
}

const PACKAGE_EXT: &str = ".bpm.tar";

impl App {
    fn load_db(&mut self) -> AResult<()> {
        println!("loading db");
        let mut db_file =
            std::fs::File::open(&self.config.db_file).context("cannot read database file")?;
        self.db = db::Db::from_reader(&mut db_file).context("failed to load database")?;
        Ok(())
    }

    fn create_load_db(&mut self) -> AResult<()> {
        let db_path = Path::new(&self.config.db_file);
        let exists = db_path
            .try_exists()
            .context("cannot access database file")?;
        if !exists {
            println!("creating empty db file");
            std::fs::File::create(db_path).context("cannot create database file")?;
        }

        self.load_db()
    }

    fn list_cmd(&mut self) -> AResult<()> {
        self.load_db()?;

        let mut tw = tabwriter::TabWriter::new(std::io::stdout());
        //write!(&mut tw, "{}\t{}\n", "name", "version")?;
        for ent in self.db.installed.iter() {
            writeln!(&mut tw, "{}\t{}", ent.id.name, ent.id.version)?;
        }
        tw.flush()?;
        Ok(())
    }

    fn search_cmd(&self, pkg_name: &str) -> AResult<()> {
        let mut results = search::SearchResults::new();
        for provider in &self.config.providers {
            let part = provider.1.as_provide().search(pkg_name);
            results.extend(part);
        }

        let mut tw = tabwriter::TabWriter::new(std::io::stdout());
        if !results.inner.is_empty() {
            writeln!(&mut tw, "\tname\tversion\tdescription")?;
            //write!(&mut tw, "\t---\t---\t---\n")?;
        }
        for (n, result) in results.inner.into_iter().enumerate() {
            let version = result.1.versions.last().unwrap();
            writeln!(&mut tw, "{})\t{}\t{}", n, result.1.name, version)?;
        }
        tw.flush()?;

        Ok(())
    }

    fn search_results(&self, pkg_name: &str, fuzzy: bool) -> AResult<search::SearchResults> {
        let mut results = search::SearchResults::new();
        for provider in &self.config.providers {
            let part = provider.1.as_provide().search(pkg_name);
            results.extend(part);
        }

        if !fuzzy {
            results
                .inner
                .retain(|_k, v| v.name == pkg_name && !v.versions.is_empty())
        }

        Ok(results)
    }

    fn find_latest_version(&self, pkg_name: &str) -> AResult<Version> {
        let results = self.search_results(pkg_name, false)?;

        if results.inner.len() != 1 {
            anyhow::bail!("could not find package '{pkg_name}'");
        }

        let versions: Vec<_> = results
            .inner
            .iter()
            .next()
            .unwrap()
            .1
            .versions
            .iter()
            .filter(|v| v.pre.is_empty())
            .collect();
        Ok((*versions.iter().last().unwrap()).clone())
    }

    fn cache_fetch_cmd(&self, pkg_name: &str, version: &Version) -> AResult<PathBuf> {
        // hmm, install first found from first provider?
        // or search all providers and install max version?
        // mirrors?

        let pkg_id = package::PackageID {
            name: String::from(pkg_name),
            version: version.clone(),
        };

        let filepath = package::to_filepath(PathBuf::from(&self.config.cache_dir), pkg_name, version);

        let mut file = std::fs::File::create(&filepath)?;

        for provider in &self.config.providers {
            if provider.1.as_provide().fetch(&pkg_id, &mut file).is_ok() {
                break;
            }
        }

        Ok(filepath)
    }

    fn install_pkg_file(&mut self, file_path: PathBuf) -> AResult<()> {
        // TODO where are we actually installing into?
        // TEMP
        let mut install_dir = PathBuf::from(&self.config.cache_dir);
        install_dir.push("install");
        create_dir(&install_dir)?;

        let file_name = file_path
            .file_name()
            .expect("invalid filename")
            .to_str()
            .expect("invalid filename");
        let pkg_name;
        let pkg_version;

        match package::name_parts(file_name) {
            Some((n, v)) => {
                pkg_name = n;
                pkg_version = v;
            }
            None => {
                anyhow::bail!("package name is invalid");
            }
        }

        // open the file
        let mut file =
            std::fs::File::open(&file_path).context("failed to open cached package file")?;

        // get the list of files and their hashes from the meta file
        let files = package::get_filelist(&mut file)?;

        // make sure the checksum matches
        let cksum_match = package::check_datachecksum(&mut file)?;
        if !cksum_match {
            anyhow::bail!("Corrupted package, checksum mismatch");
        }

        // obtain reader for embeded data archive
        file.rewind()?;
        let mut tar = tar::Archive::new(&mut file);
        let mut data_tar = package::seek_to_tar_entry("data.tar.zst", &mut tar)?;
        let mut zstd = zstd::stream::read::Decoder::new(&mut data_tar)?;
        let mut data_tar = tar::Archive::new(&mut zstd);

        // unpack all files
        //data_tar.unpack(install_dir)?;

        //let mut file_list = Vec::new();

        // unpack individual files
        for entry in data_tar.entries()? {
            let mut entry = entry?;
            let installed = entry.unpack_in(&install_dir)?;
            //println!("unpacked entry {:?} {}", entry.path(), installed);
            //file_list.push(entry.path()?.into_owned());

            // TODO can this exit cleaner?
            // failed to install a file, exit
            if !installed {
                match entry.path() {
                    Ok(path) => {
                        anyhow::bail!("failed to install file {:?}", path);
                    }
                    Err(_) => {
                        anyhow::bail!("failed to install file");
                    }
                }
            }
        }

        let mut details = db::DbPkg::new(PackageID {
            name: String::from(pkg_name),
            version: pkg_version,
        });

        for (k, v) in files.into_iter() {
            details.files.push((k, v));
        }

        details.location = install_dir;
        self.db.add_package(details);

        let mut db_file = std::fs::File::create("db")?;
        self.db
            .write_to(&mut std::io::BufWriter::new(&mut db_file))?;
        db_file.sync_all()?;

        drop(db_file);

        let db = db::Db::from_file("db")?;
        dbg!(&db);

        Ok(())
    }

    /// installing a package
    fn install_cmd(&mut self, pkg_name: &String) -> AResult<()> {
        self.create_load_db()?;

        // make sure we have a cache dir to save the package in
        create_dir(&self.config.cache_dir)?;

        //TEMP
        let mut install_dir = PathBuf::from(&self.config.cache_dir);
        install_dir.push("install");
        create_dir(&install_dir)?;

        // installing from a package file
        if pkg_name.ends_with(PACKAGE_EXT) && let Ok(true) = std::path::Path::new(pkg_name).try_exists() {

            println!("installing from package file {pkg_name}");

            // TODO check that the package file is not a path to a file in the cache dir
            // (DO NOT COPY IN PLACE)

            dbg!(&pkg_name);
            dbg!(&self.config.cache_dir);

            let pkg_filepath = Path::new(pkg_name);
            let pkg_filename = pkg_filepath
                .file_name()
                .expect("invalid filepath")
                .to_str()
                .expect("invalid filepath");

            if !package::named_properly(pkg_filename) {
                anyhow::bail!("package name is invalid");
            }

            let mut cached_filepath = PathBuf::new();
            cached_filepath.push(&self.config.cache_dir);
            cached_filepath.push(pkg_filename);

            // copy to the cache dir
            std::fs::copy(pkg_filepath, &cached_filepath).context("failed to copy package file")?;

            return self.install_pkg_file(cached_filepath);
        }

        // installing from provider
        println!("installing from provider");

        let mut split = pkg_name.split('@');

        let pkg_name = match split.next() {
            Some("") | None => {
                anyhow::bail!("empty package name");
            }
            Some(name) => name,
        };

        let version = match split.next() {
            Some("") | None => None,
            Some(v) => Some(Version::parse(v).context("invalid version format")?),
        };

        dbg!(&pkg_name);
        dbg!(&version);

        let version = match version {
            Some(v) => v,
            None => {
                // get the latest version
                println!("fetching latest version");
                self.find_latest_version(pkg_name)?
            }
        };

        println!("installing {pkg_name} {version}");
        let cached_filepath = self.cache_fetch_cmd(pkg_name, &version)?;
        return self.install_pkg_file(cached_filepath);
    }

    /// verify package state
    fn verify_cmd<S>(&mut self, pkgs: &Vec<S>) -> AResult<()>
    where
        S: AsRef<str>,
    {
        // for installed packages listed in the db,
        // walk each file and hash the version we have on disk
        // and compare to the hash stored in the db
        self.load_db()?;

        // all given names must be installed
        for name in pkgs {
            let find = self
                .db
                .installed
                .iter()
                .find(|&ent| ent.id.name == name.as_ref());
            if find.is_none() {
                anyhow::bail!("package named '{}' is not installed", name.as_ref());
            }
        }

        let filtering = !pkgs.is_empty();
        let iter = self.db.installed.iter().filter(|&ent| {
            if filtering {
                pkgs.iter().any(|name| name.as_ref() == ent.id.name)
            } else {
                true
            }
        });

        for pkg in iter {
            println!("verifying package {}", pkg.id.name);

            let root_dir = pkg.location.clone();
            for file in pkg.files.iter() {
                let (stem, db_hash) = file;
                let mut path = root_dir.clone();
                path.push(stem);
                //println!("checking file at {path:?} for hash {db_hash}");

                if !path.exists() {
                    println!("bad -- {:?} does not exist", &path);
                    continue;
                }

                let file = std::fs::File::open(&path)?;
                let reader = std::io::BufReader::new(file);
                let hash = package::blake2_hash_reader(reader)?;

                //println!("got hash {hash}");
                if &hash == db_hash {
                    println!("ok  -- {:?}", &path);
                } else {
                    println!("bad --{:?}", &path);
                }

                // TODO could put progress bars in here
            }
        }

        Ok(())
    }

    /// uninstall a package
    fn uninstall_cmd(&mut self, pkg_name: &String) -> AResult<()> {
        // from the package name,
        // find all files that belong to this package from the db

        self.load_db()?;

        let found = self.db.installed.iter().find(|e| &e.id.name == pkg_name);

        let pkg = match found {
            None => anyhow::bail!("package '{pkg_name}' not installed"),
            Some(pkg) => pkg,
        };

        //dbg!(pkg);

        for file in &pkg.files {
            let (path, _hash) = file;
            println!("removing {path}");
            // TODO actually delete the files
        }

        Ok(())
    }
}

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
        (true, true) => Ok(()),
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
        config: config::parse("config.toml")?,
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
