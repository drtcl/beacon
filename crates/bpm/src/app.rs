use bpmutil::*;
use chrono::SubsecRound;
use crate::*;
use itertools::Itertools;
use package::PackageID;
use serde::{Serialize, Deserialize};
use std::collections::BTreeMap;
use std::collections::HashSet;
use std::fs::File;
use std::io::BufWriter;
use std::io::IsTerminal;
use std::io::Read;
use std::io::Seek;

mod list;

#[derive(Debug)]
pub struct App {
    pub config: config::Config,
    pub db: db::Db,
    pub db_loaded: bool,

    pub provider_filter: provider::ProviderFilter,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Versioning {
    pub pinned_to_version: bool,
    pub pinned_to_channel: bool,
    pub channel: Option<String>,
}

impl Versioning {
    fn unpinned() -> Self {
        Self {
            pinned_to_version: false,
            pinned_to_channel: false,
            channel: None,
        }
    }
    fn pinned_version() -> Self {
        Self {
            pinned_to_version: true,
            pinned_to_channel: false,
            channel: None,
        }
    }
    fn pinned_channel(chan: &str) -> Self {
        Self {
            pinned_to_version: false,
            pinned_to_channel: true,
            channel: Some(chan.to_string())
        }
    }
}

impl App {
    pub fn load_db(&mut self) -> AResult<()> {

        if self.db_loaded {
            tracing::trace!("database already loaded");
            return Ok(())
        }

        tracing::trace!("loading database");

        if !self.config.db_file.exists() {
            tracing::warn!("database file does not exist");
        }

        let mut db_file = File::open(&self.config.db_file).context("cannot read database file")?;
        self.db = db::Db::from_reader(&mut db_file).context("failed to load database")?;
        Ok(())
    }

    pub fn db_file_exists(&self) -> bool {
        let path = Path::new(&self.config.db_file);
        path.try_exists().unwrap_or(false)
    }

    pub fn create_load_db(&mut self) -> AResult<()> {

        let db_path = Path::new(&self.config.db_file);
        let exists = db_path
            .try_exists()
            .context("cannot access database file")?;
        if !exists {
            tracing::trace!("creating empty db file");
            File::create(db_path).context("cannot create database file")?;
        }

        self.load_db()
    }

    pub fn save_db(&self) -> AResult<()> {
        tracing::trace!("saving database");
        // write to a temp file first, then move the tempfile over the existing dbfile
        let mut path = self.config.db_file.clone();
        let ext = path.extension().map_or("tmp".into(), |e| format!("{}.tmp", e));
        path.set_extension(ext);
        let mut file = BufWriter::new(File::create(&path)?);
        self.db.write_to(&mut file)?;
        std::fs::rename(&path, &self.config.db_file)?;
        Ok(())
    }

    /// `bpm search`
    ///
    /// search each provider's cached package info, merge results, print latest versions
    pub fn search_cmd(&self, pkg_name: &str, exact: bool) -> AResult<()> {

        let results = self.search_results(pkg_name, exact)?;

        let mut tw = tabwriter::TabWriter::new(std::io::stdout());
        if std::io::stdout().is_terminal() && !results.is_empty() {
            writeln!(&mut tw, "name\tversion\tdescription")?;
            writeln!(&mut tw, "----\t-------\t-----------")?;
        }
        for (name, versions) in results.iter() {
            let (version, _url) = versions.last_key_value().unwrap();
            writeln!(&mut tw, "{}\t{}", name, version)?;
        }
        tw.flush()?;

        Ok(())
    }

    /// iterate through providers with an applied filter
    fn filtered_providers(&self) -> impl Iterator<Item=&provider::Provider> {
        self.provider_filter.filter(&self.config.providers)
    }

    // -------------

    /// search and merge each provider's cached package info
    fn search_results(&self, needle: &str, exact: bool) -> AResult<search::PackageList> {

        let mut merged_results = search::PackageList::new();
        let needle_lower = needle.to_lowercase();

        for provider in self.filtered_providers() {

            // load the provider's cache file and merge it's results
            if let Ok(data) = provider.load_file() {

                let mut cached_results = data.packages;

                cached_results.retain(|name, _versions| {
                    if exact {
                        name == needle
                    } else {
                        name.contains(needle) || name.to_lowercase().contains(&needle_lower)
                    }
                });
                merged_results = search::merge_package_lists(merged_results, cached_results);
            } else {
                tracing::trace!(file=?provider.cache_file, "couldn't read cache file for provider '{}'", provider.name);
            }
        }

        Ok(merged_results)
    }

    fn get_mountpoint_dir(&self, metadata: &package::MetaData) -> AResult<config::PathType> {
        use config::MountPoint::*;
        let mount_point = self.config.get_mountpoint(metadata.mount.as_deref());
        match mount_point {
            Specified(mp) |
            Default(mp) => Ok(mp),
            DefaultDisabled => anyhow::bail!("attempt to use default target, which is disabled"),
            Invalid{name} => anyhow::bail!("package using an invalid mountpoint: {name}"),
        }
    }

    /// install a package from a local package file.
    fn install_pkg_file(&mut self, file_path: Utf8PathBuf, versioning: Versioning) -> AResult<()> {

        tracing::debug!("install_pkg_file {}", file_path);

        let package_file_filename = file_path.file_name().context("invalid filename")?;

        let pkg_name;
        let pkg_version;
        match (package::is_packagefile_name(package_file_filename), package::split_parts(package_file_filename)) {
            (true, Some((n, v))) => {
                pkg_name = n;
                pkg_version = v;
            }
            _ => {
                anyhow::bail!("package name is invalid");
            }
        }

        // open the package file and get the metadata
        let mut file = File::open(&file_path).context("failed to open package file")?;
        //let mut metadata = package::get_metadata(&mut file).context("error reading metadata")?;

        // make sure the checksum matches
        let (ok, mut metadata) = package::package_integrity_check(&mut file)?;
        if !ok {
            anyhow::bail!("failed to install {}, package file failed integrity check", pkg_name);
        }

        // package filename must match package metadata
        // name must match
        if metadata.name != pkg_name {
            anyhow::bail!("failed to installed {}, package file is corrupt. Package name does not match package metadata.", pkg_name);
        }
        // version must match
        if metadata.version != pkg_version {
            anyhow::bail!("failed to installed {}, package file is corrupt. Package version does not match package metadata.", pkg_name);
        }

        let install_dir = self.get_mountpoint_dir(&metadata)?;
        let install_dir_full = install_dir.full_path()?;

        // create the mount point dir if it doesn't exist
        if !install_dir_full.exists() {
            std::fs::create_dir_all(&install_dir_full)?;
        }

        // get the list of files and their hashes from the meta file
        //let files = package::get_filelist(&mut file)?;
        //let files = &metadata.files;

        // obtain reader for embeded data archive
        file.rewind()?;
        let mut outer_tar = tar::Archive::new(&mut file);
        let (mut inner_tar, size) = package::seek_to_tar_entry("data.tar.zst", &mut outer_tar)?;
        let mut zstd = zstd::stream::read::Decoder::new(&mut inner_tar)?;
        let mut data_tar = tar::Archive::new(&mut zstd);

        let pbar = indicatif::ProgressBar::new(metadata.files.len() as u64);
        pbar.set_style(indicatif::ProgressStyle::with_template(
            "   {msg}\n {spinner:.green} installing {prefix} {wide_bar:.green} {pos}/{len} "
        ).unwrap());
        pbar.set_prefix(metadata.name.to_string());

        // unpack all files individually
        for entry in data_tar.entries()? {

            let mut entry = entry?;

            let installed_ok = entry.unpack_in(&install_dir_full)?;

            let path = Utf8PathBuf::from_path_buf(entry.path().unwrap().into_owned()).unwrap();

            pbar.set_message(String::from(path.as_str()));

            let installed_path = join_path_utf8!(&install_dir_full, &path);

            // store the mtime
            if let Some(info) = metadata.files.get_mut(&path) {
                let header = entry.header();
                let mut mtime = header.mtime().ok();
                if mtime.is_none() {
                    mtime = get_mtime(&installed_path);
                }
                info.mtime = mtime;
            }

            if !installed_ok {
                tracing::error!("unpack skipped {:?}", entry.path());
            }

            pbar.inc(1);
            //pbar.suspend(|| println!("{}", path));
        }

        pbar.finish_and_clear();

        let mut details = db::DbPkg::new(metadata);
        details.location = Some(install_dir);
        details.versioning = versioning;
        details.package_file_filename = Some(package_file_filename.to_string());

        self.db.add_package(details);

        self.db.cache_touch(package_file_filename, None);
        self.db.cache_unuse_all_versions(pkg_name);
        self.db.cache_set_in_use(package_file_filename, true);

        self.save_db()?;

        Ok(())
    }

    fn find_package_version(&self, pkg_name: &str, which: Option<&str>) -> AResult<(search::SingleListing, Versioning)> {

        // first search our locally cached files
        if let Some(version) = which {
            let x = self.cache_package_lookup(&PackageID {
                name: pkg_name.to_string(),
                version: version.to_string()
            });
            if let Some(path) = x {
                let filename = path.file_name().context("cache file has no filename")?.to_string();
                return Ok((
                    search::SingleListing {
                        pkg_name: std::rc::Rc::<str>::from(pkg_name),
                        version: version.into(),
                        filename,
                        url: "".into(),
                        channels: vec![],
                    },
                    Versioning::pinned_version()
                ));
            }
        }

        tracing::trace!(pkg_name, which, "find_package_version");

        // get search results for just this package name
        let mut results = self.search_results(pkg_name, true)?;

        if results.len() != 1 {
            anyhow::bail!("could not find package '{pkg_name}'");
        }

        let versions = results.iter_mut().next().unwrap().1;

        // `which` could be a channel, check if it is
        let mut versioning = Versioning::default();

        if let Some(v) = which {

            if versions.iter().any(|(_version, info)| info.channels.iter().any(|c| c == v)) {
                // channel found, filter out versions that are not on this channel
                versions.retain(|_version, info| info.channels.iter().any(|c| c == v));
                //channel = Some(v);
                versioning.pinned_to_channel = true;
                versioning.channel = Some(v.to_string());

            } else {
                // must be a specific version, find it

                versions.retain(|version, _info| version.as_str() == v);
                versioning.pinned_to_version = true;
            }

            if versions.is_empty() {
                anyhow::bail!("could not find version or channel '{v}' for package '{pkg_name}'");
            }
        }

        if versions.is_empty() {
            anyhow::bail!("no versions available for package '{pkg_name}'");
        }

        {
            // keep only the last version
            let (version, urlfn) = versions.pop_last().unwrap();
            versions.clear();
            versions.insert(version, urlfn);
        }

        // take the greatest package version
        let result = search::flatten(results);
        let result = result.into_iter().next().unwrap();

        Ok((result, versioning))
    }

    /// check if a given user arg is a path to a pacakge file on the fs
    fn is_package_file_arg(arg: &str) -> Option<(Utf8PathBuf, &str, Version)> {
        let path = Utf8Path::new(arg);
        if let Ok(true) = path.try_exists() {
            tracing::trace!(path=?path, "testing if file path looks like a package");
            let filename = path.file_name();
            if let Some(filename) = filename && package::is_packagefile_name(filename) {
                if let Some((name, ver)) = package::split_parts(filename) {
                    return Some((path.into(), name, Version::new(ver)));
                }
            }
        }
        None
    }

    /// `bpm install`
    ///
    /// `bpm install foo`
    /// or `bpm install foo@1.2.3`
    /// or `bpm install path/to/foo_1.2.3.bpm`
    /// install a package from a provider or directly from a file
    pub fn install_cmd(&mut self, pkg_name_or_filepath: &str, no_pin: bool, update: bool, reinstall: bool) -> AResult<()> {

        self.create_load_db()?;

        // make sure we have a cache dir to save the package in
        create_dir(join_path_utf8!(&self.config.cache_dir, "packages"))?;

        // are we installing directly from a package file?
        // or from just a name that must be found and pulled from a provider?

        let version;
        let mut versioning;
        let mut file_to_cache = None;
        let pkg_name;

        if let Some((path, name, v)) = Self::is_package_file_arg(pkg_name_or_filepath) {

            tracing::debug!("installing directly from file {}", path);
            version = v;
            versioning = Versioning::pinned_version();
            file_to_cache = Some(path);
            pkg_name = name;

        } else {

            tracing::trace!("installing from provider");

            let mut split = pkg_name_or_filepath.split('@');

            pkg_name = match split.next() {
                Some("") | None => {
                    anyhow::bail!("empty package name");
                }
                Some(name) => name
            };

            let v = split.next();

            let listing;
            (listing, versioning) = self.find_package_version(pkg_name, v)?;
            version = Version::new(&listing.version);
        }

        if no_pin {
            versioning = Versioning::unpinned();
        }

        let current_install = self.db.installed.iter().find(|p| p.metadata.name.as_str() == pkg_name);
        let already_installed = current_install.is_some();

        // check if the package is already installed
        if let Some(current) = &current_install {
            let version_same = current.metadata.version == version.as_str();

            let pinning_same = current.versioning == versioning;

            if version_same {
                if !reinstall {

                    println!("No change. Package {} at version {} is already installed.", pkg_name, version);

                    // may not need to update because versions are the same,
                    // but may be updating from specific version to a channel or from a channel to
                    // a version (changing the pinning). Update pinning info.
                    if !pinning_same && update {
                        self.db.set_versioning(pkg_name, versioning.clone());
                        self.save_db()?;
                        if versioning.pinned_to_channel {
                            println!("Updated pin to channel {}.", versioning.channel.as_deref().unwrap_or("?"));
                        } else if versioning.pinned_to_version {
                            println!("Updated pin to version {}.", version.as_str());
                        } else {
                            println!("Update pinning, no longer pinned to a version or channel");
                        }
                    }

                    return Ok(());
                }
            } else if !update {
                println!("Package {} ({}) is already installed. Pass --update to install a different version.", pkg_name, current.metadata.version);
                return Ok(());
            } else {
                println!("Updating {} from version {} to {}", pkg_name, current.metadata.version, version);
            }
        }

        tracing::debug!(pkg_name, version=version.as_str(), "installing from provider");

        let id = PackageID {
            name: pkg_name.to_string(),
            version: version.to_string(),
        };

        let cached_file = if let Some(path) = &file_to_cache {
            self.cache_store_file(path)?
        } else {
            self.cache_package_require(&id)?
        };

        if already_installed {
            let existing_version = &current_install.unwrap().metadata.version;
            tracing::debug!("updating {pkg_name} {existing_version} -> {version}");
            return self.update_inplace(pkg_name, cached_file, versioning);
        } else {
            tracing::debug!("installing {pkg_name} {version}");
            return self.install_pkg_file(cached_file, versioning);
        }
    }

    /// `bpm uninstall` or `bpm remove`
    /// uninstall a package
    pub fn uninstall_cmd(&mut self, pkg_name: &String, verbose: bool, remove_unowned: bool) -> AResult<()> {
        // from the package name,
        // find all files that belong to this package from the db

        self.load_db()?;

        let found = self.db.installed.iter().find(|e| &e.metadata.name == pkg_name);

        if found.is_none() {
            println!("package '{pkg_name}' not installed");
            std::process::exit(0);
        }

        let pkg = found.unwrap();
        let package_file_filename = pkg.package_file_filename.clone();

        self.delete_package_files(pkg, verbose, remove_unowned)?;
        self.db.remove_package(pkg.metadata.id());
        if let Some(filename) = package_file_filename {
            self.db.cache_touch(&filename, None);
            self.db.cache_set_in_use(&filename, false);
        }
        self.save_db()?;
        Ok(())
    }

    /// install a different version of a package in-place (on top of) the existing version
    fn update_inplace(&mut self, pkg_name: &str, new_pkg_file: Utf8PathBuf, versioning: Versioning) -> AResult<()> {

        let cache_file = &new_pkg_file;
        tracing::trace!("update_inplace {cache_file:?}");

        let mut new_package_fd = std::fs::File::open(cache_file)?;

        //// make sure the checksum matches
        let (ok, new_metadata) = package::package_integrity_check(&mut new_package_fd)?;
        if !ok {
            anyhow::bail!("failed to install {}, package file failed integrity check", pkg_name);
        } else {
            tracing::trace!("package integrity check pass");
        }

        let new_files = new_metadata.files.clone();
        //dbg!(&new_files.keys());

        // gather info about the old package (the currently installed version)
        let current_pkg_info = self.db.installed.iter().find(|e| e.metadata.name == pkg_name).context("package is not currently installed")?;
        let mut old_files = current_pkg_info.metadata.files.clone();

        let location = current_pkg_info.location.as_ref().context("installed package has no location")?.clone();
        let location_full = location.full_path()?;
        tracing::trace!("installing to the same location {:?}", location);

        let bars = indicatif::MultiProgress::new();

        // old_files -= new_files
        for (path, new_file) in &new_files {
            let old_file = old_files.get(path);
            if let Some(old_file) = old_file {
                if old_file.filetype != new_file.filetype {
                } else {
                    // same path and type, the file will be kept
                    old_files.remove_entry(path);
                }
            }
        }

        // these are the files to remove
        let remove_files = old_files;

        let delete_thread = std::thread::spawn({
            let current_pkg_info = current_pkg_info.clone();
            let bars = bars.clone();
            let location_full = location_full.clone();
            move || {

                let iter = remove_files.iter().map(|(path, info)| {
                    let path = join_path_utf8!(&location_full, &path);
                    (path, info)
                });

                let delete_bar = bars.add(indicatif::ProgressBar::new(remove_files.len() as u64));
                delete_bar.set_style(indicatif::ProgressStyle::with_template(
                    " {spinner:.green} remove   {wide_bar:.red} {pos}/{len} "
                ).unwrap());

                let iter = delete_bar.wrap_iter(iter).with_finish(indicatif::ProgressFinish::AndClear);

                let delete_ok = Self::delete_files(iter, false, true);
                if let Err(e) = delete_ok {
                    eprintln!("error deleting files {:?}", e);
                }
            }
        });

        let diff_bar = indicatif::ProgressBar::new(new_files.len() as u64);
        diff_bar.set_style(indicatif::ProgressStyle::with_template(
            " {spinner:.green} diffing  {wide_bar:.blue} {pos}/{len} "
        ).unwrap());
        let diff_bar = bars.add(diff_bar);

        let skip_files = std::sync::Mutex::new(HashSet::<Utf8PathBuf>::new());

        // spawn threads for hashing/diffing existing files
        std::thread::scope(|s| {

            let (send, recv) = crossbeam::channel::bounded::<(&Utf8PathBuf, &package::FileInfo)>(256);

            let diff_threads = std::thread::available_parallelism().map(|v| v.get()).unwrap_or(1).clamp(1, 8);

            for _ in 0..diff_threads {
                let location = &location;
                let location_full = &location_full;
                let new_files = &new_files;
                let skip_files = &skip_files;
                let recv = recv.clone();
                let diff_bar = diff_bar.clone();
                s.spawn(move || {

                    let mut t_skip_files = HashSet::<Utf8PathBuf>::new();

                    // read a path from the channel
                    while let Ok((path, info)) = recv.recv() {

                        let fullpath = join_path_utf8!(location_full, &path);

                        let file_state = get_filestate(&fullpath);

                        // if the file path exists in the filesystem, it's a regular file and the
                        // new file is a regular file. We can potentially skip replacing this file,
                        // check if the current file's contents match the incoming file.
                        if !file_state.missing && file_state.file && info.filetype.is_file() {

                            // hash the file as a last resort
                            let mut do_hash = true;

                            // if the file is a different size, it needs replacing
                            if let Some(new_size) = info.size {
                                let cur_size = get_filesize(fullpath.as_str());
                                if let Ok(cur_size) = cur_size && cur_size != new_size {
                                    do_hash = false;
                                }
                            }

                            if do_hash {
                                // if the file has a different mtime, just replace it
                                if let Some(mtime) = info.mtime {
                                    if let Some(cur_mtime) = file_state.mtime && cur_mtime != mtime {
                                        do_hash = false;
                                    }
                                }
                            }

                            if do_hash {
                                if let Some(new_hash) = info.hash.as_ref() {
                                    if let Ok(mut file) = File::open(&fullpath) {
                                        if let Ok(hash) = blake3_hash_reader(&mut file) {
                                            if &hash == new_hash {
                                                // this file can be skipped during update
                                                t_skip_files.insert(path.clone());
                                            }
                                        }
                                    }
                                }
                            }
                        }

                        diff_bar.inc(1);
                    }

                    // merge this threads t_skip_files into the main skip_files
                    skip_files.lock().unwrap().extend(t_skip_files);
                });
            }

            for (path, info) in new_files.iter() {
                let _ = send.send((path, info));
            }
            drop(send);
        });

        diff_bar.finish_and_clear();

        let mut skip_files = skip_files.into_inner()?;

        // wait for deletes to be finished before starting unpacks,
        // there could be conflicting file types
        if delete_thread.join().is_err() {
            anyhow::bail!("Error in file deletion thread");
        }

        let update_bar = bars.add(indicatif::ProgressBar::new((new_files.len() - skip_files.len()) as u64));
        //let update_bar = bars.add(indicatif::ProgressBar::new(new_files.len() as u64));
        //update_bar.set_position(skip_files.len() as u64);

        update_bar.set_style(indicatif::ProgressStyle::with_template(
            "   {msg}\n {spinner:.green} updating {wide_bar:.green} {pos}/{len} "
        ).unwrap());
        update_bar.set_prefix(new_metadata.name.to_string());

        new_package_fd.rewind()?;
        let mut outer_tar = tar::Archive::new(&mut new_package_fd);
        let (mut inner_tar, _size) = package::seek_to_tar_entry("data.tar.zst", &mut outer_tar)?;
        let mut zstd = zstd::stream::read::Decoder::new(&mut inner_tar)?;
        let mut data_tar = tar::Archive::new(&mut zstd);

        for entry in data_tar.entries()? {
            let mut entry = entry?;
            let path = entry.path()?;
            let path = Utf8PathBuf::from(&path.to_string_lossy());
            if skip_files.remove(&path) {
                tracing::trace!("skipping   {}", path);
            } else {
                tracing::debug!("updating   {}", path);
                update_bar.set_message(String::from(path.as_str()));
                let _ok = entry.unpack_in(&location_full);
                //TODO handle error
                update_bar.inc(1);
            }
        }

        update_bar.finish_and_clear();

        let mut details = db::DbPkg::new(new_metadata);
        details.location = Some(location);
        details.versioning = versioning;
        let pkg_filename = new_pkg_file.file_name().map(str::to_string);
        details.package_file_filename = pkg_filename;

        self.db.add_package(details);

        if let Some(filename) = new_pkg_file.file_name() {
            self.db.cache_touch(filename, None);
            self.db.cache_unuse_all_versions(pkg_name);
            self.db.cache_set_in_use(filename, true);
        }

        self.save_db()?;

        Ok(())
    }

    /// `bpm update`
    pub fn update_packages_cmd(&mut self, pkgs: &[&String]) -> AResult<()> {

        // nothing to do if the db file doesn't exist yet, nothing to update
        if !self.db_file_exists() {
            return Ok(());
        }

        self.load_db()?;

        // determine which packages need to be updated
        let mut updates = Vec::new();

        for pkg in &self.db.installed {

            // skip pkgs that are not listed at the cli
            if !pkgs.is_empty() && !pkgs.contains(&&pkg.metadata.name) {
                continue;
            }

            if pkg.versioning.pinned_to_version {
                println!("{} is pinned to {}, skipping", pkg.metadata.name, pkg.metadata.version);
                continue;
            }

            tracing::trace!("{}: current version {}", pkg.metadata.name, pkg.metadata.version);

            // updates stay on a channel?
            let mut channel = None;
            if pkg.versioning.pinned_to_channel {
                if let Some(c) = &pkg.versioning.channel {
                    channel = Some(c.clone());
                }
            }

            let result = self.find_package_version(&pkg.metadata.name, channel.as_deref());

            if let Ok((listing, versioning)) = result {
                let version = listing.version;

                // if the version is the same as already installed, skip the update
                if version == Version::from(pkg.metadata.version.as_str()) {
                    //println!("{} already up-to-date", pkg.metadata.name);
                    continue;
                }

                //println!("{}: updating to {}", pkg.metadata.name, version);

                let cached_file = self.cache_package_require(&PackageID{
                    name: pkg.metadata.name.clone(),
                    version: version.to_string(),
                });

                if cached_file.is_err() {
                    println!("failed to cache fetch {} {}", pkg.metadata.name, version);
                    continue;
                }

                let cached_file = cached_file.unwrap();
                //self.update_inplace(&pkg, cached_file)?;
                updates.push((pkg.metadata.name.clone(), pkg.metadata.version.clone(), version, versioning, cached_file));
            }
        }

        if !updates.is_empty() {
            println!("{} package{} to update:", updates.len(), tern!(updates.len() > 1, "s", ""));
            for (name, oldv, newv, _versioning, _pkgfile) in &updates {
                println!("  {}: {} -> {}", name, oldv, newv);
            }
        } else {
            println!("No updates to apply");
        }

        //TODO can put confirmation prompt here

        for (pkg_name, _oldv, _newv, versioning, new_pkg_file) in updates {
            self.update_inplace(&pkg_name, new_pkg_file, versioning)?;
        }

        Ok(())
    }

    /// `bpm pin`
    /// Pin a package to a channel or the currently installed version
    pub fn pin(&mut self, pkg_name: &str, channel: Option<&str>) -> AResult<()> {

        self.load_db()?;
        for pkg in &mut self.db.installed {
            if pkg.metadata.name == pkg_name {
                if let Some(channel) = channel {
                    pkg.versioning = Versioning::pinned_channel(channel);
                } else {
                    pkg.versioning = Versioning::pinned_version();
                }
                self.save_db()?;
                return Ok(());
            }
        }

        anyhow::bail!("package not found");
    }

    /// `bpm unpin`
    /// Unpin a package
    pub fn unpin(&mut self, pkg_name: &str) -> AResult<()> {

        self.load_db()?;
        for pkg in &mut self.db.installed {
            if pkg.metadata.name == pkg_name {
                pkg.versioning = Versioning::unpinned();
                self.save_db()?;
                return Ok(());
            }
        }

        anyhow::bail!("package not found");
    }

    /// `bpm verify`
    /// Verify package state.
    ///
    /// For installed packages listed in the db,
    /// walk each file and hash the version we have on disk
    /// and compare that to the hash stored in the db.
    pub fn verify_cmd<S>(&mut self, pkgs: &Vec<S>, restore: bool, restore_volatile: bool, verbose: bool, allow_mtime: bool) -> AResult<()>
        where S: AsRef<str>,
    {
        tracing::trace!(restore=restore, restore_volatile=restore_volatile, "verify");

        // if the db file doesn't exist, dont' attempt to load it, return 0 packages
        if !self.db_file_exists() {
            return Ok(());
        }

        self.load_db()?;

        // all given names must be installed
        for name in pkgs {
            let find = self.db.installed.iter().find(|&ent| ent.metadata.name == name.as_ref());
            if find.is_none() {
                anyhow::bail!("package named '{}' is not installed", name.as_ref());
            }
        }

        let filtering = !pkgs.is_empty();
        let db_iter = self.db.installed.iter().filter(|&ent| {
            if filtering {
                pkgs.iter().any(|name| name.as_ref() == ent.metadata.name)
            } else {
                true
            }
        });

        // TODO could put progress bars in here

        for pkg in db_iter {

            let mut pristine = true;
            vout!(verbose, "# verifying package {}", pkg.metadata.name);

            let mut restore_files = HashSet::new();

            let root_dir = pkg.location.as_ref().expect("package has no installation location").clone();
            let root_dir_full = root_dir.full_path()?;

            for (filepath, fileinfo) in pkg.metadata.files.iter() {

                let path = join_path_utf8!(&root_dir_full, filepath);
                //println!("{}", path);

                if fileinfo.volatile && !restore_volatile {
                    tracing::trace!("skipping volatile file {}::{}", &pkg.metadata.name, filepath);
                    continue;
                }

                let state = get_filestate(&path);
                let mut modified = false;

                if fileinfo.filetype.is_dir() {

                    // as long as the path exists and it is a dir, then it is unmodified
                    if state.missing || !state.dir {
                        modified = true;
                    }

                } else if fileinfo.filetype.is_link() {

                    if state.missing || !state.link {
                        modified = true;
                    } else {
                        let db_link = fileinfo.filetype.get_link();
                        let db_link = db_link.as_ref().map(Utf8Path::new);

                        let fs_link = std::fs::read_link(&path);
                        match fs_link {
                            Err(_) => {
                                println!("error: cannot read link {}", path);
                                modified = true;
                            }
                            Ok(fs_link) => {
                                match Utf8PathBuf::from_path_buf(fs_link) {
                                    Err(path) => {
                                        println!("error: invalid path, non-utf8, {}", path.display());
                                        modified = true;
                                    }
                                    Ok(fs_link) => {
                                        if db_link != Some(fs_link.as_path()) {
                                            modified = true;
                                        }
                                    }
                                }
                            }
                        }
                    }

                } else if fileinfo.filetype.is_file() {

                    if state.missing || !state.file {
                        modified = true;
                    } else {

                        // path exists and it is a file
                        // check mtime or hash

                        let mut check_hash = true;

                        if allow_mtime {
                            let db_mtime = fileinfo.mtime;
                            if db_mtime.is_some() {
                                let fs_mtime = state.mtime;
                                //println!("db_mtime {:?}", db_mtime);
                                //println!("fs_mtime {:?}", fs_mtime);
                                if fs_mtime.is_some() && db_mtime == fs_mtime {
                                    //vout!(verbose, "  mtime same");
                                    check_hash = false;
                                }
                            }
                        }

                        if check_hash {
                            let db_hash = fileinfo.hash.as_ref().expect("installed file has no hash");
                            let file = File::open(&path)?;
                            let reader = std::io::BufReader::new(file);
                            let hash = blake3_hash_reader(reader)?;

                            //println!("     path  {}", &filepath);
                            //println!("  db_hash  {}", db_hash);
                            //println!("     hash  {}", hash);

                            if &hash != db_hash {
                                modified = true;
                            }
                        }
                    }
                }

                if modified {
                    pristine = false;
                    if state.missing {
                        println!(" deleted   {}", &filepath);
                    } else {
                        println!(" modified  {}", &filepath);
                    }
                    restore_files.insert(filepath.clone());
                } else {
                    vout!(verbose, "           {}", &filepath);
                }
            }

            println!("> {} -- {}", pkg.metadata.name, tern!(pristine, "OK", "MODIFIED"));

            //println!("restore_files {:?}", restore_files);
            if restore && !restore_files.is_empty() {

                let path = self.cache_package_require(&PackageID{
                    name: pkg.metadata.name.clone(),
                    version: pkg.metadata.version.clone(),
                }).context("could not find package file")?;
                let cache_file = std::fs::File::open(path).context("reading cached package file")?;

                // TODO potential to get a different package here
                // Should check that the meta data and data hash are the same
                // as the ones in the db (the installed version)

                let mut outer_tar = tar::Archive::new(&cache_file);
                let (mut data_file, _size) = package::seek_to_tar_entry(package::DATA_FILE_NAME, &mut outer_tar)?;
                let mut zstd = zstd::Decoder::new(&mut data_file)?;
                let mut data_tar = tar::Archive::new(&mut zstd);

                // if the install location is missing, try to create it
                let location_full = pkg.location.as_ref().context("package has no install location")?.full_path()?;
                if !location_full.exists() {
                    create_dir(&location_full).context("failed to create missing directory")?;
                }

                for ent in data_tar.entries()? {
                    let mut ent = ent.context("error reading tar")?;
                    let path = Utf8PathBuf::from(&ent.path()?.to_string_lossy());
                    let full_path = join_path_utf8!(&location_full, &path);
                    if let Some(xpath) = restore_files.take(&path) {
                        let _ok = ent.unpack(full_path);
                        //dbg!(_ok);
                        println!(" restored  {}", xpath);
                    }

                    // when there are no more files to restore, stop iterating the package
                    if restore_files.is_empty() {
                        break;
                    }
                }

                println!("> {} -- RESTORED", pkg.metadata.name);
            }
        }

        Ok(())
    }

    /// Iterate a collection of files and delete them.
    /// Delete symlinks and regular files first, then delete all directories afterward.
    fn delete_files<'a, I, P>(files: I, verbose: bool, remove_unowned: bool) -> AResult<()>
        where
            P : std::fmt::Debug + AsRef<Utf8Path>,
            I : Iterator<Item=(P, &'a package::FileInfo)>,
    {

        let mut dirs = Vec::new();

        for (filepath, fileinfo) in files {

            let filepath = filepath.as_ref();
            let exists = matches!(filepath.try_exists(), Ok(true));

            match &fileinfo.filetype {
                package::FileType::Link(to) => {
                    vout!(verbose, "delete {filepath} -> {to}");
                    tracing::trace!("delete  {filepath}");
                    let e = std::fs::remove_file(filepath);
                    if let Err(e) = e && exists {
                        eprintln!("error deleting {filepath}: {e}");
                    }
                }
                package::FileType::File => {
                    vout!(verbose, "delete {filepath}");
                    tracing::trace!("delete  {filepath}");
                    let e = std::fs::remove_file(filepath);
                    if let Err(e) = e && exists {
                        eprintln!("error deleting {filepath}: {e}");
                    }
                }
                package::FileType::Dir => {
                    dirs.push(filepath.to_path_buf());
                }
            }
        }

        dirs.reverse();
        for path in dirs {
            vout!(verbose, "delete {path}");
            tracing::trace!("delete  {path}");

            let exists = matches!(path.try_exists(), Ok(true));
            let e = if remove_unowned {
                std::fs::remove_dir_all(&path)
            } else {
                let ret = std::fs::remove_dir(&path);
                match ret {
                    Err(ref e) if e.kind() == std::io::ErrorKind::DirectoryNotEmpty => {
                        Ok(())
                    },
                    _ => ret,
                }
            };

            if let Err(e) = e && exists {
                eprintln!("error deleting {path}: {e}");
            }
        }

        Ok(())
    }

    fn delete_package_files(&self, pkg: &db::DbPkg, verbose: bool, remove_unowned: bool) -> AResult<()> {

        let location = pkg.location.as_ref().context("package has no install location")?;
        let location_full = location.full_path()?;

        let iter = pkg.metadata.files.iter().map(|(path, info)| {
            let path = join_path_utf8!(&location_full, path);
            (path, info)
        });

        Self::delete_files(iter, verbose, remove_unowned)
    }

    /// `bpm query owner <file>`
    pub fn query_owner(&mut self, file: &str) -> AResult<()> {

        // canonicalize the first existing parent path
        let partial_canonicalize = |path: &Utf8Path| {
            for parent in path.ancestors().skip(1) {
                //println!("ancestor {} exists {:?}", parent, parent.try_exists());
                if let Ok(true) = parent.try_exists() {
                    let rest = path.strip_prefix(parent);
                    let parent = parent.canonicalize_utf8();
                    //println!("  !! {:?}", parent);
                    //println!("  rest {:?}", rest);
                    if let (Ok(parent), Ok(rest)) = (parent, rest) {
                        return join_path_utf8!(parent, rest);
                    }
                    break;
                }
            }
            return path.to_path_buf();
        };

        let mut needle = Utf8PathBuf::from(file);

        if needle.is_relative() {
            let cwd = std::env::current_dir()?;
            let cwd = Utf8PathBuf::from_path_buf(cwd).expect("failed to get cwd");
            needle = join_path_utf8!(cwd, needle);
        }

        needle = partial_canonicalize(&needle);

        tracing::trace!("query_owner converted path from {} to {}", file, needle);

        self.load_db()?;
        for pkg in &self.db.installed {

            let pkg_loc = pkg.location.as_ref()
                .and_then(|p| p.full_path().ok())
                .and_then(|p| p.canonicalize_utf8().ok());

            if let Some(pkg_loc) = pkg_loc {
                //println!("checking package {}", &pkg.metadata.name);
                //println!("install location: {}", pkg_loc);
                if needle.starts_with(&pkg_loc) {
                    //println!("could be in this package");
                    for path in pkg.metadata.files.keys() {
                        let full_path = pkg_loc.join(path);
                        //println!("{:30} {}", path, full_path);
                        if full_path == needle {
                            println!("{} {}", &pkg.metadata.name, &pkg.metadata.version);
                            return Ok(());
                        }
                    }
                } else {
                    //println!("not in this package");
                    continue;
                }
            }
        }

        Ok(())
    }

    /// `bpm query list-files <pkg>`
    pub fn query_files(&mut self, pkg_name: &str, depth: Option<u32>, absolute: bool, show_type: bool) -> AResult<()> {

        let depth = depth.unwrap_or(0);

        let output = |root: &Utf8Path, path: &Utf8Path, info: &package::FileInfo| {
            if show_type {
                let c = match &info.filetype {
                    package::FileType::Dir => 'd',
                    package::FileType::File => 'f',
                    package::FileType::Link(to) => 's',
                };
                print!("{c} ");
            }
            if absolute {
                let path = root.join(path);
                println!("{}", path);
            } else {
                println!("{}", path);
            }
        };

        self.load_db()?;
        for pkg in &self.db.installed {
            if pkg.metadata.name == pkg_name {

                let root = pkg.location.as_ref().expect("package doesn't have an install location").full_path()?.canonicalize_utf8()?;

                for (path, info) in &pkg.metadata.files {
                    if depth > 0 {
                        let count = path.components().count();
                        if count <= depth as usize {
                            output(&root, path, info);
                        }
                    } else {
                        output(&root, path, info);
                    }
                }

                return Ok(());
            }
        }

        anyhow::bail!("package not installed")
    }

    /// `bpm scan`
    pub fn scan_cmd(&self, debounce: std::time::Duration) -> AResult<()> {

        tracing::trace!("scanning providers");

        let now = chrono::Utc::now().round_subsecs(0);
        tracing::trace!("now                {:?}", now);

        let mut skip_scan = true;

        if debounce.is_zero() {
            skip_scan = false;
        } else {
            // If any provider needs scanning, scan all (not all, just ones in filter).
            // A provider needs scan if the file doesn't exist.
            for provider in self.filtered_providers() {
                if let Ok(data) = provider.load_file() {
                    let debounce_timepoint = data.scan_time + debounce;
                    if debounce_timepoint < now {
                        skip_scan = false;
                    }
                    tracing::trace!("{:-18} {:?}", &provider.name, data.scan_time);
                    tracing::trace!("debounce_timepoint {:?} {}", debounce_timepoint, tern!(now > debounce_timepoint, "scan!", "skip"));
                } else {
                    tracing::trace!("error loading file, scanning");
                    skip_scan = false;
                }
            }
        }

        if skip_scan {
            tracing::trace!("skipping scan");
            return Ok(());
        }

        // make sure there is a cache/provider dir to cache search results in
        create_dir(join_path!(&self.config.cache_dir, "provider"))?;

        for provider in self.filtered_providers() {

            tracing::debug!("scanning {}", &provider.name);
            let list = provider.as_provide().scan();

            if let Ok(list) = list {

                let pkg_count = list.len();
                let ver_count : usize = list.iter().map(|p| p.1.len()).sum();
                tracing::debug!("[scan] {}: {} packages, {} versions", &provider.name, pkg_count, ver_count);

                // write the package list to a cache file
                let out = provider::ProviderFile {
                    scan_time: now,
                    packages: list,
                };

                //let s = serde_json::to_string(&out)?;
                let s = serde_json::to_string_pretty(&out)?;
                let mut file = File::create(&provider.cache_file)?;
                file.write_all(s.as_bytes())?;
            } else {
                tracing::debug!("error while scanning {}", &provider.name);
                let out = provider::ProviderFile {
                    scan_time: now,
                    packages: BTreeMap::new(),
                };
                let s = serde_json::to_string_pretty(&out)?;
                let mut file = File::create(&provider.cache_file)?;
                file.write_all(s.as_bytes())?;
            }
        }
        Ok(())
    }

    /// `bpm cache clear`
    pub fn cache_clear(&mut self) -> AResult<()> {

        let retention = self.config.cache_retention;

        let now = chrono::Utc::now().round_subsecs(0);
        tracing::trace!("cache retention {:?} ({})", retention, now - retention);

        self.load_db()?;

        // gather a list of package files in the cache
        let pkg_dir = join_path!(&self.config.cache_dir, "packages");

        for path in walkdir::WalkDir::new(&pkg_dir)
            .max_depth(1) // package files are a flat list at the root dir
            .into_iter()
            .skip(1)      // skip the root dir
            .flatten()    // skip error entries
            .filter_map(|entry| Utf8Path::from_path(entry.path()).map(Utf8Path::to_path_buf))
        {
            if let Some(filename) = path.file_name() {
                let mut remove = false;

                let mut touch = None;
                let mut in_use = false;

                match self.db.cache_files.iter().find(|e| e.filename == filename) {
                    None => {
                        remove = true;
                    }
                    Some(ent) => {
                        in_use = ent.in_use;
                        touch = Some(ent.touched);

                        if !ent.in_use {

                            let retention = ent.retention.unwrap_or(retention);

                            let remove_time = ent.touched + retention;
                            if remove_time < now {
                                remove = true;
                            }
                        }
                    }
                }

                tracing::trace!(touch=?touch, in_use=in_use, "cache {} {}", tern!(remove, "remove", "keep"), filename);

                if remove {
                    let pkg_path = join_path!(&pkg_dir, &filename);
                    let ok = std::fs::remove_file(&pkg_path);
                    if let Err(e) = ok {
                        eprintln!("failed to remove {}: {}", pkg_path.display(), e);
                    }

                    self.db.cache_files.retain(|e| e.filename != filename);
                }
            }
        }

        self.save_db()?;
        Ok(())
    }

    /// `bpm cache evict`
    pub fn cache_evict(&mut self, pkg: &str, version: Option<&String>, in_use: bool) -> AResult<()> {

        self.load_db()?;

        tracing::trace!("cache evict {}", pkg);

        for entry in walkdir::WalkDir::new(join_path_utf8!(&self.config.cache_dir, "packages"))
            .max_depth(1)
            .into_iter()
            .skip(1) // skip the cache/packages dir
            .flatten()
        {
            let path = Utf8Path::from_path(entry.path());

            if let Some(path) = path {
                if let Some(fname) = path.file_name() {
                    if package::is_packagefile_name(fname) {
                        if let Some((pname, pversion)) = package::split_parts(fname) {

                            //tracing::trace!(name=pname, version=pversion, "cache walk");

                            if pname != pkg {
                                continue;
                            }

                            if version.is_some() && version.unwrap() != pversion {
                                continue;
                            }

                            let find = self.db.cache_files.iter().find(|ent| ent.filename == fname);
                            let p_in_use = find.map(|ent| ent.in_use).unwrap_or(false);
                            if p_in_use && !in_use {
                                tracing::trace!("cache evict skip, file in use {}", fname);
                                continue;
                            }

                            tracing::trace!(file=fname, "cache delete");
                            let _ = std::fs::remove_file(path);
                            self.db.cache_evict(fname);
                        }
                    }
                }
            }
        }

        self.save_db()?;

        Ok(())
    }

    /// `bpm cache list`
    pub fn cache_list(&mut self) -> AResult<()> {

        self.load_db()?;

        tracing::trace!("cache list");

        let mut tw = tabwriter::TabWriter::new(std::io::stdout());
        let _ = writeln!(&mut tw, "name\tversion\tfilename\tsize\tin use\texpiration");

        let retention = self.config.cache_retention;
        let now = chrono::Utc::now().round_subsecs(0);

        for entry in walkdir::WalkDir::new(join_path_utf8!(&self.config.cache_dir, "packages"))
            .max_depth(1)
            .sort_by_file_name()
            .into_iter()
            .skip(1) // skip the cache/packages dir
            .flatten()
        {
            let path = Utf8Path::from_path(entry.path());

            if let Some(path) = path {
                if let Some(fname) = path.file_name() {
                    if package::is_packagefile_name(fname) {
                        if let Some((name, version)) = package::split_parts(fname) {

                            let find = self.db.cache_files.iter().find(|ent| ent.filename == fname);
                            let retention = find.and_then(|ent| ent.retention).unwrap_or(retention);

                            let in_use = if find.map(|ent| ent.in_use).unwrap_or(false) {
                                "yes"
                            } else {
                                "no"
                            };

                            let mut duration = String::new();
                            if let Some(touch) = find.map(|ent| ent.touched) {
                                let expire = touch + retention;
                                if now < expire {
                                    let d = (expire- now).to_std().unwrap();
                                    duration = humantime::format_duration(d).to_string();
                                } else {
                                    let d = (now - expire).to_std().unwrap();
                                    duration = humantime::format_duration(d).to_string();
                                    duration.push_str(" ago");
                                }
                            };

                            let size = get_filesize(path.as_str()).map_or(String::from("?"), |s| format!("{}", indicatif::HumanBytes(s)));
                            let _ = writeln!(&mut tw, "{}\t{}\t{}\t{}\t{}\t{}", name, version, fname, size, in_use, duration);
                        }
                    }
                }
            }
        }

        let _ = tw.flush();

        Ok(())
    }

    /// `bpm cache fetch name@version`
    /// or
    /// `bpm cache fetch /path/to/packagefile`
    pub fn cache_fetch(&mut self, mut pkg_name: &str) -> AResult<()> {

        tracing::trace!("cache fetch {}", pkg_name);

        if let Some((path, name, v)) = Self::is_package_file_arg(pkg_name) {
            tracing::trace!("cache fetch directly from file {}", path);
            self.cache_store_file(&path)?;
            let _ = self.cache_touch(name, Some(v.to_string()).as_ref(), None);
            return Ok(());
        }

        let mut split = pkg_name.split('@');

        pkg_name = match split.next() {
            Some("") | None => {
                anyhow::bail!("empty package name");
            }
            Some(name) => name
        };

        let v = split.next();

        let (listing, _versioning) = self.find_package_version(pkg_name, v)?;
        let version = Version::new(&listing.version);

        let id = PackageID {
            name: pkg_name.to_string(),
            version: version.to_string(),
        };

        let cache_path = self.cache_package_require(&id)?;
        tracing::trace!("cache fetched {}", cache_path);

        self.load_db()?;
        if let Some(fname) = cache_path.file_name() {
            self.db.cache_touch(fname, None);
        }
        self.save_db()?;

        Ok(())
    }

    /// `bpm cache touch`
    pub fn cache_touch(&mut self, pkg: &str, version: Option<&String>, duration: Option<std::time::Duration>) -> AResult<()> {

        self.load_db()?;

        tracing::trace!(pkg, version, duration=?duration, "cache touch");

        for entry in walkdir::WalkDir::new(join_path_utf8!(&self.config.cache_dir, "packages"))
            .max_depth(1)
            .into_iter()
            .skip(1) // skip the cache/packages dir
            .flatten()
        {
            let path = Utf8Path::from_path(entry.path());

            if let Some(path) = path {
                if let Some(fname) = path.file_name() {
                    if package::is_packagefile_name(fname) {
                        if let Some((pname, pversion)) = package::split_parts(fname) {

                            if pname != pkg {
                                continue;
                            }

                            if version.is_some() && version.unwrap() != pversion {
                                continue;
                            }

                            tracing::trace!(file=fname, duration=?duration, "cache touch");
                            self.db.cache_touch(fname, duration);
                        }
                    }
                }
            }
        }

        self.save_db()?;

        Ok(())
    }

    /// LOOKUP a package in the cache.
    /// Given a PackageID, find the package file in the cache dir if it exists
    fn cache_package_lookup(&self, id: &PackageID) -> Option<Utf8PathBuf> {

        for entry in walkdir::WalkDir::new(join_path_utf8!(&self.config.cache_dir, "packages"))
            .max_depth(1)
            .into_iter()
            .flatten()
        {
            let filename = entry.file_name().to_string_lossy();
            if package::filename_match(&filename, id) {
                tracing::trace!("cache_package_lookup() hit {}", filename);
                let path = Utf8PathBuf::from_path_buf(entry.into_path());
                return match path {
                    Ok(path) => Some(path),
                    Err(_) => {
                        tracing::error!("cache hit invalid non-utf8 path");
                        None
                    }
                }
                //return Some(entry.into_path());
            }

        }
        tracing::trace!("cache_package_lookup() miss {} {}", id.name, id.version);
        None
    }

    /// LOOKUP a package in the cache OR FETCH a package and cache it.
    fn cache_package_require(&self, id: &PackageID) -> AResult<Utf8PathBuf> {
        let cached_file = match self.cache_package_lookup(id) {
            Some(cached_file) => cached_file,
            None => self.cache_fetch_cmd(id)?,
        };
        Ok(cached_file)
    }

    /// Directly copy a file into the cache dir
    pub fn cache_store_file(&self, path: &Utf8Path) -> AResult<Utf8PathBuf> {

        // check that the package file is not a path to a file in the cache dir
        // (do not copy in place)

        let filename = path.file_name().context("path has no filename")?;
        let cache_path = join_path_utf8!(&self.config.cache_dir, "packages", filename);

        let in_cache_dir = {
            let cache_path = cache_path.canonicalize_utf8().ok();
            let path = path.canonicalize_utf8().ok();
            tracing::trace!("{:?} == {:?} {}",  cache_path, path, cache_path == path);
            path == cache_path
        };

        if !in_cache_dir {

            // copy to the cache dir
            tracing::trace!("copying {} to {}", path, cache_path);

            let filesize = get_filesize(cache_path.as_str()).ok().unwrap_or(0);
            let pbar = indicatif::ProgressBar::new(filesize);
            pbar.set_style(indicatif::ProgressStyle::with_template(
                " {spinner:.green} caching package {wide_bar:.green} {bytes_per_sec}  {bytes}/{total_bytes} "
            ).unwrap());

            std::io::copy(
                &mut File::open(path)?,
                &mut pbar.wrap_write(&mut File::create(&cache_path)?)
            ).context("failed to copy package file into cache")?;

            pbar.finish_and_clear();

            // TODO could validate the package here, and remove it if it fails
        }

        Ok(cache_path)
    }

    /// fetch a package by name AND version.
    /// network: yes
    pub fn cache_fetch_cmd(&self, id: &PackageID) -> AResult<Utf8PathBuf> {

        tracing::debug!(pkg=id.name, version=id.version, "cache fetch");

        // ensure that we have the cache/packages dir
        std::fs::create_dir_all(join_path_utf8!(&self.config.cache_dir, "packages")).context("failed to create cache/packages dir")?;

        let temp_name = format!("temp_download_{}", std::process::id());
        let temp_path = join_path_utf8!(&self.config.cache_dir, temp_name);
        let mut file = File::create(&temp_path)?;

        for provider in self.filtered_providers() {

            if let Ok(provider::ProviderFile{packages, ..}) = provider.load_file() {
                if let Some(versions) = packages.get(&id.name) {
                    if let Some(x) = versions.get(&id.version.as_str().into()) {

                        let final_path = join_path_utf8!(&self.config.cache_dir, "packages", &x.filename);

                        let result = provider.as_provide().fetch(&mut file, id, &x.url);
                        if result.is_ok() {
                            file.sync_all()?;
                            drop(file);
                            tracing::trace!("moving fetched package file from {} to {}", temp_path, final_path);
                            std::fs::rename(&temp_path, &final_path).context("failed to move file")?;
                            //println!("final_path {:?}", final_path);
                            return Ok(final_path);
                        }
                    }
                }
            }
        }

        Err(anyhow::anyhow!("failed to fetch package"))
    }

} // impl App

//fn path_to_string(path: &Path) -> String {
//    path.to_string_lossy().to_string()
//}
