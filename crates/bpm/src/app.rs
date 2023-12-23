use crate::*;
use std::io::BufWriter;
use std::io::IsTerminal;
use chrono::SubsecRound;
use package::PackageID;
use std::fs::File;
use std::io::Seek;
use std::io::Read;
use std::collections::HashSet;
use std::collections::BTreeMap;

use bpmutil::*;

mod list;

#[derive(Debug)]
pub struct App {
    pub config: config::Config,
    pub db: db::Db,
    pub db_loaded: bool,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Versioning {
    pub pinned_to_version: bool,
    pub pinned_to_channel: bool,
    pub channel: Option<String>,
}

impl Versioning {
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

    /// search and merge each provider's cached package info
    fn search_results(&self, pkg_name: &str, exact: bool) -> AResult<search::PackageList> {

        let mut merged_results = search::PackageList::new();

        for provider in &self.config.providers {

            // load the provider's cache file and merge it's results
            if let Ok(data) = provider.load_file() {

                let mut cached_results = data.packages;

                cached_results.retain(|name, _versions| {
                    if exact {
                        name == pkg_name
                    } else {
                        name.contains(pkg_name)
                    }
                });
                merged_results = search::merge_package_lists(merged_results, cached_results);
            } else {
                tracing::trace!(file=?provider.cache_file, "couldn't read cache file for provider '{}'", provider.name);
            }
        }

        Ok(merged_results)
    }

    fn get_mountpoint_dir(&self, metadata: &package::MetaData) -> AResult<Utf8PathBuf> {
        let pkg_mount_point = metadata.mount.as_deref();
        let mount_point = self.config.get_mountpoint(pkg_mount_point);
        match mount_point {
            config::MountPoint::Specified(mp) |
            config::MountPoint::Default(mp) => Ok(mp),
            config::MountPoint::DefaultDisabled => anyhow::bail!("attempt to use default target, which is disabled"),
            config::MountPoint::Invalid{name} => anyhow::bail!("package using an invalid mountpoint: {name}"),
        }
    }

    /// install a package from a local package file.
    fn install_pkg_file(&mut self, file_path: Utf8PathBuf, versioning: Versioning) -> AResult<()> {

        tracing::debug!("install_pkg_file {}", file_path);

        let package_file_filename = file_path.file_name().context("invalid filename")?;

        let pkg_name;
        let pkg_version;
        match (package::is_packagefile_name(&package_file_filename), package::split_parts(package_file_filename)) {
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
        let mut metadata = package::get_metadata(&mut file).context("error reading metadata")?;

        // make sure the checksum matches
        if !package::package_integrity_check(&mut file)? {
            anyhow::bail!("failed to install {}, package file failed integrity check", pkg_name);
        }

        // package filename must match package metadata
        // name must match
        if metadata.name != pkg_name {
            anyhow::bail!("failed to installed {}, package file is corrupt. Package name does not match package metadata.", pkg_name);
        }
        // version must match
        if metadata.version != pkg_version.to_string() {
            anyhow::bail!("failed to installed {}, package file is corrupt. Package version does not match package metadata.", pkg_name);
        }

        let install_dir = self.get_mountpoint_dir(&metadata)?;

        // create the mount point dir if it doesn't exist
        if !install_dir.exists() {
            std::fs::create_dir_all(&install_dir)?;
        }

        // get the list of files and their hashes from the meta file
        //let files = package::get_filelist(&mut file)?;
        //let files = &metadata.files;

        // obtain reader for embeded data archive
        file.rewind()?;
        let mut outer_tar = tar::Archive::new(&mut file);
        let mut inner_tar = package::seek_to_tar_entry("data.tar.zst", &mut outer_tar)?;
        let mut zstd = zstd::stream::read::Decoder::new(&mut inner_tar)?;
        let mut data_tar = tar::Archive::new(&mut zstd);

        // unpack all files individually
        for entry in data_tar.entries()? {
            let mut entry = entry?;

            let installed_ok = entry.unpack_in(&install_dir)?;

            let path = Utf8PathBuf::from_path_buf(entry.path().unwrap().into_owned()).unwrap();
            let installed_path = join_path_utf8!(&install_dir, &path);

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
        }

        let mut details = db::DbPkg::new(metadata);
        details.location = install_dir.into();
        details.versioning = versioning;
        details.package_file_filename = Some(package_file_filename.to_string());

        self.db.add_package(details);

        self.db.cache_touch(package_file_filename);
        self.db.cache_unuse_all_versions(pkg_name);
        self.db.cache_set_in_use(package_file_filename, true);

        self.save_db()?;

        Ok(())
    }

    fn find_package_version(&self, pkg_name: &str, which: Option<&str>) -> AResult<(search::SingleListing, Versioning)> {

        tracing::trace!(pkg_name, which, "find_package_version");

        // get search results for just this package name
        let mut results = self.search_results(pkg_name, true)?;

        if results.len() != 1 {
            anyhow::bail!("could not find package '{pkg_name}'");
        }

        let versions = results.iter_mut().next().unwrap().1;

        // `which` could be a channel, check if it is
        //let mut channel = None;
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

                versions.retain(|version, _info| version == v);
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

        //dbg!(versions);

        // take the greatest package version
        let result = search::flatten(results);
        let result = result.into_iter().next().unwrap();

        //dbg!(&result);
        //dbg!(&versioning);

        Ok((result, versioning))
    }

    /// subcommand for installing a package
    pub fn install_cmd(&mut self, pkg_name: &String, no_pin: bool) -> AResult<()> {

        self.create_load_db()?;

        // make sure we have a cache dir to save the package in
        create_dir(&self.config.cache_dir)?;

        // installing from a package file
        {
            let path = Utf8Path::new(pkg_name);
            let filename = path.file_name();
            if let Ok(true) = path.try_exists() {
                tracing::trace!(path=?path, "install from file");
                if let Some(filename) = filename && package::is_packagefile_name(filename) {
                    tracing::trace!("installing directly from a package file {pkg_name}");

                    // TODO check that the package file is not a path to a file in the cache dir
                    // (DO NOT COPY IN PLACE)

                    let cache_path = join_path_utf8!(&self.config.cache_dir, filename);

                    // copy to the cache dir
                    std::fs::copy(path, &cache_path).context("failed to copy package file")?;

                    //TODO sanity check file contents

                    return self.install_pkg_file(cache_path, Versioning::pinned_version());
                } else {
                    anyhow::bail!("invalid package file")
                }
            }
        }

        // installing from provider

        let mut split = pkg_name.split('@');

        let pkg_name = match split.next() {
            Some("") | None => {
                anyhow::bail!("empty package name");
            }
            Some(name) => name,
        };

        let (listing, mut versioning) = self.find_package_version(pkg_name, split.next())?;
        let version = Version::new(&listing.version);

        if no_pin {
            versioning.pinned_to_version = false;
        }

        //dbg!(&listing);
        //dbg!(&versioning);
        //dbg!(&no_pin);
        //dbg!(&version);

        tracing::debug!(pkg_name, version=version.as_str(), "installing from provider");

        let id = PackageID {
            name: pkg_name.to_string(),
            version: version.to_string(),
        };

        let cached_file = self.cache_package_require(&id)?;

        tracing::debug!("installing {pkg_name} {version}");
        return self.install_pkg_file(cached_file, versioning);
    }

    /// uninstall a package
    pub fn uninstall_cmd(&mut self, pkg_name: &String) -> AResult<()> {
        // from the package name,
        // find all files that belong to this package from the db

        self.load_db()?;

        let found = self.db.installed.iter().find(|e| &e.metadata.name == pkg_name);

        if found.is_none() {
            println!("package '{pkg_name}' not installed");
            std::process::exit(0);
            //anyhow::bail!("package '{pkg_name}' not installed"),
        }

        let pkg = found.unwrap();
        let package_file_filename = pkg.package_file_filename.clone();

        self.delete_package_files(pkg)?;
        self.db.remove_package(pkg.metadata.id());
        if let Some(filename) = package_file_filename {
            self.db.cache_touch(&filename);
            self.db.cache_set_in_use(&filename, false);
        }
        self.save_db()?;
        Ok(())
    }

    fn update_inplace(&mut self, pkg_name: String, new_pkg_file: Utf8PathBuf, versioning: Versioning) -> AResult<()> {

        let cache_file = &new_pkg_file;
        tracing::trace!("update_inplace {cache_file:?}");

        // gather info about the old package (the currently installed version)
        let current_pkg_info = self.db.installed.iter().find(|e| e.metadata.name == pkg_name).context("package is not currently intalled")?;
        //dbg!(&current_pkg_info);
        let mut old_files = current_pkg_info.metadata.files.clone();
        //dbg!(old_files.keys());

        let mut new_package_fd = std::fs::File::open(cache_file)?;
        let metadata = package::get_metadata(&mut new_package_fd)?;

        // make sure the checksum matches
        if !package::package_integrity_check(&mut new_package_fd)? {
            anyhow::bail!("failed to install {}, package file failed integrity check", pkg_name);
        } else {
            //tracing::trace!
        }

        let new_files = metadata.files.clone();
        dbg!(&new_files.keys());

        // old_files -= new_files
        for (path, new_file) in &new_files {
            let old_file = old_files.get(path);
            if let Some(old_file) = old_file {
                if old_file.filetype != new_file.filetype {
                } else {
                    // same path and type, can keep it around
                    old_files.remove_entry(path);
                }
            }
        }

        // these are the files to remove
        let remove_files = old_files;
        //dbg!(remove_files.keys());

        let iter = remove_files.iter().map(|(path, info)| {
            let path = build_pkg_file_path(current_pkg_info, &path).unwrap();
            (path, info)
        });

        let delete_ok = Self::delete_files(iter);
        if let Err(e) = delete_ok {
            eprintln!("error deleting files {:?}", e);
        }

        let location = current_pkg_info.location.as_ref().context("installed package has no location")?.clone();
        println!("installing to the same location {}", location);

        let mut skip_files = HashSet::new();

        for (path, info) in &new_files {
            // if already exists, check hash
            let fullpath = join_path_utf8!(&location, &path);
            if let Ok(true) = fullpath.try_exists() {
                if !info.filetype.is_dir() {
                    println!("getting hash for {}", fullpath);
                    let mut file = File::open(&fullpath)?;
                    let hash = blake3_hash_reader(&mut file)?;

                    if Some(&hash) == info.hash.as_ref() {
                        // this file can be skipped during update
                        skip_files.insert(path);
                    } else {
                        // overwrite this file
                        println!("{} != {}", &hash, info.hash.as_ref().unwrap());
                        println!("this file must be re-installed");
                    }
                }
            }
        }

        println!("skip files {:?}", skip_files);

        new_package_fd.rewind()?;
        let mut outer_tar = tar::Archive::new(&mut new_package_fd);
        let mut inner_tar = package::seek_to_tar_entry("data.tar.zst", &mut outer_tar)?;
        let mut zstd = zstd::stream::read::Decoder::new(&mut inner_tar)?;
        let mut data_tar = tar::Archive::new(&mut zstd);

        for entry in data_tar.entries()? {
            let mut entry = entry?;
            let path = entry.path()?;
            //dbg!(&path);
            //let x = path.as_ref().to_string_lossy().to_string(); //TODO dd
            let path = Utf8PathBuf::from(&path.to_string_lossy());
            if skip_files.remove(&path) {
                tracing::trace!("skipping   {}", path);
            } else {
                tracing::trace!("updating   {}", path);
                let _ok = entry.unpack_in(&location);
                //dbg!(_ok);
            }
        }

        let mut details = db::DbPkg::new(metadata);
        details.location = Some(location);
        details.versioning = versioning;
        let pkg_filename = new_pkg_file.file_name().map(str::to_string);
        details.package_file_filename = pkg_filename;

        //dbg!(&details);
        self.db.add_package(details);

        //self.db.cache_touch(&new_pkg_file);
        //self.db.cache_set_in_use(package_file_filename, true);

        self.save_db()?;

        Ok(())
    }

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

            println!("{}: current version {}", pkg.metadata.name, pkg.metadata.version);

            // updates stay on a channel?
            let mut channel = None;
            if pkg.versioning.pinned_to_channel {
                if let Some(c) = &pkg.versioning.channel {
                    channel = Some(c.clone());
                }
            }

            let result = self.find_package_version(&pkg.metadata.name, channel.as_deref());

            if let Ok((listing, versioning)) = result {
                //dbg!(&listing);
                //dbg!(&versioning);
                let version = listing.version;
                println!("{}: updating to {}", pkg.metadata.name, version);

                let cached_file = self.cache_package_require(&PackageID{
                    name: pkg.metadata.name.clone(),
                    version: version.to_string(),
                });

                if cached_file.is_err() {
                    println!("failed to cache fetch {} {}", pkg.metadata.name, version);
                    continue;
                }

                let cached_file = cached_file.unwrap();
                dbg!(&cached_file);
                //self.update_inplace(&pkg, cached_file)?;
                updates.push((pkg.metadata.name.clone(), pkg.metadata.version.clone(), version, versioning, cached_file));
            }
        }

        println!("updates to apply:");
        for (name, oldv, newv, _versioning, _pkgfile) in &updates {
            println!("{} : {} -> {}", name, oldv, newv);
        }

        //TODO can put confirmation prompt here

        dbg!(&updates);
        for (pkg_name, _oldv, _newv, versioning, new_pkg_file) in updates {
            dbg!(&pkg_name);
            dbg!(&new_pkg_file);
            let _ok = self.update_inplace(pkg_name, new_pkg_file, versioning)?;
            dbg!(_ok);
        }

        Ok(())
    }

    /// Verify package state.
    ///
    /// For installed packages listed in the db,
    /// walk each file and hash the version we have on disk
    /// and compare that to the hash stored in the db.
    pub fn verify_cmd<S>(&mut self, pkgs: &Vec<S>, restore: bool, verbose: bool, allow_mtime: bool) -> AResult<()>
        where S: AsRef<str>,
    {
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
            for (filepath, fileinfo) in pkg.metadata.files.iter() {

                let path = join_path_utf8!(&root_dir, filepath);
                //println!("{}", path);

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
                let mut data_file = package::seek_to_tar_entry(package::DATA_FILE_NAME, &mut outer_tar)?;
                let mut zstd = zstd::Decoder::new(&mut data_file)?;
                let mut data_tar = tar::Archive::new(&mut zstd);

                for ent in data_tar.entries()? {
                    let mut ent = ent.context("error reading tar")?;
                    let path = Utf8PathBuf::from(&ent.path()?.to_string_lossy());
                    let full_path = build_pkg_file_path(pkg, &path)?;
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

                println!("> {} -- {}", pkg.metadata.name, "RESTORED");
            }
        }

        Ok(())
    }

    fn delete_files<'a, I, P>(files: I) -> AResult<()>
        where
            P : std::fmt::Debug + AsRef<Utf8Path>,
            I : Iterator<Item=(P, &'a package::FileInfo)>,
    {

        let mut dirs = Vec::new();

        for (filepath, fileinfo) in files {
            //let path = PathBuf::from(filepath.as_ref());

            let filepath = filepath.as_ref();
            let exists = matches!(filepath.try_exists(), Ok(true));

            match &fileinfo.filetype {
                package::FileType::Link(to) => {
                    //let path = build_pkg_file_path(pkg, filepath)?;
                    println!("removing {filepath:?} -> {to}");
                    //println!("         {}", path.display());
                    let e = std::fs::remove_file(filepath);
                    if let Err(e) = e && exists {
                        eprintln!("error deleting {}", filepath);
                    }
                }
                package::FileType::File => {
                    //let path = build_pkg_file_path(pkg, filepath)?;
                    println!("removing {filepath:?}");
                    //println!("         {}", path.display());
                    let e = std::fs::remove_file(filepath);
                    if let Err(e) = e && exists {
                        eprintln!("error deleting {}", filepath);
                    }
                }
                package::FileType::Dir => {
                    //let path = build_pkg_file_path(pkg, filepath)?;
                    //dirs.push(path);
                    dirs.push(filepath.to_path_buf());
                }
            }
        }

        dirs.reverse();
        for path in dirs {
            println!("removing dir {path:?}");

            let exists = matches!(path.try_exists(), Ok(true));
            let e = std::fs::remove_dir(&path);
            if let Err(e) = e && exists {
                eprintln!("error deleting {}", path);
            }
        }

        Ok(())
    }

    fn delete_package_files(&self, pkg: &db::DbPkg) -> AResult<()> {

        let location = pkg.location.as_ref().context("package has no install location")?;

        let iter = pkg.metadata.files.iter().map(|(path, info)| {
            let path = join_path_utf8!(&location, path);
            (path, info)
        });

        Self::delete_files(iter)

//        //dbg!(pkg);
//
//        let mut dirs = Vec::new();
//
//        for (filepath, fileinfo) in &pkg.metadata.files {
//            //println!("removing {filepath}");
//            //dbg!(fileinfo);
//            match &fileinfo.filetype {
//                package::FileType::Link(to) => {
//                    let path = build_pkg_file_path(pkg, filepath)?;
//                    println!("removing {filepath} -> {to}");
//                    println!("         {}", path.display());
//                    std::fs::remove_file(path)?;
//                }
//                package::FileType::File => {
//                    let path = build_pkg_file_path(pkg, filepath)?;
//                    println!("removing {filepath}");
//                    println!("         {}", path.display());
//                    std::fs::remove_file(path)?;
//                }
//                package::FileType::Dir => {
//                    let path = build_pkg_file_path(pkg, filepath)?;
//                    dirs.push(path);
//                }
//            }
//        }
//
//        dirs.reverse();
//        for path in dirs {
//            println!("removing dir {}", path.display());
//            std::fs::remove_dir(path)?;
//        }
//
//        Ok(())
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

            if let Some(pkg_loc) = pkg.location.as_ref().and_then(|p| p.canonicalize_utf8().ok()) {
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

                let root = pkg.location.as_ref().expect("package doesn't have an install location");
                let root = root.canonicalize_utf8()?;

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

    pub fn scan_cmd(&self, provider_filter: provider::ProviderFilter, debounce: std::time::Duration) -> AResult<()> {

        tracing::trace!("scanning providers");

        let now = chrono::Utc::now().round_subsecs(0);
        tracing::trace!("now                {:?}", now);

        let mut skip_scan = true;

        if debounce.is_zero() {
            skip_scan = false;
        } else {
            // if any provider needs scanning, scan all
            for provider in provider_filter.filter(&self.config.providers) {
                if let Ok(data) = provider.load_file() {
                    let debounce_timepoint = data.scan_time + debounce;
                    if debounce_timepoint < now {
                        skip_scan = false;
                    }
                    tracing::trace!("{:-18} {:?}", &provider.name, data.scan_time);
                    tracing::trace!("debounce_timepoint {:?} {}", debounce_timepoint, tern!(now > debounce_timepoint, "scan!", "skip"));
                }
            }
        }

        if skip_scan {
            tracing::trace!("skipping scan");
            return Ok(());
        }

        // make sure there is a cache/provider dir to cache search results in
        create_dir(join_path!(&self.config.cache_dir, "provider"))?;

        for provider in provider_filter.filter(&self.config.providers) {

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

    pub fn cache_clear(&mut self) -> AResult<()> {

        let retention = chrono::Duration::from_std(self.config.cache_retention)?;
        //let retention = chrono::Duration::seconds(10);

        let now = chrono::Utc::now().round_subsecs(0);
        tracing::trace!("cache retention {} ({})", retention, now - retention);

        self.load_db()?;

        // gather a list of package files in the cache
        let pkg_dir = join_path!(&self.config.cache_dir, "packages");
        let mut pkgs = Vec::new();

        for entry in walkdir::WalkDir::new(&pkg_dir)
            .max_depth(1) // package files are a flat list at the root dir
            .into_iter()
            .skip(1)      // skip the root dir
            .flatten()    // skip error entries
        {
            pkgs.push(entry.file_name().to_string_lossy().to_string());
        }

        for filename in pkgs {
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

        self.save_db()?;
        Ok(())
    }

    pub fn cache_touch(&mut self, filename: &str, duration: Option<std::time::Duration>) -> AResult<()> {

        dbg!(&filename);
        dbg!(&duration);

        //if let Ok(true) = std::fs::try_exists(filename) {
        //}

        todo!();
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

    /// fetch a package by name AND version.
    /// network: yes
    pub fn cache_fetch_cmd(&self, id: &PackageID) -> AResult<Utf8PathBuf> {

        tracing::debug!(pkg=id.name, version=id.version, "cache fetch");

        // ensure that we have the cache/packages dir
        std::fs::create_dir_all(join_path_utf8!(&self.config.cache_dir, "packages")).context("failed to create cache/packages dir")?;

        let temp_name = format!("temp_download_{}", std::process::id());
        let temp_path = join_path_utf8!(&self.config.cache_dir, temp_name);
        let mut file = File::create(&temp_path)?;

        for provider in &self.config.providers {

            if let Ok(provider::ProviderFile{packages, ..}) = provider.load_file() {
                if let Some(versions) = packages.get(&id.name) {
                    if let Some(x) = versions.get(&id.version) {
                        //dbg!(x);

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

/// Build a full filepath from a relative path from a package file.
fn build_pkg_file_path<T: AsRef<str>>(pkg: &db::DbPkg, path: &T) -> AResult<Utf8PathBuf> {
    let mut fullpath = Utf8PathBuf::from(pkg.location.as_ref().context("package has no install location")?);
    fullpath.push(path.as_ref());
    Ok(fullpath)

    //note: return Ok(fullpath.canonicalize()?) // can't canonicalize because that resolves symlinks
}

fn path_to_string(path: &Path) -> String {
    path.to_string_lossy().to_string()
}
