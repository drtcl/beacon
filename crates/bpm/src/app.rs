use crate::*;
use std::io::BufWriter;
use std::io::IsTerminal;
use package::PackageID;
use std::fs::File;

mod list;

#[derive(Debug)]
pub struct App {
    pub config: config::Config,
    pub db: db::Db,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Versioning {
    pub pinned_to_version: bool,
    pub pinned_to_channel: bool,
    pub channel: Option<String>,
}

impl App {
    pub fn load_db(&mut self) -> AResult<()> {
        tracing::trace!("loading database");

        if !self.config.db_file.exists() {
            tracing::warn!("database file does not exist");
        }

        let mut db_file = File::open(&self.config.db_file).context("cannot read database file")?;
        self.db = db::Db::from_reader(&mut db_file).context("failed to load database")?;
        Ok(())
    }

    pub fn create_load_db(&mut self) -> AResult<()> {

        tracing::trace!("loading database");

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
        let mut path = self.config.db_file.clone();
        let ext = path.extension().map_or("tmp".into(), |e| format!("{}.tmp", e.to_string_lossy()));
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
            if let Ok(mut cached_results) = provider.load_cache() {

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

    fn get_cached_package_path(&self, id: &PackageID) -> Option<PathBuf> {

        for entry in walkdir::WalkDir::new(&self.config.cache_dir) {
            if let Ok(entry) = entry {
                let filename = entry.file_name().to_string_lossy();
                if package::filename_match(&filename, id) {
                    tracing::trace!("found cached package file {}", filename);
                    return Some(entry.into_path());
                }
            }
        }
        tracing::trace!("package file not found in cache");
        None
    }

    /// fetch a package by name AND version
    pub fn cache_fetch_cmd(&self, id: &PackageID) -> AResult<PathBuf> {

        tracing::debug!(pkg=id.name, version=id.version, "cache fetch");

        // ensure that we have the cache/packages dir
        std::fs::create_dir_all(join_path!(&self.config.cache_dir, "packages")).context("failed to create cache/packages dir")?;

        let temp_path = join_path!(&self.config.cache_dir, "temp.download");
        let mut file = File::create(&temp_path)?;

        for provider in &self.config.providers {
            if let Ok(packages) = provider.load_cache() {
                if let Some(versions) = packages.get(&id.name) {
                    if let Some(x) = versions.get(&id.version) {
                        dbg!(x);

                        let final_path = join_path!(&self.config.cache_dir, "packages", &x.filename);

                        let result = provider.as_provide().fetch(&mut file, &id, &x.url);
                        if result.is_ok() {
                            file.sync_all()?;
                            drop(file);
                            tracing::trace!("moving fetched package file from {} to {}", temp_path.display(), final_path.display());
                            std::fs::rename(&temp_path, &final_path).context("failed to move file")?;
                            return Ok(final_path);
                        }
                    }
                }
            }
        }

        Err(anyhow::anyhow!("failed to fetch package"))
    }

    fn get_mountpoint_dir(&self, metadata: &package::MetaData) -> AResult<PathBuf> {
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
    fn install_pkg_file(&mut self, file_path: PathBuf, versioning: Versioning) -> AResult<()> {

        tracing::debug!("install_pkg_file {}", file_path.display());

        let file_name = file_path
            .file_name().context("invalid filename")?
            .to_str().context("invalid filename")?;

        //dbg!(file_name);

        let pkg_name;
        let pkg_version;
        match pkg::name_parts(file_name) {
            Some((n, v)) => {
                pkg_name = n;
                pkg_version = v;
            }
            None => {
                anyhow::bail!("package name is invalid");
            }
        }

        // open the file
        let mut file = File::open(&file_path).context("failed to open cached package file")?;
        let metadata = package::get_metadata(&mut file)?;

        // package filename must match package metadata
        // name must match
        if metadata.name != pkg_name {
            anyhow::bail!("failed to installed {}, package file is corrupt. Package name does not match package metadata.", pkg_name);
        }
        // version must match
        if metadata.version != pkg_version.to_string() {
            anyhow::bail!("failed to installed {}, package file is corrupt. Package version does not match package metadata.", pkg_name);
        }

        // make sure the checksum matches
        if !pkg::check_datachecksum(&metadata, &mut file)? {
            anyhow::bail!("failed to installed {}, package file is corrupt. Package checksum mismatch", pkg_name);
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
        let mut tar = tar::Archive::new(&mut file);
        //let mut data_tar = pkg::seek_to_tar_entry("data.tar.zst", &mut tar)?;
        let mut data_tar = package::seek_to_tar_entry("data.tar.zst", &mut tar)?;
        let mut zstd = zstd::stream::read::Decoder::new(&mut data_tar)?;
        let mut data_tar = tar::Archive::new(&mut zstd);

        // unpack all files
        //data_tar.unpack(install_dir)?;

        //let mut file_list = Vec::new();

        //TODO check that the tar only unpacks files listed in metadata.files

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

        //let mut details = db::DbPkg::new(PackageID {
        //    name: String::from(pkg_name),
        //    version: pkg_version.to_string(),
        //});

        //for (k, v) in files.into_iter() {
        //    details.files.push((k, v));
        //}

        let mut details = db::DbPkg::new(metadata);
        details.location = install_dir.into();
        details.versioning = versioning;

        self.db.add_package(details);
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
    pub fn install_cmd(&mut self, pkg_name: &String) -> AResult<()> {
        self.create_load_db()?;

        // make sure we have a cache dir to save the package in
        create_dir(&self.config.cache_dir)?;

        // installing from a package file
        {
            let path = Path::new(pkg_name);
            let filename = path.file_name().map(|s| s.to_string_lossy().to_string());
            if let Ok(true) = path.try_exists() && let Some(filename) = filename && package::is_packagefile_name(&filename) {

                tracing::trace!("installing from package file {pkg_name}");

                // TODO check that the package file is not a path to a file in the cache dir
                // (DO NOT COPY IN PLACE)

                let cache_path = join_path!(&self.config.cache_dir, filename);

                // copy to the cache dir
                std::fs::copy(path, &cache_path).context("failed to copy package file")?;

                //TODO sanity check file contents

                return self.install_pkg_file(cache_path, Versioning::default());
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

        let (listing, versioning) = self.find_package_version(pkg_name, split.next())?;
        let version = Version::new(&listing.version);

        //dbg!(&listing);
        //dbg!(&versioning);
        //dbg!(&version);

        tracing::debug!(pkg_name, version=version.as_str(), "installing from provider");

        let id = PackageID {
            name: pkg_name.to_string(),
            version: version.to_string(),
        };

        let cached_file = if let Some(cached_file) = self.get_cached_package_path(&id) {
            cached_file
        } else {
            let cached_file = self.cache_fetch_cmd(&id)?;
            cached_file
        };

        tracing::debug!("installing {pkg_name} {version}");
        return self.install_pkg_file(cached_file, versioning);
    }

    /// verify package state
    pub fn verify_cmd<S>(&mut self, pkgs: &Vec<S>) -> AResult<()>
        where S: AsRef<str>,
    {
        // for installed packages listed in the db,
        // walk each file and hash the version we have on disk
        // and compare to the hash stored in the db
        self.load_db()?;

        // all given names must be installed
        for name in pkgs {
            let find = self.db.installed.iter().find(|&ent| ent.metadata.name == name.as_ref());
            if find.is_none() {
                anyhow::bail!("package named '{}' is not installed", name.as_ref());
            }
        }

        let filtering = !pkgs.is_empty();
        let iter = self.db.installed.iter().filter(|&ent| {
            if filtering {
                pkgs.iter().any(|name| name.as_ref() == ent.metadata.name)
            } else {
                true
            }
        });

        for pkg in iter {
            println!("verifying package {}", pkg.metadata.name);

            let root_dir = pkg.location.as_ref().expect("package has no installation location").clone();
            for (filepath, fileinfo) in pkg.metadata.files.iter() {

                if !fileinfo.filetype.is_file() {
                    println!("skipping non-file {}", filepath);
                    continue;
                }

                let db_hash = fileinfo.hash.as_ref().expect("installed file has no hash");

                let path = join_path!(&root_dir, filepath);

                println!("checking file at {path:?} for hash {db_hash}");

                if !path.exists() {
                    println!("bad -- {:?} does not exist", &path);
                    continue;
                }

                let file = File::open(&path)?;
                let reader = std::io::BufReader::new(file);
                let hash = pkg::blake2_hash_reader(reader)?;

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
    pub fn uninstall_cmd(&mut self, pkg_name: &String) -> AResult<()> {
        // from the package name,
        // find all files that belong to this package from the db

        self.load_db()?;

        let found = self.db.installed.iter().find(|e| &e.metadata.name == pkg_name);

        let pkg = match found {
            None => {
                println!("package '{pkg_name}' not installed");
                std::process::exit(0);
                //anyhow::bail!("package '{pkg_name}' not installed"),
            }
            Some(pkg) => pkg,
        };

        self.delete_package_files(pkg)?;
        self.db.remove_package(pkg.metadata.id());
        self.save_db()?;
        Ok(())
    }

    fn delete_package_files(&self, pkg: &db::DbPkg) -> AResult<()> {

        //dbg!(pkg);

        // TODO actually delete the files

        let mut dirs = Vec::new();

        for (filepath, fileinfo) in &pkg.metadata.files {
            //println!("removing {filepath}");
            //dbg!(fileinfo);
            match &fileinfo.filetype {
                package::FileType::Link(to) => {
                    let path = get_pkg_file_path(pkg, filepath)?;
                    println!("removing {filepath} -> {to}");
                    println!("         {}", path.display());
                    std::fs::remove_file(path)?;
                }
                package::FileType::File => {
                    let path = get_pkg_file_path(pkg, filepath)?;
                    println!("removing {filepath}");
                    println!("         {}", path.display());
                    std::fs::remove_file(path)?;
                }
                package::FileType::Dir => {
                    let path = get_pkg_file_path(pkg, filepath)?;
                    dirs.push(path);
                }
            }
        }

        dirs.reverse();
        for path in dirs {
            println!("removing dir {}", path.display());
            std::fs::remove_dir(path)?;
        }

        Ok(())
    }

    pub fn scan_cmd(&self) -> AResult<()> {

        tracing::trace!("scanning providers");

        // make sure there is a cache/provider dir to cache search results in
        create_dir(join_path!(&self.config.cache_dir, "provider"))?;

        for provider in &self.config.providers {
            let list = provider.as_provide().scan();

            if let Ok(list) = list {
                // write the package list to a cache file

                let s = serde_json::to_string(&list)?;
                //let s = serde_json::to_string_pretty(&list)?;
                let mut file = File::create(&provider.cache_file)?;
                file.write_all(s.as_bytes())?;
            }
        }
        Ok(())
    }
}

fn get_pkg_file_path(pkg: &db::DbPkg, path: &str) -> AResult<PathBuf> {
    let mut fullpath = PathBuf::from(pkg.location.as_ref().context("package has no location")?);
    fullpath.push(path);
    //Ok(fullpath.canonicalize()?) // can't canonicalize because that resolves symlinks
    Ok(fullpath)
}
