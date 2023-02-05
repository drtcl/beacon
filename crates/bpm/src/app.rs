use crate::*;

pub struct App {
    pub config: config::Config,
    pub db: db::Db,
}

impl App {
    pub fn load_db(&mut self) -> AResult<()> {
        println!("loading db");
        let mut db_file =
            std::fs::File::open(&self.config.db_file).context("cannot read database file")?;
        self.db = db::Db::from_reader(&mut db_file).context("failed to load database")?;
        Ok(())
    }

    pub fn create_load_db(&mut self) -> AResult<()> {
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

    pub fn list_cmd(&mut self) -> AResult<()> {
        self.load_db()?;

        let mut tw = tabwriter::TabWriter::new(std::io::stdout());
        //write!(&mut tw, "{}\t{}\n", "name", "version")?;
        for ent in self.db.installed.iter() {
            //writeln!(&mut tw, "{}\t{}", ent.id.name, ent.id.version)?;
            writeln!(&mut tw, "{}\t{}", ent.metadata.id.name, ent.metadata.id.version)?;
        }
        tw.flush()?;
        Ok(())
    }

    pub fn search_cmd(&self, pkg_name: &str) -> AResult<()> {
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

    pub fn search_results(&self, pkg_name: &str, fuzzy: bool) -> AResult<search::SearchResults> {
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

    pub fn find_latest_version(&self, pkg_name: &str) -> AResult<Version> {
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

    pub fn cache_fetch_cmd(&self, pkg_name: &str, version: &Version) -> AResult<PathBuf> {
        // hmm, install first found from first provider?
        // or search all providers and install max version?
        // mirrors?

        todo!();

//        let pkg_id = package::PackageID {
//            name: String::from(pkg_name),
//            version: version.clone(),
//        };
//
//        let filepath = package::to_filepath(PathBuf::from(&self.config.cache_dir), pkg_name, version);
//
//        let mut file = std::fs::File::create(&filepath)?;
//
//        for provider in &self.config.providers {
//            if provider.1.as_provide().fetch(&pkg_id, &mut file).is_ok() {
//                break;
//            }
//        }
//
//        Ok(filepath)
    }

    fn get_mountpoint_dir(&self, metadata: &package::MetaData) -> AResult<PathBuf> {
        //let mount_point = pkg::get_mountpoint(&mut pkg_file)?;
        let mount_point = &metadata.mount;
        let mount_point = match mount_point {
            Some(loc) => String::from(loc),
            None => {
                if !self.config.use_default_target {
                    anyhow::bail!("attempt to use default target, which is disabled");
                }
                "TARGET".to_string()
            },
        };
        let mount_point = self.config.get_mountpoint(&mount_point).context(format!("invalid mount point {mount_point}"))?;
        Ok(mount_point)
    }

    pub fn install_pkg_file(&mut self, file_path: PathBuf) -> AResult<()> {

        println!("install_pkg_file {}", file_path.display());

        let file_name = file_path
            .file_name()
            .expect("invalid filename")
            .to_str()
            .expect("invalid filename");
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
        let mut file = std::fs::File::open(&file_path).context("failed to open cached package file")?;
        let metadata = package::get_metadata(&mut file)?;

        if metadata.id.name != pkg_name {
            todo!();
        }
        if metadata.id.version != pkg_version.to_string() {
            todo!();
        }

        let install_dir = self.get_mountpoint_dir(&metadata)?;

        // get the list of files and their hashes from the meta file
        //let files = package::get_filelist(&mut file)?;
        //let files = &metadata.files;

        // make sure the checksum matches

        let cksum_match = pkg::check_datachecksum(&metadata, &mut file)?;
        if !cksum_match {
            anyhow::bail!("Corrupted package, checksum mismatch");
        }

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

        self.db.add_package(details);

        let mut db_file = std::fs::File::create(&self.config.db_file)?;
        //self.db.write_to(&mut std::io::BufWriter::new(&mut db_file))?;
        self.db.write_to(&mut db_file)?;
        db_file.sync_all()?;
        drop(db_file);

        let db = db::Db::from_file(&self.config.db_file)?;
        dbg!(&db);

        Ok(())
    }

    /// installing a package
    pub fn install_cmd(&mut self, pkg_name: &String) -> AResult<()> {
        self.create_load_db()?;

        // make sure we have a cache dir to save the package in
        create_dir(&self.config.cache_dir)?;

        // installing from a package file
        if pkg_name.ends_with(PACKAGE_EXT) && let Ok(true) = std::path::Path::new(pkg_name).try_exists() {

            println!("installing from package file {pkg_name}");

            // TODO check that the package file is not a path to a file in the cache dir
            // (DO NOT COPY IN PLACE)

            //dbg!(&pkg_name);
            //dbg!(&self.config.cache_dir);

            let pkg_filepath = Path::new(pkg_name);
            let pkg_filename = pkg_filepath
                .file_name()
                .expect("invalid filepath")
                .to_str()
                .expect("invalid filepath");

            if !pkg::named_properly(pkg_filename) {
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
    pub fn verify_cmd<S>(&mut self, pkgs: &Vec<S>) -> AResult<()>
        where S: AsRef<str>,
    {
        // for installed packages listed in the db,
        // walk each file and hash the version we have on disk
        // and compare to the hash stored in the db
        self.load_db()?;

        // all given names must be installed
        for name in pkgs {
            let find = self.db.installed.iter().find(|&ent| ent.metadata.id.name == name.as_ref());
            if find.is_none() {
                anyhow::bail!("package named '{}' is not installed", name.as_ref());
            }
        }

        let filtering = !pkgs.is_empty();
        let iter = self.db.installed.iter().filter(|&ent| {
            if filtering {
                pkgs.iter().any(|name| name.as_ref() == ent.metadata.id.name)
            } else {
                true
            }
        });

        for pkg in iter {
            println!("verifying package {}", pkg.metadata.id.name);

            let root_dir = pkg.location.as_ref().expect("package has no installation location").clone();
            for (filepath, fileinfo) in pkg.metadata.files.iter() {

                if !fileinfo.filetype.is_file() {
                    println!("skipping non-file {}", filepath);
                    continue;
                }

                let mut path = root_dir.clone();
                let db_hash = fileinfo.hash.as_ref().expect("installed file has no hash");
                path.push(filepath);
                println!("checking file at {path:?} for hash {db_hash}");

                if !path.exists() {
                    println!("bad -- {:?} does not exist", &path);
                    continue;
                }

                let file = std::fs::File::open(&path)?;
                let reader = std::io::BufReader::new(file);
                let hash = pkg::blake2_hash_reader(reader)?;

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
    pub fn uninstall_cmd(&mut self, pkg_name: &String) -> AResult<()> {
        // from the package name,
        // find all files that belong to this package from the db

        self.load_db()?;

        let found = self.db.installed.iter().find(|e| &e.metadata.id.name == pkg_name);

        let pkg = match found {
            None => anyhow::bail!("package '{pkg_name}' not installed"),
            Some(pkg) => pkg,
        };

        //dbg!(pkg);

        for (filepath, fileinfo) in &pkg.metadata.files {
            println!("removing {filepath}");
            // TODO actually delete the files
        }

        Ok(())
    }
}

