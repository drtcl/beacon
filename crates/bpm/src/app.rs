#![allow(clippy::collapsible_else_if)]

use bpmutil::*;
use chrono::SubsecRound;
use crate::*;
use package::PackageID;
use rand::distributions::{Alphanumeric, DistString};
use serde::{Serialize, Deserialize};
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::HashSet;
use std::fs::File;
use std::io::BufWriter;
use std::io::IsTerminal;
use std::io::Seek;
use std::sync::Mutex;
use std::time::Duration;

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

mod list;

const TEMP_DOWNLOAD_PREFX : &str = "temp_download_";

#[derive(Debug)]
pub struct App {
    config: config::Config,
    db: db::Db,
    db_loaded: bool,

    pub provider_filter: provider::ProviderFilter,
    arch_filter: Vec<String>,

    lockfile: Option<File>,
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

#[derive(Debug)]
enum PkgArgType {
    Filepath{
        path: Utf8PathBuf,
        name: String,
        version: Version,
        arch: Option<String>,
    },
    Unversioned(String),
    Versioned(String, String),
}

//impl PkgArgType {
//    fn path(&self) -> Option<&Utf8PathBuf> {
//        match self {
//            Self::Filepath { path, .. } => Some(path),
//            _ => None
//        }
//    }
//    fn name(&self) -> Option<&String> {
//        match self {
//            Self::Filepath { name, .. } |
//            Self::Unversioned(name) |
//            Self::Versioned(name, ..) => Some(name),
//        }
//    }
//    fn version(&self) -> Option<&str> {
//        match self {
//            Self::Filepath { version, .. } => Some(version.as_str()),
//            Self::Versioned(_, version) => Some(version.as_str()),
//            Self::Unversioned(_) => None,
//        }
//    }
//}

/// check if a given user arg is a path to a pacakge file on the fs
// return (filepath, package_name, version, arch)
fn is_package_file_arg(arg: &str) -> Option<(Utf8PathBuf, &str, Version, Option<&str>)> {
    let path = Utf8Path::new(arg);
    if let Ok(true) = path.try_exists() {
        tracing::trace!(path=?path, "testing if file path looks like a package");
        let filename = path.file_name();
        if let Some(filename) = filename && package::is_packagefile_name(filename) {
            if let Some((name, ver, arch)) = package::split_parts(filename) {
                return Some((path.into(), name, Version::new(ver), arch));
            }
        }
    }
    None
}

fn parse_pkg_arg(arg: &str) -> AResult<PkgArgType> {

    if let Some((path, name, version, arch)) = is_package_file_arg(arg) {
        return Ok(PkgArgType::Filepath{
            path,
            name: name.into(),
            version,
            arch: arch.map(String::from),
        });
    }

    let mut split = None;
    if arg.contains("@") {
        split = Some(arg.split('@'));
    } else if arg.contains('=') {
        split = Some(arg.split('='));
    }
    if let Some(split) = &mut split {

        let pkg_name = match split.next() {
            Some("") | None => {
                anyhow::bail!("empty package name");
            }
            Some(name) => name
        };
        let version = match split.next() {
            Some("") | None => {
                anyhow::bail!("empty verson");
            }
            Some(name) => name
        };

        return Ok(PkgArgType::Versioned(pkg_name.into(), version.into()));

    } else {
        return Ok(PkgArgType::Unversioned(arg.into()));
    }
}

impl App {

    pub fn new(config: config::Config) -> Self {

        Self {
            config,
            db: db::Db::new(),
            db_loaded: false,
            provider_filter: provider::ProviderFilter::empty(),
            arch_filter: vec!["".into()],
            lockfile: None,
        }
    }

    pub fn exclusive_lock(&mut self) -> AResult<()> {

        if self.lockfile.is_none() {
            if let Some(path) = &self.config.lockfile {
                let file = bpmutil::open_lockfile(path)?;
                if !file.try_lock().context("file lock")? {
                    std::thread::sleep(std::time::Duration::from_secs(1));
                    if !file.try_lock().context("file lock")? {
                        eprintln!("waiting for file lock");
                        file.lock().context("file lock")?;
                    }
                }
                self.lockfile = Some(file);
            }
        }

        Ok(())
    }

    pub fn shared_lock(&mut self) -> AResult<()> {

        if self.lockfile.is_none() {
            if let Some(path) = &self.config.lockfile {
                let file = bpmutil::open_lockfile(path)?;
                if !file.try_lock_shared().context("file lock")? {
                    std::thread::sleep(std::time::Duration::from_secs(1));
                    if !file.try_lock_shared().context("file lock")? {
                        eprintln!("waiting for file lock");
                        file.lock_shared().context("file lock")?;
                    }
                }
                self.lockfile = Some(file);
            }
        }

        Ok(())
    }

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

        if let Some(parent) = db_path.parent() {
            if !parent.exists() {
                if std::fs::create_dir_all(parent).is_ok() {
                    tracing::debug!("created db parent dir {}", parent.display());
                } else {
                    tracing::warn!("could not create missing db parent dir {}", parent.display());
                }
            }
        }

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
    pub fn search_cmd(&mut self, pkg_name: &str, exact: bool) -> AResult<()> {

        self.shared_lock()?;

        let results = self.search_results(pkg_name, exact)?;

        let mut tw = tabwriter::TabWriter::new(std::io::stdout());
        let mut sep = ' ';
        if std::io::stdout().is_terminal() && !results.packages.is_empty() {
            writeln!(&mut tw, "name\tversion\tarch")?;
            sep = '\t';
        }

        for (name, pkg_info) in results.packages.iter() {
            let (version, vlist) = pkg_info.versions.last_key_value().unwrap();

            // make a list of the archs that this version is for. Move "noarch" to the front
            let mut archs : Vec<&str> = Vec::new();
            archs.extend(vlist.iter().map(|e| e.arch.as_deref().unwrap_or("noarch")));
            archs.sort();
            archs.dedup();
            if let Some(idx) = archs.iter().position(|x| *x == "noarch") {
                let e = archs.remove(idx);
                archs.insert(0, e);
            }
            let arch = archs.join(",");
            writeln!(&mut tw, "{}{}{}{}{}", name, sep, version, sep, arch)?;
        }
        tw.flush()?;
        Ok(())
    }

    /// iterate through providers with an applied filter
    fn filtered_providers(&self) -> impl Iterator<Item=&provider::Provider> {
        self.provider_filter.filter(&self.config.providers)
    }

    // note: arch_filter is expected to never be empty
    pub fn setup_arch_filter(&mut self, args: Option<Vec<&String>>) {
        if let Some(filters) = args {
            self.arch_filter = filters.iter().map(|s| s.to_string()).collect();
        } else if let Some(filters) = &self.config.arch_filter {
            self.arch_filter = filters.iter().map(|s| s.to_string()).collect();
        }
    }

    // -------------

    /// search and merge each provider's cached package info
    fn search_results(&self, needle: &str, exact: bool) -> AResult<scan_result::ScanResult> {

        let needle_lower = needle.to_lowercase();
        let mut merged_results = scan_result::ScanResult::default();

        for provider in self.filtered_providers() {

            // load the provider's cache file and merge it's results
            if let Ok(data) = provider.load_file() {

                let mut cached_results = data.packages;

                cached_results.packages.retain(|name, _versions| {
                    if exact {
                        name == needle
                    } else {
                        name.contains(needle) || name.to_lowercase().contains(&needle_lower)
                    }
                });
                merged_results.merge(cached_results);
            } else {
                tracing::trace!(file=?provider.cache_file, "couldn't read cache file for provider '{}'", provider.name);
            }
        }

        // filter by arch
        if !self.arch_filter.is_empty() {
            let arch_filter = self.arch_filter.iter().map(|s| s.as_str()).collect::<Vec<&str>>();
            merged_results.filter_arch(arch_filter.as_slice());
        }

        Ok(merged_results)
    }

    fn get_mountpoint_dir(&self, metadata: &package::MetaData, user_target: Option<&String>) -> AResult<config::PathType> {

        let mount_point = if let Some(target) = user_target {
            self.config.get_mountpoint_user(target)
        } else {
            self.config.get_mountpoint(metadata.mount.as_deref())
        };

        use config::MountPoint;
        match mount_point {
            MountPoint::Specified(mp) |
            MountPoint::User(mp) |
            MountPoint::Default(mp) => Ok(mp),
            MountPoint::DefaultDisabled => anyhow::bail!("attempt to use default target, which is disabled"),
            MountPoint::Invalid{name} => anyhow::bail!("package using an invalid mountpoint: {name}"),
        }
    }

    /// install a package from a local package file.
    fn install_pkg_file(&mut self, file_path: Utf8PathBuf, package_hash: Option<String>, versioning: Versioning, target: Option<&String>) -> AResult<()> {

        tracing::debug!("install_pkg_file {}", file_path);

        let package_file_filename = file_path.file_name().context("package file has no filename")?;
        let package_hash = package_hash.or_else(|| {
            self.db.cache_get_hash(package_file_filename)
        });

        let pkg_name;
        let pkg_version;
        let pkg_arch;
        match (package::is_packagefile_name(package_file_filename), package::split_parts(package_file_filename)) {
            (true, Some((n, v, a))) => {
                pkg_name = n;
                pkg_version = v;
                pkg_arch = a;
            }
            _ => {
                anyhow::bail!("package name is invalid");
            }
        }

        // open the package file and get the metadata
        let mut file = File::open(&file_path).context("failed to open package file")?;

        // verify packagefile integrity
        let check = package::package_integrity_check_full(&mut file, Some(package_file_filename), package_hash.as_deref());
        if check.is_err() || !check.unwrap().good() {
            anyhow::bail!("failed to install {}, package file failed integrity check", pkg_name);
        }

        let mut metadata = package::get_metadata(&mut file).context("error reading metadata")?;

        // package filename must match package metadata
        // name must match
        if metadata.name != pkg_name {
            anyhow::bail!("failed to installed {}, package file is corrupt. Package name from filename does not match package internal metadata.", pkg_name);
        }
        // version must match
        if metadata.version != pkg_version {
            anyhow::bail!("failed to installed {}, package file is corrupt. Package version from filename does not match package internal metadata.", pkg_name);
        }
        // arch must match
        if metadata.arch.as_deref() != pkg_arch {
            anyhow::bail!("failed to installed {}, package file is corrupt. Package architecture file filename does not match package internal metadata.", pkg_name);
        }

        let install_dir = self.get_mountpoint_dir(&metadata, target)?;
        let install_dir_full = install_dir.full_path()?;

        // create the mount point dir if it doesn't exist
        if !install_dir_full.exists() {
            tracing::trace!("creating install dir {install_dir_full}");
            std::fs::create_dir_all(&install_dir_full)?;
        }

        // get the list of files and their hashes from the meta file
        //let files = package::get_filelist(&mut file)?;
        //let files = &metadata.files;

        // obtain reader for embeded data archive
        file.rewind()?;
        let mut outer_tar = tar::Archive::new(&mut file);
        let (mut inner_tar, _size) = package::seek_to_tar_entry("data.tar.zst", &mut outer_tar)?;
        let mut zstd = zstd::stream::read::Decoder::new(&mut inner_tar)?;
        let mut data_tar = tar::Archive::new(&mut zstd);

        let bar = bpmutil::status::global().add_task(Some("install"), Some(metadata.name.as_str()), Some(metadata.files.len() as u64));
        bar.set_style(indicatif::ProgressStyle::with_template(
            #[allow(clippy::literal_string_with_formatting_args)]
            "   {msg}\n {spinner:.green} installing {prefix} {wide_bar:.green} {pos}/{len} "
        ).unwrap());
        bar.set_prefix(metadata.name.to_string());

        let mut details = db::DbPkg::new(metadata.clone());
        details.location = Some(install_dir);
        details.versioning = versioning;
        details.package_file_filename = Some(package_file_filename.to_string());

        self.db.add_package(details);

        self.db.cache_touch(package_file_filename, None);
        self.db.cache_unuse_all_versions(pkg_name);
        self.db.cache_set_in_use(package_file_filename, true);

        self.save_db()?;

        #[cfg(unix)]
        let mut ro_dirs = HashMap::new();

        // unpack all files individually
        for entry in data_tar.entries()? {

            let mut entry = entry?;

            let path = Utf8PathBuf::from_path_buf(entry.path().unwrap().into_owned()).unwrap();
            //bar.suspend(|| println!("{}", path));

            let installed_path = join_path_utf8!(&install_dir_full, &path);

            // check if the parent dir is readonly,
            // need to add write permission to be able to install files into the dir
            #[cfg(unix)]
            if let Some(parent_dir) = installed_path.parent() {
                if !ro_dirs.contains_key(parent_dir) {
                    ro_dirs.insert(parent_dir.to_path_buf(), None);
                    if let Ok(md) = std::fs::metadata(parent_dir) {

                        let mut perms = md.permissions();
                        let mut mode = perms.mode();

                        if 0 == (mode & 0o200) {
                            // user does not have write access
                            ro_dirs.insert(parent_dir.to_path_buf(), Some(mode));

                            mode |= 0o200;
                            perms.set_mode(mode);
                            let _ = std::fs::set_permissions(parent_dir, perms);
                        }
                    }
                }
            }

            let installed_ok = entry.unpack_in(&install_dir_full)?;

            bar.set_message(String::from(path.as_str()));

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

            bar.inc(1);
        }

        // restore the permissions on any readonly dir that was modified during installation
        #[cfg(unix)]
        for (dir, mode) in ro_dirs {
            if let Some(mode) = mode {
                let md = std::fs::metadata(&dir)?;
                let mut perms = md.permissions();
                perms.set_mode(mode);
                let _ = std::fs::set_permissions(&dir, perms);
            }
        }

        bar.finish_and_clear();

        println!("Installation complete");

        Ok(())
    }

    /// search our local cached files for pkg@version, if not found there, search providers
    fn find_package_version(&self, pkg_name: &str, which: Option<&str>, required_arch: Option<&str>, preferred_arch: Option<&str>) -> AResult<(search::SingleListing, Versioning)> {

        tracing::trace!(name=pkg_name, version=which, req_arch=?required_arch, pref_arch=?preferred_arch, "find_package_version");

        // note: (expect) self.arch_filter is expected to never be empty
        //
        // this builds an iterator over [required_arch, preferred_arch, *arch_filter - preferred_arch]
        // and take()s only the first one if required_arch.is_some(), otherwise everything.
        let pref_avail = preferred_arch.is_some() && self.arch_filter.iter().any(|f| package::ArchMatcher::from(f).matches(preferred_arch.unwrap()));
        let arch_filter_iter = required_arch.into_iter()
            .chain(preferred_arch.into_iter().filter(|_| pref_avail))
            .chain(
                self.arch_filter.iter().filter(|f| {
                    tern!(pref_avail, *f != preferred_arch.unwrap(), true)
                })
                .map(|f| f.as_str())
            )
            .take(tern!(required_arch.is_some(), 1, self.arch_filter.len()));

        // first search our locally cached files
        if let Some(version) = which {

            for arch in arch_filter_iter.clone() {
                let path = self.cache_package_lookup(&PackageID {
                    name: pkg_name.to_string(),
                    version: version.to_string(),
                    arch: Some(arch.to_string()),
                });
                if let Some(path) = path {
                    let filename = path.file_name().context("cache file has no filename")?.to_string();
                    return Ok((
                        search::SingleListing {
                            pkg_name: std::rc::Rc::<str>::from(pkg_name),
                            version: version.into(),
                            filename,
                            url: "".into(),
                            channels: vec![],
                            arch: Some(arch.to_string()),
                        },
                        Versioning::pinned_version()
                    ));
                }
            }
        }

        // get search results for just this package name
        let mut results = self.search_results(pkg_name, true)?;

        if results.package_count() != 1 {
            anyhow::bail!("could not find package '{pkg_name}'");
        }

        let mut versioning = Versioning::default();

        // `which` could be a channel or a specific version, check
        if let Some(v) = which {

            let pinfo = results.packages.iter_mut().next().context("missing expected package info")?.1;
            if pinfo.has_channel(v) {
                results.filter_channel(&[v]);
                versioning.pinned_to_channel = true;
                versioning.channel = Some(v.to_string());
            } else if pinfo.has_version(v) {
                pinfo.versions.retain(|version, _vlist| version.as_str() == v);
                versioning.pinned_to_version = true;
            } else {
                anyhow::bail!("could not find version or channel '{v}' for package '{pkg_name}'");
            }
        }

        // grab the first version entry matching the arch filter ordering
        let mut pinfo = results.packages.into_iter().next().context("missing expected package info")?.1;
        if let Some((version, vlist)) = pinfo.versions.pop_last() {

            for arch in arch_filter_iter {
                if let Some(vinfo) = vlist.iter().find(|ent| package::ArchMatcher::from(arch).matches(&ent.arch)) {
                    return Ok((search::SingleListing {
                        pkg_name: std::rc::Rc::<str>::from(pkg_name),
                        version: version.into(),
                        filename: vinfo.filename.clone(),
                        url: vinfo.uri.clone(),
                        channels: vinfo.channels.clone(),
                        arch: vinfo.arch.clone(),
                    }, versioning));
                }
            }
        }

        anyhow::bail!("no versions available for package '{pkg_name}'");
    }

    /// `bpm install`
    ///
    /// `bpm install foo`
    /// or `bpm install foo@1.2.3`
    /// or `bpm install path/to/foo_1.2.3.bpm`
    /// install a package from a provider or directly from a file
    pub fn install_cmd(&mut self, pkg_name_or_filepath: &str, no_pin: bool, update: bool, reinstall: bool, target: Option<&String>) -> AResult<()> {

        self.exclusive_lock()?;

        let _trace_span = tracing::trace_span!("bpm install").entered();

        let mut pkg_file_path : Option<Utf8PathBuf> = None;
        let pkg_name : String;
        let mut pkg_version : Option<String> = None;
        let mut pkg_arch : Option<Option<String>> = None;
        let mut from_file = false;

        match parse_pkg_arg(pkg_name_or_filepath)? {
            PkgArgType::Filepath { path, name, version, arch } => {
                tracing::debug!("[install] directly from file {path}");
                pkg_file_path = Some(path);
                pkg_name = name;
                pkg_version = Some(version.to_string());
                pkg_arch = Some(arch);
                from_file = true;
            }
            PkgArgType::Unversioned(name) => {
                tracing::debug!(name, "[install] from package name, no version.");
                pkg_name = name;
            }
            PkgArgType::Versioned(name, version) => {
                tracing::debug!(name, version, "[install] from package name and version.");
                pkg_name = name;
                pkg_version = Some(version);
            }
        }

        self.create_load_db()?;

        // check if this package is already installed
        // gather info about the current installation
        let current_install = self.db.installed.iter().find(|p| p.metadata.name.as_str() == pkg_name);
        let current_install = current_install.cloned();
        let current_install = current_install.as_ref();
        let already_installed = current_install.is_some();

        let current_version : Option<&str> = current_install.map(|v| v.metadata.version.as_str());
        let current_arch    : Option<&str> = current_install.and_then(|v| v.metadata.arch.as_deref().or(Some("noarch")));

        // if that package is already installed AND --update/--reinstall was not given, then we're done
        if already_installed && !(update || reinstall) {
            println!("Package {} ({}) is already installed. Pass --update to install a different version.", pkg_name, current_version.unwrap_or(""));
            return Ok(());
        }

        let (pkg_version, pkg_arch, mut versioning) = if from_file {
            (pkg_version.unwrap(), pkg_arch.flatten(), Versioning::pinned_version())
        } else {
            // Find the package version to update to.
            // The user may have given a specific version or channel
            //   OR they may not have, in which case we're grabbing the greatest version.
            // Packages prefer to stay on the same arch that they currently have installed.
            let (listing, versioning) =
                self.find_package_version(
                    pkg_name.as_str(),
                    pkg_version.as_deref(), // None OR specific version or channel given by user
                    None,                   // no specific arch required,
                    current_arch            // the currently installed arch IS preferred
                )?;

            let pkg_version = listing.version.to_string();
            let pkg_arch = listing.arch;
            (pkg_version, pkg_arch, versioning)
        };

        tracing::debug!(
            name=pkg_name,
            version=pkg_version,
            channel=?versioning.channel,
            arch=pkg_arch,
            pin =% (if versioning.pinned_to_version { "version" } else if versioning.pinned_to_channel { "channel" } else { "None" }),
            "[install]"
        );

        if no_pin {
            versioning = Versioning::unpinned();
        }

        let version_same = already_installed && current_version == Some(pkg_version.as_str());
        let arch_same    = already_installed && package::ArchMatcher::from(current_arch).matches(&pkg_arch);
        let pinning_same = current_install.is_some_and(|info| info.versioning == versioning);

        if version_same && !reinstall {

            println!("No change. Package {} at version {} is already installed.", pkg_name, pkg_version);

            if !arch_same {
                println!("This looks like an attempt to change package architecture from '{}' to '{}'.",  current_arch.unwrap_or("noarch"), pkg_arch.as_deref().unwrap_or("noarch"));
                println!("Pass --reinstall to change package architecture.");
            }

            if !pinning_same && update {
                self.db.set_versioning(&pkg_name, versioning.clone());
                self.save_db()?;
                if versioning.pinned_to_channel {
                    println!("Updated pin to channel {}.", versioning.channel.as_deref().unwrap_or("?"));
                } else if versioning.pinned_to_version {
                    println!("Updated pin to version {}.", pkg_version);
                } else {
                    println!("Updated pinning, no longer pinned to a version or channel");
                }
            }
            return Ok(());
        }

        // continue to installation

        let id = PackageID {
            name: pkg_name.clone(),
            version: pkg_version.to_string(),
            arch: pkg_arch,
        };

        let (is_new, cache_file, package_hash) = if let Some(path) = pkg_file_path {
            self.cache_store_file(&path)?
        } else {
            //println!("Fetching {pkg_name} {pkg_version}");
            self.cache_package_require(&id)?
        };

        // insert the package file into cache
        if is_new {
            if let Some(filename) = cache_file.file_name() {
                self.db.cache_insert(filename, package_hash.clone(), None);
                self.save_db()?;
            }
        }

        if already_installed {
            if reinstall {
                println!("Installing (re-install) {pkg_name} {pkg_version}");
            } else {
                println!("Updating {} from version {} to {}", pkg_name, current_version.unwrap_or("?"), pkg_version);
            }
        } else {
            println!("Installing {pkg_name} {pkg_version}");
        }

        let ret = if already_installed {
            self.update_inplace(&pkg_name, cache_file, package_hash, versioning)
        } else {
            self.install_pkg_file(cache_file, package_hash, versioning, target)
        };

        if self.config.cache_auto_clean {
            tracing::trace!("[install] cache auto clean");
            let _ = self.cache_clean();
        }

        return ret;
    }

    /// `bpm uninstall` or `bpm remove`
    /// uninstall a package
    pub fn uninstall_cmd(&mut self, pkg_name: &String, verbose: bool, remove_unowned: bool) -> AResult<()> {

        self.exclusive_lock()?;

        // from the package name,
        // find all files that belong to this package from the db

        if !self.db_file_exists() {
            println!("package '{pkg_name}' not installed");
            std::process::exit(0);
        }

        self.load_db()?;

        let found = self.db.installed.iter().find(|e| &e.metadata.name == pkg_name);

        if found.is_none() {
            println!("package '{pkg_name}' not installed");
            std::process::exit(0);
        }

        let pkg = found.unwrap();
        let package_file_filename = pkg.package_file_filename.clone();

        println!("Uninstalling {} {}", pkg.metadata.name, pkg.metadata.version);

        self.delete_package_files(pkg, verbose, remove_unowned)?;

        self.db.remove_package(pkg.metadata.id());

        if let Some(filename) = package_file_filename {
            self.db.cache_set_in_use(&filename, false);

            if self.config.cache_touch_on_uninstall {
                self.db.cache_touch(&filename, None);
            }
        }

        self.save_db()?;

        println!("Uninstall complete");

        if self.config.cache_auto_clean {
            tracing::trace!("[uninstall] cache auto clean");
            let _ = self.cache_clean();
        }

        Ok(())
    }

    /// install a different version of a package in-place (on top of) the existing version
    fn update_inplace(&mut self, pkg_name: &str, new_pkg_file: Utf8PathBuf, package_hash: Option<String>, versioning: Versioning) -> AResult<()> {

        tracing::trace!("update_inplace {:?}", new_pkg_file);

        let package_file_filename = new_pkg_file.file_name().context("package has no filename")?;
        let package_hash = package_hash.or_else(|| {
            self.db.cache_get_hash(package_file_filename)
        });

        let mut new_package_fd = std::fs::File::open(&new_pkg_file)?;

        // verify packagefile integrity
        let check = package::package_integrity_check_full(&mut new_package_fd, Some(package_file_filename), package_hash.as_deref());
        if check.is_err() || !check.unwrap().good() {
            anyhow::bail!("failed to install {}, package file failed integrity check", pkg_name);
        } else {
            tracing::trace!("package integrity check pass");
        }

        let new_metadata = package::get_metadata(&mut new_package_fd).context("error reading metadata")?;

        let new_files = new_metadata.files.clone();

        // gather info about the old package (the currently installed version)
        let current_pkg_info = self.db.installed.iter().find(|e| e.metadata.name == pkg_name).context("package is not currently installed")?;
        let old_package_filename = current_pkg_info.package_file_filename.clone();
        let mut old_files = current_pkg_info.metadata.files.clone();

        let location = current_pkg_info.location.as_ref().context("installed package has no location")?.clone();
        let location_full = location.full_path()?;
        tracing::trace!("installing to the same location {:?}", location);

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
            let location_full = location_full.clone();
            let pkg_name = pkg_name.to_string();
            move || {

                let iter = remove_files.iter().map(|(path, info)| {
                    let path = join_path_utf8!(&location_full, &path);
                    (path, info)
                });

                let delete_bar = bpmutil::status::global().add_task(Some("delete"), Some(pkg_name), Some(remove_files.len() as u64));
                delete_bar.set_style(indicatif::ProgressStyle::with_template(
                    #[allow(clippy::literal_string_with_formatting_args)]
                    " {spinner:.green} remove   {wide_bar:.red} {pos}/{len} "
                ).unwrap());

                let iter = delete_bar.wrap_iter(iter).with_finish(indicatif::ProgressFinish::AndClear);

                let delete_ok = Self::delete_files(iter, false, true);
                if let Err(e) = delete_ok {
                    eprintln!("error deleting files {:?}", e);
                }
            }
        });

        let diff_bar = bpmutil::status::global().add_task(Some("diff"), Some(pkg_name.to_string()), Some(new_files.len() as u64));
        diff_bar.set_style(indicatif::ProgressStyle::with_template(
            #[allow(clippy::literal_string_with_formatting_args)]
            " {spinner:.green} diffing  {wide_bar:.blue} {pos}/{len} "
        ).unwrap());

        // files that have the same content and mtime
        let skip_files = std::sync::Mutex::new(HashSet::<Utf8PathBuf>::new());

        // spawn threads for hashing/diffing existing files
        std::thread::scope(|s| {

            let (send, recv) = crossbeam::channel::bounded::<(&Utf8PathBuf, &package::FileInfo)>(256);

            let diff_threads = std::thread::available_parallelism().map(|v| v.get()).unwrap_or(1).clamp(1, 12);

            for _ in 0..diff_threads {
                let location_full = &location_full;
                let skip_files = &skip_files;
                let recv = recv.clone();
                let diff_bar = &diff_bar;
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
                                if let Some(new_hash) = info.hash.as_ref() {
                                    if let Ok(mut file) = File::open(&fullpath) {
                                        if let Ok(hash) = blake3_hash_reader(&mut file) {
                                            if &hash == new_hash {

                                                // hashes match, file content is the same

                                                let mut skip = true;

                                                // check if the file needs its mtime adjusted
                                                if let Some(mtime) = info.mtime {
                                                    if let Some(cur_mtime) = file_state.mtime && cur_mtime != mtime {
                                                        // the file have a different mtime
                                                        //diff_bar.bar().suspend(|| {
                                                        //    println!("{} has different mtime", path);
                                                        //    println!("   old {}", cur_mtime);
                                                        //    println!("   new {}", mtime);
                                                        //});

                                                        // the file contents are the same, but they have different mtime.
                                                        // attempt to update the mtime here, if we get an error doing so, just unpack the file again
                                                        if let Some(mtime) = info.mtime {
                                                            let ftime = filetime::FileTime::from_unix_time(mtime as i64, 0);
                                                            if filetime::set_file_mtime(&fullpath, ftime).is_ok() {
                                                                //println!("{} mtime adjusted", path);
                                                            } else {
                                                                //println!("{} mtime set FAIL", path);
                                                                skip = false;
                                                            }
                                                        }
                                                    }
                                                }

                                                if skip {
                                                    // this file can be skipped during update
                                                    t_skip_files.insert(path.clone());
                                                }
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

        let update_bar = bpmutil::status::global().add_task(Some("update"), Some(pkg_name), Some((new_files.len() - skip_files.len()) as u64));
        update_bar.set_style(indicatif::ProgressStyle::with_template(
            #[allow(clippy::literal_string_with_formatting_args)]
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
        details.package_file_filename = Some(String::from(package_file_filename));

        self.db.add_package(details);
        self.db.cache_touch(package_file_filename, None);
        self.db.cache_unuse_all_versions(pkg_name);
        self.db.cache_set_in_use(package_file_filename, true);

        if self.config.cache_touch_on_uninstall {
            if let Some(package_file_filename) = old_package_filename {
                self.db.cache_touch(&package_file_filename, None);
            }
        }

        self.save_db()?;

        Ok(())
    }

    /// `bpm update`
    pub fn update_packages_cmd(&mut self, pkgs: &[&String]) -> AResult<()> {

        self.exclusive_lock()?;

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

            tracing::trace!("[update] {}: current version {}", pkg.metadata.name, pkg.metadata.version);

            // updates stay on a channel?
            let mut channel = None;
            if pkg.versioning.pinned_to_channel {
                if let Some(c) = &pkg.versioning.channel {
                    channel = Some(c.clone());
                }
            }

            let required_arch = &pkg.metadata.arch;
            let required_arch = required_arch.as_deref().or(Some("noarch"));

            let result = self.find_package_version(&pkg.metadata.name, channel.as_deref(), required_arch, None);

            if let Ok((listing, versioning)) = result {
                // if the version is the same as already installed, skip the update
                if listing.version == Version::from(pkg.metadata.version.as_str()) {
                    println!("{} already up-to-date", pkg.metadata.name);
                    continue;
                }
                let cached_file = self.cache_package_lookup(&PackageID{
                    name: pkg.metadata.name.clone(),
                    version: listing.version.to_string(),
                    arch: listing.arch.clone(),
                });

                updates.push((pkg.metadata.name.clone(), pkg.metadata.version.clone(), listing, versioning, cached_file));
            }
        }

        if updates.is_empty() {
            println!("No updates to apply");
            return Ok(());
        }

        println!("{} package{} to update:", updates.len(), tern!(updates.len() > 1, "s", ""));
        for (name, oldv, listing, _versioning, _cache_file) in &updates {
            println!("  {}: {} -> {}", name, oldv, listing.version);
        }

        if updates.iter().any(|(_name, _version, _listing, _versioning, cache_file)| cache_file.is_none()) {

            println!("Fetching Packages");

            for (name, _oldv, listing, _versioning, cache_file) in &mut updates {
                if cache_file.is_none() {
                    let ret = self.cache_package_require(&PackageID{
                        name: name.to_string(),
                        version: listing.version.to_string(),
                        arch: listing.arch.clone(),
                    });

                    if let Ok((is_new, path, hash)) = ret {

                        // if the new package file is new, insert into the cache
                        if is_new {
                            if let Some(filename) = path.file_name() {
                                self.db.cache_insert(filename, hash, None);
                            }
                        }

                        *cache_file = Some(path);

                    } else {
                        println!("failed to fetch {} {}, skipping", name, listing.version);
                    }
                }
            }
        }

        // remove any updates that don't have an incoming package file
        updates.retain(|(_name, _version, _listing, _versioning, cache_file)| cache_file.is_some());

        let mut count = 0;
        for (name, oldv, listing, versioning, cache_file) in updates {
            if let Some(path) = cache_file {
                println!("Updating {} {} -> {}", name, oldv, listing.version);
                self.update_inplace(&name, path, None, versioning)?;
                count += 1;
            }
        }

        println!("Updates Complete, {} package{} updated", count, tern!(count > 1, "s", ""));

        //TODO can put confirmation prompt here

        if self.config.cache_auto_clean {
            tracing::trace!("[update] cache auto clean");
            let _ = self.cache_clean();
        }

        Ok(())
    }

    /// `bpm pin`
    /// Pin a package to a channel or the currently installed version
    pub fn pin(&mut self, pkg_name: &str, channel: Option<&str>) -> AResult<()> {

        self.exclusive_lock()?;

        if self.db_file_exists() {
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
        }

        anyhow::bail!("package not found");
    }

    /// `bpm unpin`
    /// Unpin a package
    pub fn unpin(&mut self, pkg_name: &str) -> AResult<()> {

        self.exclusive_lock()?;

        if self.db_file_exists() {
            self.load_db()?;
            for pkg in &mut self.db.installed {
                if pkg.metadata.name == pkg_name {
                    pkg.versioning = Versioning::unpinned();
                    self.save_db()?;
                    return Ok(());
                }
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
    pub fn verify_cmd<S>(&mut self, pkgs: &Vec<S>, restore: bool, restore_volatile: bool, fail_fast: bool, verbose: u8, allow_mtime: bool) -> AResult<()>
        where S: AsRef<str>,
    {
        self.exclusive_lock()?;

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

        let mut new_cache_files = Vec::new();

        let mut report = BTreeMap::new();

        for pkg in db_iter {

            let verify_bar = bpmutil::status::global().add_task(Some("verify"), Some(pkg.metadata.name.as_str()), Some(pkg.metadata.files.len() as u64));
            verify_bar.set_style(indicatif::ProgressStyle::with_template(
                &format!(" {{spinner:.green}} verify {} {{wide_bar:.green}} {{pos}}/{{len}} ", pkg.metadata.name)
            ).unwrap());

            verify_bar.bar().suspend(|| voutl!(1, verbose, "# {} {}", pkg.metadata.name, pkg.metadata.version));

            let mut pristine = true;

            let mut restore_files = HashSet::new();

            let root_dir = pkg.location.as_ref().expect("package has no installation location").clone();
            let root_dir_full = root_dir.full_path()?;

            for (filepath, fileinfo) in pkg.metadata.files.iter() {

                let path = join_path_utf8!(&root_dir_full, filepath);

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
                                //verify_bar.bar().suspend(|| {
                                //    println!("{path}");
                                //    println!("  db_mtime {:?}", db_mtime);
                                //    println!("  fs_mtime {:?}", fs_mtime);
                                //});
                                if fs_mtime.is_some() && db_mtime == fs_mtime {
                                    //verify_bar.bar().suspend(|| {
                                    //    vout!(verbose > 0, "  mtime same");
                                    //});
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
                        verify_bar.bar().suspend(|| voutl!(1, verbose, " D {}", &filepath));
                    } else {
                        verify_bar.bar().suspend(|| voutl!(1, verbose, " M {}", &filepath));
                    }
                    restore_files.insert(filepath.clone());
                    if !restore && fail_fast {
                        break;
                    }
                } else {
                    if !restore {
                        verify_bar.bar().suspend(|| voutl!(2, verbose, "   {}", &filepath));
                    }
                }

                verify_bar.inc(1);
            }

            verify_bar.finish_and_clear();
            drop(verify_bar);

            report.insert(pkg.metadata.name.clone(), tern!(pristine, "unmodified", "modified"));

            if restore && !restore_files.is_empty() {

                let restore_bar = bpmutil::status::global().add_task(Some("restore"), Some(pkg.metadata.name.as_str()), Some(restore_files.len() as u64));
                restore_bar.set_style(indicatif::ProgressStyle::with_template(
                    &format!(" {{spinner:.green}} restore {} {{wide_bar:.cyan}} {{pos}}/{{len}} ", pkg.metadata.name)
                ).unwrap());

                let (is_new, path, hash) = self.cache_package_require(&PackageID{
                    name: pkg.metadata.name.clone(),
                    version: pkg.metadata.version.clone(),
                    arch: pkg.metadata.arch.clone(),
                }).context("could not find package file")?;

                if let Some(filename) = path.file_name() {
                    new_cache_files.push((filename.to_owned(), hash, is_new));
                }

                let cache_file = std::fs::File::open(path).context("reading cached package file")?;

                // TODO potential to get a different package here
                // could check that the meta data and data hash are the same as the ones in the db (the installed version)

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
                        restore_bar.inc(1);
                        restore_bar.bar().suspend(|| {
                            voutl!(1, verbose, " R {}", xpath);
                        });
                    }

                    // when there are no more files to restore, stop iterating the package
                    if restore_files.is_empty() {
                        break;
                    }
                }

                restore_bar.finish_and_clear();
                report.insert(pkg.metadata.name.clone(), "restored");
            }
        }

        if !report.is_empty() {
            let stdout = std::io::stdout();
            if stdout.is_terminal() {
                let _ = serde_json::to_writer_pretty(&mut std::io::stdout(), &report);
            } else {
                let _ = serde_json::to_writer(&mut std::io::stdout(), &report);
            }
            println!();
        }

        for (filename, hash, is_new) in new_cache_files {
            if is_new {
                self.db.cache_insert(&filename, hash, None);
            } else {
                self.db.cache_touch(&filename, None);
            }
            self.db.cache_set_in_use(&filename, true);
        }
        self.save_db()?;

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

        #[cfg(unix)]
        let mut ro_dirs = HashMap::new();

        for (filepath, fileinfo) in files {

            let filepath = filepath.as_ref();
            let exists = matches!(filepath.try_exists(), Ok(true));

            // check if the parent dir is readonly,
            // need to add write permission to be able to remove files from the dir
            #[cfg(unix)]
            if exists && let Some(parent_dir) = filepath.parent() && !ro_dirs.contains_key(parent_dir) {
                ro_dirs.insert(parent_dir.to_path_buf(), None);
                if let Ok(md) = std::fs::metadata(parent_dir) {

                    let mut perms = md.permissions();
                    let mut mode = perms.mode();

                    if 0 == (mode & 0o200) {
                        // user does not have write access
                        ro_dirs.insert(parent_dir.to_path_buf(), Some(mode));

                        mode |= 0o200;
                        perms.set_mode(mode);
                        let _ = std::fs::set_permissions(parent_dir, perms);
                    }
                }
            }

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

                    #[cfg(windows)] {
                        // the dir must not be readonly
                        let md = std::fs::metadata(&filepath)?;
                        if md.permissions().readonly() {
                            let mut perms = md.permissions();
                            perms.set_readonly(false);
                            let _ = std::fs::set_permissions(filepath, perms);
                        }
                    }
                }
            }
        }

        #[cfg(unix)]
        ro_dirs.retain(|_dir, mode| mode.is_some());

        dirs.reverse();
        for dir in dirs {

            // don't need to restore dirs that are being removed
            #[cfg(unix)]
            ro_dirs.remove(&dir);

            vout!(verbose, "delete {dir}");
            tracing::trace!("delete  {dir}");

            let exists = matches!(dir.try_exists(), Ok(true));
            let e = if remove_unowned {
                std::fs::remove_dir_all(&dir)
            } else {
                let ret = std::fs::remove_dir(&dir);
                match ret {
                    Err(ref e) if e.kind() == std::io::ErrorKind::DirectoryNotEmpty => {
                        Ok(())
                    },
                    _ => ret,
                }
            };

            if let Err(e) = e && exists {
                eprintln!("error deleting {dir}: {e}");
            }
        }

        // restore the permissions on any readonly dir that were modified
        #[cfg(unix)]
        for (dir, mode) in ro_dirs {
            if let Some(mode) = mode {
                let md = std::fs::metadata(&dir)?;
                let mut perms = md.permissions();
                perms.set_mode(mode);
                let _ = std::fs::set_permissions(&dir, perms);
            }
        }

        Ok(())
    }

    fn delete_package_files(&self, pkg: &db::DbPkg, verbose: bool, remove_unowned: bool) -> AResult<()> {

        let location = pkg.location.as_ref().context("package has no install location")?;
        let location_full = location.full_path()?;

        let count = pkg.metadata.files.len();

        let iter = pkg.metadata.files.iter().map(|(path, info)| {
            let path = join_path_utf8!(&location_full, path);
            (path, info)
        });

        let delete_bar = bpmutil::status::global().add_task(Some("uninstall"), Some(pkg.metadata.name.as_str()), Some(count as u64));
        delete_bar.set_style(indicatif::ProgressStyle::with_template(
            #[allow(clippy::literal_string_with_formatting_args)]
            " {spinner:.green} remove   {wide_bar:.red} {pos}/{len} "
        ).unwrap());

        let iter = delete_bar.wrap_iter(iter);

        Self::delete_files(iter, verbose, remove_unowned)
    }

    /// `bpm query owner <file>`
    pub fn query_owner(&mut self, file: &str) -> AResult<()> {

        self.shared_lock()?;

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

        if !self.db_file_exists() {
            return Ok(());
        }

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

        self.shared_lock()?;

        let depth = depth.unwrap_or(0);

        let output = |root: &Utf8Path, path: &Utf8Path, info: &package::FileInfo| {
            if show_type {
                let c = match &info.filetype {
                    package::FileType::Dir => 'd',
                    package::FileType::File => 'f',
                    package::FileType::Link(_to) => 's',
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

        if !self.db_file_exists() {
            anyhow::bail!("package not installed")
        }

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

    /// `bpm query kv`
    pub fn query_kv(&mut self, pkg_names: Option<&[&str]>, keys: Option<&[&str]>) -> AResult<()> {

        self.shared_lock()?;

        let one_pkg = pkg_names.is_some() && pkg_names.unwrap().len() == 1;
        let one_key = keys.is_some() && keys.unwrap().len() == 1;
        let no_filter = pkg_names.is_none();

        let mut parsed = Vec::new();
        if let Some(pkg_names) = pkg_names {
            for pkg_name in pkg_names {
                let x = parse_pkg_arg(pkg_name)?;
                parsed.push((pkg_name.to_string(), x));
            }
        }

        let read_file = |path: &Utf8Path| -> AResult<_> {
            let mut file = File::open(path).context("failed to open package file")?;
            let (_ok, metadata) = package::package_integrity_check(&mut file)?;
            Ok(metadata.kv)
        };

        let prune = |kv: BTreeMap<String, String>| -> _ {
            let mut out = scan_result::Kv::new();
            for (k, v) in kv {
                if let Some(keys) = keys {
                    if keys.contains(&k.as_str()) {
                        out.insert(k, v);
                    }
                } else {
                    out.insert(k, v);
                }
            }
            out
        };

        let mut master = HashMap::<String, scan_result::Kv>::new();

        if no_filter {
            // querying all installed packages
            if self.db_file_exists() {
                self.load_db()?;
                for pkg in self.db.installed.iter() {
                    let kv = prune(pkg.metadata.kv.clone());
                    master.insert(pkg.metadata.name.clone(), kv);
                }
            }
        } else {
            for (original, x) in parsed {

                match x {
                    PkgArgType::Unversioned(name) => {
                        // check db
                        self.load_db()?;
                        if let Some(pkg) = self.db.installed.iter().find(|pkg| pkg.metadata.name == name) {
                            let kv = prune(pkg.metadata.kv.clone());
                            master.insert(original, kv);
                        }
                    }
                    PkgArgType::Versioned(name, version) => {
                        // check cache
                        let mut id = PackageID{name, version, arch: None};
                        let mut found = false;
                        for arch in self.arch_filter.iter() {
                            id.arch = Some(arch.to_string());
                            if let Some(path) = self.cache_package_lookup(&id) {
                                if let Ok(kv) = read_file(&path) {
                                    let kv = prune(kv);
                                    master.insert(original, kv);
                                    found = true;
                                    break;
                                }
                            }
                        }
                        if one_pkg && !found {
                            anyhow::bail!("package not cached, you may need to 'cache fetch' it first.");
                        }
                    }
                    PkgArgType::Filepath { path, .. } => {
                        // read package file
                        if let Ok(kv) = read_file(&path) {
                            let kv = prune(kv);
                            master.insert(original, kv);
                        }
                    }
                }
            }
        }

        if one_pkg && master.is_empty() {
            anyhow::bail!("package not found");
        }

        master.retain(|_pkg_name, kv| !kv.is_empty());
        if one_pkg && one_key && master.is_empty() {
            anyhow::bail!("key not found");
        }

        if one_pkg && let Some(item) = master.iter().next() {
            if one_key {
                println!("{}", item.1.iter().next().unwrap().1);
            } else {
                println!("{}", serde_json::to_string_pretty(&item.1)?);
            }
        } else {
            println!("{}", serde_json::to_string_pretty(&master)?);
        }

        Ok(())
    }

    /// `bpm query kv --from-provider`
    /// Read the KV from the provider cache file.
    pub fn query_kv_provider(&mut self, pkg_names: Option<&[&str]>, keys: Option<&[&str]>) -> AResult<()> {

        self.shared_lock()?;

        let one_pkg = pkg_names.is_some() && pkg_names.unwrap().len() == 1;
        let one_key = keys.is_some() && keys.unwrap().len() == 1;

        let mut merged_results = scan_result::ScanResult::default();

        for provider in self.filtered_providers() {

            // load the provider's cache file and merge it's results
            if let Ok(data) = provider.load_file() {

                let mut cached_results = data.packages;

                if let Some(filter_names) = pkg_names && !filter_names.is_empty() {
                    cached_results.packages.retain(|name, _info| {
                        filter_names.contains(&name.as_str())
                        //filter_names.iter().any(|fname| fname.eq_ignore_ascii_case(name))
                    });
                }

                merged_results.merge(cached_results);
            } else {
                tracing::trace!(file=?provider.cache_file, "couldn't read cache file for provider '{}'", provider.name);
            }
        }

        if merged_results.packages.is_empty() {
            anyhow::bail!("no packages found");
        }

        let mut master = HashMap::<String, scan_result::Kv>::new();

        for (name, info) in merged_results.packages.into_iter() {
            if let Some(mut kv) = info.kv {
                if let Some(keys) = keys && !keys.is_empty() {
                    kv.retain(|k, _v| {
                        keys.contains(&k.as_str())
                    });
                }

                if one_pkg {
                    if one_key {
                        if kv.is_empty() {
                            anyhow::bail!("key not found");
                        } else {
                            println!("{}", kv.iter().next().unwrap().1);
                        }
                    } else {
                        println!("{}", serde_json::to_string_pretty(&kv)?);
                    }
                    return Ok(());
                } else if !kv.is_empty() {
                    master.insert(name, kv);
                }
            } else if one_pkg {
                anyhow::bail!("No KV");
            }
        }

        println!("{}", serde_json::to_string_pretty(&master)?);
        Ok(())
    }

    /// `bpm scan`
    pub fn scan_cmd(&mut self, debounce: Option<Duration>) -> AResult<()> {

        // This would normally be an exclusive lock because this modifies files.
        // However, we're using a shared lock here and exclusive locks on the individual provider
        // files to support running multiple processes doing scans in parrallel
        self.shared_lock()?;

        tracing::trace!("[scan] checking debounce times");

        let now = chrono::Utc::now().round_subsecs(0);
        tracing::trace!("[scan] {:?} now", now);

        let mut skip_scan = true;

        let debounce = debounce.unwrap_or(self.config.scan_debounce);
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
                    tracing::trace!("[scan] {:?} {}", data.scan_time, &provider.name);
                    tracing::trace!("[scan] {:?} debounce_timepoint -- {}", debounce_timepoint, tern!(now > debounce_timepoint, "scan!", "skip"));
                } else {
                    tracing::trace!("[scan] error loading file, scanning");
                    skip_scan = false;
                }
            }
        }

        if skip_scan {
            tracing::trace!("[scan] debounce, skipping scan");
            return Ok(());
        }

        // make sure there is a cache/provider dir to cache search results in
        create_dir(join_path!(&self.config.cache_dir, "provider"))?;

        // max thread count
        let mut tmax = self.config.scan_threads as u32;
        if tmax == 0 {
            tmax = std::thread::available_parallelism().map_or(1, |v| v.get() as u32);
        };
        let tcount = AtomicU32::new(0);

        std::thread::scope(|s| {

            for provider in self.filtered_providers() {

                while tcount.load(std::sync::atomic::Ordering::SeqCst) >= tmax {
                    // this doesn't need to be any more sophisticated than a spin wait
                    std::thread::sleep(std::time::Duration::from_millis(100));
                }

                // incrment active thread counter
                tcount.fetch_add(1, std::sync::atomic::Ordering::SeqCst);

                s.spawn(|| -> AResult<()> {

                    let tid = std::thread::current().id().as_u64();

                    // decrement the thread counter upon drop (when this thread is done)
                    let _decr = AtomicDecrDrop{val: &tcount};

                    let mut lock_path = provider.cache_file.clone();
                    if let Some(e) = lock_path.extension() {
                        lock_path = lock_path.with_extension(format!("{}.lock", e));
                    } else {
                        lock_path = lock_path.with_extension(".lock");
                    }

                    let mut lock_contended = false;
                    let lock_file = bpmutil::open_lockfile(&lock_path)?;
                    if !lock_file.try_lock().context("file lock")? {
                        lock_contended = true;
                        tracing::debug!("[scan] [t{tid}] waiting for file lock");
                        lock_file.lock().context("file lock")?;
                    }

                    // if the first attempt to get the lockfile failed, something else had the file
                    // locked, likely another scan in parallel. Check if the file was just updated,
                    // and skip the scan if so.
                    if lock_contended {
                        tracing::debug!("[scan] [t{tid}] lockfile was contended, checking if debounce is satisfied");
                        if let Ok(data) = provider.load_file() {
                            let debounce_timepoint = data.scan_time + tern!(debounce.is_zero(), Duration::from_secs(10), debounce);
                            if now < debounce_timepoint {
                                tracing::debug!("[scan] [t{tid}] detected concurrent scan with acceptable debounce, skipping scan");
                                return Ok(());
                            }
                        }
                    }

                    tracing::debug!("[scan] [t{tid}] scanning {}", &provider.name);

                    let arch_filter = self.arch_filter.iter().map(|s| s.as_str()).collect::<Vec<&str>>();
                    let arch_filter = if arch_filter.is_empty() { None } else { Some(arch_filter) };
                    let arch_filter = arch_filter.as_deref();

                    let list = scan_result::Scan::scan(provider.as_provide(), arch_filter);

                    if let Ok(list) = list {

                        let pkg_count = list.packages.len();
                        let ver_count : usize = list.packages.iter().map(|pkg_info| pkg_info.1.versions.len()).sum();
                        tracing::debug!("[scan] [t{}] {}: {} packages, {} versions", tid, &provider.name, pkg_count, ver_count);

                        let now = chrono::Utc::now().round_subsecs(0);

                        // write the package list to a cache file
                        let out = provider::ProviderFile {
                            scan_time: now,
                            packages: list,
                        };

                        let s = serde_json::to_string_pretty(&out)?;
                        let mut file = File::create(&provider.cache_file)?;
                        file.write_all(s.as_bytes())?;
                    } else {
                        tracing::warn!("[scan] [t{}] error while scanning {}: {}", tid, &provider.name, list.unwrap_err());
                        let out = provider::ProviderFile {
                            scan_time: now,
                            packages: Default::default(),
                        };
                        let s = serde_json::to_string_pretty(&out)?;
                        let mut file = File::create(&provider.cache_file)?;
                        file.write_all(s.as_bytes())?;
                    }
                    Ok(())

                }); // spawn
            }

        }); // end of thread scope, all threads are done

        Ok(())
    }

    /// clear any temp download files that may be left behind by
    /// any previous runs that were killed while downloading
    fn clear_temp_downloads(cache_dir: &Utf8Path) {

        if let Ok(files) = std::fs::read_dir(join_path_utf8!(cache_dir, "packages")) {
            for file in files.flatten() {
                let path = Utf8Path::from_path(file.path().as_path()).map(Utf8Path::to_path_buf);

                if file.file_type().is_ok_and(|t| t.is_file())
                    && let Some(path) = path
                    && let Some(name) = path.file_name()
                    && name.starts_with(TEMP_DOWNLOAD_PREFX) {

                    let fd = std::fs::File::open(&path);
                    if let Ok(fd) = fd && let Ok(true) = fd.try_lock() {
                        tracing::trace!("removing {}", name);
                        let _ = fd.unlock();
                        drop(fd);
                        let _ = std::fs::remove_file(path);
                    } else {
                        tracing::trace!("cannot remove {}, could not get lock", name);
                    }
                }
            }
        }
    }

    /// `bpm cache clean`
    /// Remove package files that have expired.
    pub fn cache_clean(&mut self) -> AResult<()> {

        self.exclusive_lock()?;

        let retention = self.config.cache_retention;

        let now = chrono::Utc::now().round_subsecs(0);
        tracing::trace!("cache retention {:?} ({})", retention, now - retention);

        // it's okay if the db doesn't exist, then we won't preserve any in-use package files,
        // and none should be in use if the db doesn't exist.
        let _ = self.load_db();

        // gather a list of package files in the cache
        let pkg_dir = join_path!(&self.config.cache_dir, "packages");

        // remove any saved cache files that no longer exist in the filesystem
        self.db.cache_files.retain(|f| {
            join_path!(&pkg_dir, &f.filename).exists()
        });

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

                tracing::trace!(touch=?touch, in_use=in_use, "[cache clean] {} {}", tern!(remove, "remove", "keep"), filename);

                if remove {
                    let ok = std::fs::remove_file(&path);
                    if let Err(e) = ok {
                        eprintln!("failed to remove {}: {}", path, e);
                    } else {
                        self.db.cache_files.retain(|e| e.filename != filename);
                    }
                }
            }
        }

        // save changes to db
        self.save_db()?;

        // also remove any temp download files if we can get an exclusive lock on them
        Self::clear_temp_downloads(&self.config.cache_dir);

        Ok(())
    }

    /// `bpm cache clear`
    /// Remove all cached package files.
    /// Optionally including the package files that are "in use" (packages thats are currently installed).
    pub fn cache_clear(&mut self, in_use: bool) -> AResult<()> {

        self.exclusive_lock()?;

        // it's okay if the db doesn't exist, then we won't preserve any in-use package files,
        // and none should be in use if the db doesn't exist.
        let _ = self.load_db();

        // gather a list of package files in the cache
        let pkg_dir = join_path!(&self.config.cache_dir, "packages");

        // remove any saved cache files that no longer exist in the filesystem
        self.db.cache_files.retain(|f| {
            join_path!(&pkg_dir, &f.filename).exists()
        });

        for path in walkdir::WalkDir::new(&pkg_dir)
            .max_depth(1) // package files are a flat list at the root dir
            .into_iter()
            .skip(1)      // skip the root dir
            .flatten()    // skip error entries
            .filter_map(|entry| Utf8Path::from_path(entry.path()).map(Utf8Path::to_path_buf))
        {
            if let Some(filename) = path.file_name() {
                let mut remove = true;

                match self.db.cache_files.iter().find(|e| e.filename == filename) {
                    None => { }
                    Some(ent) => {
                        if ent.in_use && !in_use {
                            remove = false;
                        }
                    }
                }

                if remove {
                    tracing::trace!("cache clear - remove {}", filename);
                    let ok = std::fs::remove_file(&path);
                    if let Err(e) = ok {
                        eprintln!("failed to remove {}: {}", path, e);
                    } else {
                        self.db.cache_files.retain(|e| e.filename != filename);
                    }
                }
            }
        }

        self.save_db()?;

        // also remove any temp download files if we can get an exclusive lock on them
        Self::clear_temp_downloads(&self.config.cache_dir);

        Ok(())
    }

    /// `bpm cache evict`
    /// Remove a specific package (all versions, or a specific version) from the cache.
    /// Optionally removing package files that are "in use" (installed).
    pub fn cache_evict(&mut self, pkg: &str, version: Option<&String>, in_use: bool) -> AResult<()> {

        self.exclusive_lock()?;

        // it's okay if the db doesn't exist, then we won't preserve any in-use package files,
        // and none should be in use if the db doesn't exist.
        let _ = self.load_db();

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
                        // note: package files of any arch are all evicted
                        if let Some((pname, pversion, _arch)) = package::split_parts(fname) {

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

        self.shared_lock()?;

        tracing::trace!("cache list");

        // it's okay if the db doesn't exist, we just won't know if the package is in use or when it expires.
        // But if the db file doesn't exist, then it shouldn't be in use, and it should likely be considered always expired.
        let _ = self.load_db();

        let term = std::io::stdout().is_terminal();
        let mut tw = tabwriter::TabWriter::new(std::io::stdout());

        if term {
            let _ = writeln!(&mut tw, "name\tversion\tarch\tfilename\tsize\tin use\texpiration");
        }

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
                        if let Some((name, version, arch)) = package::split_parts(fname) {

                            let arch = arch.unwrap_or("noarch");

                            let find = self.db.cache_files.iter().find(|ent| ent.filename == fname);
                            let retention = find.and_then(|ent| ent.retention).unwrap_or(retention);

                            let in_use = tern!(find.map(|ent| ent.in_use).unwrap_or(false), "yes", "no");

                            let mut duration = String::from("?");
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
                            if term {
                                let _ = writeln!(&mut tw, "{}\t{}\t{}\t{}\t{}\t{}\t{}", name, version, arch, fname, size, in_use, duration);
                            } else {
                                println!("{},{},{},{},{},{},{}", name, version, arch, fname, size, in_use, duration);
                            }
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
    pub fn cache_fetch(&mut self, pkgs: &[&String]) -> AResult<()> {

        self.exclusive_lock()?;

        let pkg_args : Result<Vec<PkgArgType>, _> = pkgs.iter().map(|s| parse_pkg_arg(s)).collect();
        let mut pkg_args = pkg_args?;

        self.create_load_db()?;

        // --- first, do any that are direct file paths ---

        let mut error_count = 0;

        let filepaths : Vec<_> = pkg_args.extract_if(.., |arg| matches!(arg, PkgArgType::Filepath{..})).collect();
        for f in filepaths {
            match f {
                PkgArgType::Filepath { path, .. } => {
                    //println!("   copy {}", path);
                    tracing::info!("cache copy {}", path);
                    if let Ok((did_cache, cache_path, hash)) = self.cache_store_file(&path) {
                        // don't want to insert a new cache entry if the package wasn't actually
                        // copied to the cache, this can happen if the given file path is a path
                        // inside the cache dir.
                        if did_cache {
                            if let Some(filename) = cache_path.file_name() {
                                self.db.cache_insert(filename, hash, None);
                            } else {
                                tracing::warn!("filepath {} had no filename", cache_path);
                            }
                        }
                    } else {
                        error_count += 1;
                    }
                }
                PkgArgType::Unversioned(..) |
                PkgArgType::Versioned(..) => {}
            }
        }


        // --- second, do any that are coming from a provider ---

        let pkg_args : Vec<_> = pkg_args.into_iter().filter_map(|p| {
            match p {
                PkgArgType::Filepath{..} => None,
                PkgArgType::Versioned(name, version) => Some((name, Some(version))),
                PkgArgType::Unversioned(name) => Some((name, None)),
            }
        })
        .collect();

        let mut ids = Vec::new();
        for (name, version) in pkg_args {
            let (listing, _versioning) = self.find_package_version(&name, version.as_deref(), None, None)?;
            let id = PackageID {
                name,
                version: listing.version.to_string(),
                arch: listing.arch,
            };
            ids.push(id);
        }

        let providers : Vec<&provider::Provider> = self.filtered_providers().collect();

        let mut jobs = std::cmp::max(1, self.config.cache_fetch_jobs as usize);
        jobs = std::cmp::min(jobs, ids.len());

        let ids = Mutex::new(ids);
        let touch : Mutex<Vec<_>> = Mutex::new(Vec::new());

        let error_count = std::sync::atomic::AtomicU32::new(error_count);

        std::thread::scope(|s| {
            for _tid in 0..jobs {
                let ids = &ids;
                let cache_dir = &self.config.cache_dir;
                let providers = &providers;
                let error_count = &error_count;
                let touch = &touch;
                let _t = s.spawn(move || {
                    loop {
                        let id = match ids.lock().unwrap().pop() {
                            None => { break; },
                            Some(id) => id
                        };
                        if let Ok((is_new, cache_path, hash)) = Self::mt_cache_package_require(providers, cache_dir, &id) {
                            tracing::trace!("cache fetched {}", cache_path);
                            if is_new {
                                touch.lock().unwrap().push((cache_path, hash));
                            }
                        } else {
                            tracing::warn!("failed to fetch {}", id.name);
                            eprintln!("failed to fetch {}@{}", id.name, id.version);
                            error_count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        }
                    }
                });
            }
        });

        for (path, hash) in touch.into_inner().unwrap() {
            if let Some(fname) = path.file_name() {
                self.db.cache_insert(fname, hash, None);
            }
        }
        self.save_db()?;

        let error_count = error_count.into_inner();
        if error_count > 0 {
            return Err(anyhow::anyhow!("failed to fetch {} package{}", error_count, tern!(error_count == 1, "", "s")));
        }

        Ok(())
    }

    /// `bpm cache touch`
    pub fn cache_touch(&mut self, pkg: &str, version: Option<&String>, duration: Option<std::time::Duration>) -> AResult<()> {

        self.exclusive_lock()?;
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
                        // note: package files of any arch are all touched
                        if let Some((pname, pversion, _arch)) = package::split_parts(fname) {

                            if pname != pkg {
                                continue;
                            }

                            if let Some(version) = version && version != pversion {
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
        Self::mt_cache_package_lookup(&self.config.cache_dir, id)
    }

    /// LOOKUP a package in the cache.
    /// Given a PackageID, find the package file in the cache dir if it exists
    fn mt_cache_package_lookup(cache_dir: &Utf8Path, id: &PackageID) -> Option<Utf8PathBuf> {

        for entry in walkdir::WalkDir::new(join_path_utf8!(&cache_dir, "packages"))
            .min_depth(1) // skip the directory itself (at depth 0)
            .max_depth(1) // only the contents of the directory directly, not in any sub dir (depth 1)
            .into_iter()
            .flatten()
        {
            let filename = entry.file_name().to_string_lossy();
            if package::filename_match(&filename, id) {
                tracing::trace!("mt_cache_package_lookup() hit {}", filename);
                let path = Utf8PathBuf::from_path_buf(entry.into_path());
                return match path {
                    Ok(path) => Some(path),
                    Err(_) => {
                        tracing::error!("cache hit invalid non-utf8 path");
                        None
                    }
                }
            }

        }
        tracing::trace!("mt_cache_package_lookup() miss {} {}", id.name, id.version);
        None
    }

    /// LOOKUP a package in the cache
    /// OR FETCH a package and cache it.
    fn cache_package_require(&self, id: &PackageID) -> AResult<(bool, Utf8PathBuf, Option<String>)> {
        match self.cache_package_lookup(id) {
            Some(cached_file) => Ok((false, cached_file, None)),
            None => {
                let (cache_file, hash) = self.cache_fetch_cmd(id)?;
                Ok((true, cache_file, hash))
            }
        }
    }

    /// LOOKUP a package in the cache
    /// OR FETCH a package and cache it.
    ///
    /// returns (is_new, cache_path, pkg_hash)
    fn mt_cache_package_require(providers: &[&provider::Provider], cache_dir: &Utf8Path, id: &PackageID) -> AResult<(bool, Utf8PathBuf, Option<String>)> {
        match Self::mt_cache_package_lookup(cache_dir, id) {
            Some(cached_file) => Ok((false, cached_file, None)),
            None => {
                //let (cache_file, hash) = Self::mt_cache_fetch_cmd(providers, cache_dir, id, bars)?;
                //let (cache_file, hash) = Self::mt_cache_fetch_cmd(providers, cache_dir, id, bars)?;
                let (cache_file, hash) = Self::mt_cache_fetch_cmd(providers, cache_dir, id)?;
                Ok((true, cache_file, hash))
            }
        }
    }


    // returns (cache_path, pkg_hash)
    fn mt_cache_fetch_cmd(providers: &[&provider::Provider], cache_dir: &Utf8Path, id: &PackageID) -> AResult<(Utf8PathBuf, Option<String>)> {
        Self::mt_cache_fetch_cmd2(providers.iter().copied(), cache_dir, id)
    }

    // note: this does not error on the first failure, it will try another provider
    //
    // returns (cache_path, pkg_hash)
    fn mt_cache_fetch_cmd2<'a, T>(providers: T, cache_dir: &Utf8Path, id: &PackageID) -> AResult<(Utf8PathBuf, Option<String>)>
        where T: Iterator<Item=&'a provider::Provider>,
    {

        tracing::debug!(pkg=id.name, version=id.version, "[cache fetch]");

        // ensure that we have the cache/packages dir
        std::fs::create_dir_all(join_path_utf8!(&cache_dir, "packages")).context("failed to create cache/packages dir")?;

        for provider in providers {

            tracing::trace!(name=provider.name, "[cache fetch] trying provider");

            if let Ok(provider::ProviderFile{packages, ..}) = provider.load_file() {
                if let Some(pkg_info) = packages.packages.get(&id.name) {
                    if let Some(vlist) = pkg_info.versions.get(&id.version.as_str().into()) {

                        for info in vlist.iter().filter(|ent| package::ArchMatcher::from(id.arch.as_deref()).matches(ent.arch.as_deref())) {

                            tracing::debug!(uri=info.uri, "[cache fetch] trying");

                            let final_path = join_path_utf8!(&cache_dir, "packages", &info.filename);

                            let rand_str = Alphanumeric.sample_string(&mut rand::thread_rng(), 6);
                            let temp_path = final_path.with_file_name(format!("{}{}_{}.fetch", TEMP_DOWNLOAD_PREFX, rand_str, &info.filename));

                            let mut file = std::fs::OpenOptions::new().create(true).read(true).write(true).truncate(true).open(&temp_path)?;

                            if let Ok(true) = file.try_lock() {
                                // good
                            } else {
                                tracing::warn!("could not acquire lock on temp download file {}", temp_path);
                            }

                            let result = provider.as_provide().fetch(&mut file, id, &info.uri);
                            match result {
                                Err(ref _e) => { tracing::warn!(uri=info.uri, "[cache fetch] fetch failed"); }
                                Ok(_v)      => { tracing::debug!(uri=info.uri, "[cache fetch] fetch success"); }
                            }

                            let mut err = result.is_err();
                            if !err { err = file.sync_all().is_err(); }
                            if !err { err = file.rewind().is_err(); }

                            // the packagefile's hash, to be inserted into the cache for faster verification later
                            let mut hash = None;

                            if !err {

                                // integrity check the package
                                let check = package::package_integrity_check_full(&mut file, Some(&info.filename), None);

                                if let Ok(check) = check && check.good() {
                                    // good!
                                    tracing::trace!("[cache fetch] integrity check good {}", info.filename);
                                    hash = Some(check.file_hash);
                                } else {
                                    tracing::trace!("[cache fetch] integrity check fail {}", info.filename);
                                    // delete temp file and return error
                                    drop(file);
                                    let _ = std::fs::remove_file(&temp_path);
                                    err = true;
                                }
                            }

                            if !err {
                                tracing::trace!("moving fetched package file from {} to {}", temp_path, final_path);
                                err = std::fs::rename(&temp_path, &final_path).context("failed to move file").is_err();
                            }

                            if !err {
                                return Ok((final_path, hash));
                            }
                        }
                    }
                }
            }
        }

        Err(anyhow::anyhow!("failed to fetch package"))
    }

    /// Directly copy a file into the cache dir
    ///
    /// returns (did_copy, cached_path, packagefile_hash)
    pub fn cache_store_file(&self, path: &Utf8Path) -> AResult<(bool, Utf8PathBuf, Option<String>)> {

        // ensure that we have the cache/packages dir
        std::fs::create_dir_all(join_path_utf8!(&self.config.cache_dir, "packages")).context("failed to create cache/packages dir")?;

        // check that the package file is not a path to a file in the cache dir
        // (do not copy in place)

        let filename = path.file_name().context("path has no filename")?;
        let cache_path = join_path_utf8!(&self.config.cache_dir, "packages", filename);
        let temp_path = cache_path.with_file_name(format!("{}_{}.move", TEMP_DOWNLOAD_PREFX, filename));

        let in_cache_dir = {
            let cache_path = cache_path.canonicalize_utf8().ok();
            let path = path.canonicalize_utf8().ok();
            tracing::trace!("{:?} == {:?} {}",  cache_path, path, cache_path == path);
            path == cache_path
        };

        if in_cache_dir {
            return Ok((false, cache_path, None));
        }

        // copy to the cache dir
        tracing::trace!("copying {} to {}", path, temp_path);

        let filesize = get_filesize(path.as_str()).ok();
        let bar = bpmutil::status::global().add_task(Some("copyfile"), Some(filename), filesize);
        bar.set_style(indicatif::ProgressStyle::with_template(
            #[allow(clippy::literal_string_with_formatting_args)]
            " {spinner:.green} caching package {wide_bar:.green} {bytes_per_sec}  {bytes}/{total_bytes} "
        ).unwrap());

        std::io::copy(
            &mut File::open(path)?,
            &mut bar.wrap_write(&mut File::create(&temp_path)?)
        ).context("failed to copy package file into cache")?;

        bar.finish_and_clear();

        // integrity check the package
        let mut file = std::fs::File::open(&temp_path)?;
        //let check = package::package_integrity_check_full(&mut file, Some(filename), None, Some(&self.status));
        let check = package::package_integrity_check_full(&mut file, Some(filename), None);

        // the packagefile's hash, to be inserted into the cache for faster verification later
        let hash;

        if let Ok(check) = check && check.good() {
            // good!
            tracing::trace!("integrity check good {}", filename);
            hash = Some(check.file_hash);
        } else {
            tracing::trace!("integrity check fail {}", filename);
            // delete temp file and return error
            let _ = std::fs::remove_file(&temp_path);
            anyhow::bail!("package integrity check failed, rejecting package");
        }

        tracing::trace!("moving {} to {}", temp_path, cache_path);
        std::fs::rename(&temp_path, &cache_path).context("rename file")?;

        Ok((true, cache_path, hash))
    }

    /// fetch a package by name AND version.
    /// network: yes
    ///
    pub fn cache_fetch_cmd(&self, id: &PackageID) -> AResult<(Utf8PathBuf, Option<String>)> {
        Self::mt_cache_fetch_cmd2(self.filtered_providers(), &self.config.cache_dir, id)
    }

} // impl App

use std::sync::atomic::AtomicU32;

struct AtomicDecrDrop<'a> {
    val: &'a AtomicU32,
}

impl<'a> Drop for AtomicDecrDrop<'a> {
    fn drop(&mut self) {
        self.val.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
    }
}
