use bpmutil::*;
use chrono::SubsecRound;
use crate::*;
use indicatif::MultiProgress;
use itertools::Itertools;
use package::PackageID;
use rand::distributions::{Alphanumeric, DistString};
use serde::{Serialize, Deserialize};
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::BTreeSet;
use std::fs::File;
use std::io::BufWriter;
use std::io::IsTerminal;
use std::io::Read;
use std::io::Seek;
use std::ops::Deref;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

#[cfg(unix)]
use std::os::unix::fs::MetadataExt;

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

//TODO multiple paths for a provider
//TODO fastpath, pre-scan, pre-scan age limit, version it
//TODO verify at cache time, store packagefile hash for quick verifies later
//
//DONE -t, --target <dir> for the install command, for new installs only, ignored with --update,--reinstall
//...
//DONE dirs without write perms cause problems
//...
//DONE config scan.debounce default --debounce value
//DONE lockfile more granular
//...
//DONE threads for scanning multiple providers at a time
//DONE config scan.threads
//...
//DONE make sure the directories can be made if they don't exist
//DONE cleanup errors about not being able to read database file when it really doesn't need to read db
//DONE http/file scan arch filters
//DONE arch in config
//DONE --arch in many subcmds
//DONE simple truthy replacements
//DONE custom replacements
//DONE env replacements
//DONE basic lockfile
//DONE cache auto_clean (install, uninstall, update)
//DONE cache touch_on_uninstall
//DONE multi provider fetching (failure fallback)
//...
//DONE http async, multi-scan
//DONE fetch multiple packages at once

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

impl PkgArgType {
    fn path(&self) -> Option<&Utf8PathBuf> {
        match self {
            Self::Filepath { path, .. } => Some(path),
            _ => None
        }
    }
    fn name(&self) -> Option<&String> {
        match self {
            Self::Filepath { name, .. } |
            Self::Unversioned(name) |
            Self::Versioned(name, ..) => Some(name),
        }
    }
    fn version(&self) -> Option<&str> {
        match self {
            Self::Filepath { version, .. } => Some(version.as_str()),
            Self::Versioned(_, version) => Some(version.as_str()),
            Self::Unversioned(_) => None,
        }
    }
}

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

        let mut results = self.search_results(pkg_name, exact)?;

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

    pub fn setup_arch_filter(&mut self, args: Option<Vec<&String>>) {
        if let Some(filters) = args {
            self.arch_filter = filters.iter().map(|s| s.to_string()).collect();
        } else if let Some(filters) = &self.config.arch_filter {
            self.arch_filter = filters.iter().map(|s| s.to_string()).collect();
        //} else {
            //self.arch_filter.push(String::from(""));
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
            let mut arch_filter = self.arch_filter.iter().map(|s| s.as_str()).collect::<Vec<&str>>();
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
    fn install_pkg_file(&mut self, file_path: Utf8PathBuf, versioning: Versioning, target: Option<&String>) -> AResult<()> {

        tracing::debug!("install_pkg_file {}", file_path);

        let package_file_filename = file_path.file_name().context("invalid filename")?;

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
        //let mut metadata = package::get_metadata(&mut file).context("error reading metadata")?;

        // make sure the checksum matches
        let (ok, mut metadata) = package::package_integrity_check(&mut file)?;
        if !ok {
            anyhow::bail!("failed to install {}, package file failed integrity check", pkg_name);
        }

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
        let (mut inner_tar, size) = package::seek_to_tar_entry("data.tar.zst", &mut outer_tar)?;
        let mut zstd = zstd::stream::read::Decoder::new(&mut inner_tar)?;
        let mut data_tar = tar::Archive::new(&mut zstd);

        let pbar = indicatif::ProgressBar::new(metadata.files.len() as u64);
        pbar.set_style(indicatif::ProgressStyle::with_template(
            #[allow(clippy::literal_string_with_formatting_args)]
            "   {msg}\n {spinner:.green} installing {prefix} {wide_bar:.green} {pos}/{len} "
        ).unwrap());
        pbar.set_prefix(metadata.name.to_string());

        #[cfg(unix)]
        let mut ro_dirs = HashMap::new();

        // unpack all files individually
        for entry in data_tar.entries()? {

            let mut entry = entry?;

            let path = Utf8PathBuf::from_path_buf(entry.path().unwrap().into_owned()).unwrap();
            //pbar.suspend(|| println!("{}", path));

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
                            std::fs::set_permissions(parent_dir, perms)?;
                        }
                    }
                }
            }

            let installed_ok = entry.unpack_in(&install_dir_full)?;

            pbar.set_message(String::from(path.as_str()));

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
        }

        // restore the permissions on any readonly dir that was modified during installation
        #[cfg(unix)]
        for (dir, mode) in ro_dirs {
            if let Some(mode) = mode {
                let md = std::fs::metadata(&dir)?;
                let mut perms = md.permissions();
                perms.set_mode(mode);
                std::fs::set_permissions(&dir, perms)?;
            }
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
        let mut arch_filter_iter = required_arch.into_iter()
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
        if let Some((version, mut vlist)) = pinfo.versions.pop_last() {

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

        let trace_span = tracing::trace_span!("bpm install").entered();

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
            let (listing, mut versioning) =
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

        let cache_file = if let Some(path) = pkg_file_path {
            self.cache_store_file(&path)?
        } else {
            //println!("Fetching {pkg_name} {pkg_version}");
            self.cache_package_require(&id)?
        };

        if already_installed {
            if reinstall {
                println!("Installing (re-install) {pkg_name} {pkg_version}");
            } else {
                println!("Updating {} from version {} to {}", pkg_name, current_version.unwrap_or("?"), pkg_version);
            }
        } else {
            println!("Installing {pkg_name} {pkg_version}");
        }

        //// cache touch the file now
        //if let Some(filename) = cache_file.file_name() {
        //    self.db.cache_touch(filename, None);
        //    self.save_db();
        //}

        let ret = if already_installed {
            self.update_inplace(&pkg_name, cache_file, versioning)
        } else {
            self.install_pkg_file(cache_file, versioning, target)
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

        self.delete_package_files(pkg, verbose, remove_unowned)?;
        self.db.remove_package(pkg.metadata.id());
        if let Some(filename) = package_file_filename {
            self.db.cache_set_in_use(&filename, false);

            if self.config.cache_touch_on_uninstall {
                self.db.cache_touch(&filename, None);
            }
        }
        self.save_db()?;

        if self.config.cache_auto_clean {
            tracing::trace!("[uninstall] cache auto clean");
            let _ = self.cache_clean();
        }

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
        let old_package_filename = current_pkg_info.package_file_filename.clone();
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

        let diff_bar = indicatif::ProgressBar::new(new_files.len() as u64);
        diff_bar.set_style(indicatif::ProgressStyle::with_template(
            #[allow(clippy::literal_string_with_formatting_args)]
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
        let pkg_filename = new_pkg_file.file_name().map(str::to_string);
        details.package_file_filename = pkg_filename;

        self.db.add_package(details);

        if let Some(filename) = new_pkg_file.file_name() {
            self.db.cache_touch(filename, None);
            self.db.cache_unuse_all_versions(pkg_name);
            self.db.cache_set_in_use(filename, true);

            if self.config.cache_touch_on_uninstall {
                if let Some(filename) = old_package_filename {
                    self.db.cache_touch(&filename, None);
                }
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
        let mut updates2 = Vec::new();

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

                updates2.push((pkg.metadata.name.clone(), pkg.metadata.version.clone(), listing, versioning, cached_file));
            }
        }

        if updates2.is_empty() {
            println!("No updates to apply");
            return Ok(());
        }

        println!("{} package{} to update:", updates2.len(), tern!(updates2.len() > 1, "s", ""));
        for (name, oldv, listing, _versioning, cache_file) in &updates2 {
            println!("  {}: {} -> {}", name, oldv, listing.version);
        }

        if updates2.iter().any(|(_name, _version, _listing, _versioning, cache_file)| cache_file.is_none()) {
            println!("Fetching Packages");
            for (name, oldv, listing, versioning, cache_file) in &mut updates2 {
                if cache_file.is_none() {
                    let ret = self.cache_package_require(&PackageID{
                        name: name.to_string(),
                        version: listing.version.to_string(),
                        arch: listing.arch.clone(),
                    });
                    if let Ok(path) = ret {
                        *cache_file = Some(path);
                    } else {
                        println!("failed to fetch {} {}, skipping", name, listing.version);
                    }
                }
            }
        }

        // remove any updates that don't have an incoming package file
        updates2.retain(|(_name, _version, _listing, _versioning, cache_file)| cache_file.is_some());

        let mut count = 0;
        for (name, oldv, listing, versioning, cache_file) in updates2 {
            if let Some(path) = cache_file {
                println!("Updating {} {} -> {}", name, oldv, listing.version);
                self.update_inplace(&name, path, versioning)?;
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
    pub fn verify_cmd<S>(&mut self, pkgs: &Vec<S>, restore: bool, restore_volatile: bool, verbose: bool, allow_mtime: bool) -> AResult<()>
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

        // TODO could put progress bars in here

        let mut new_cache_files = Vec::new();

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
                    arch: pkg.metadata.arch.clone(),
                }).context("could not find package file")?;

                if let Some(filename) = path.file_name() {
                    new_cache_files.push(filename.to_owned());
                }

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

        for filename in new_cache_files {
            self.db.cache_touch(&filename, None);
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
                        std::fs::set_permissions(parent_dir, perms)?;
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

        // restore the permissions on any readonly dir that was modified during installation
        #[cfg(unix)]
        for (dir, mode) in ro_dirs {
            if let Some(mode) = mode {
                let md = std::fs::metadata(&dir)?;
                let mut perms = md.permissions();
                perms.set_mode(mode);
                std::fs::set_permissions(&dir, perms)?;
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
            let (ok, metadata) = package::package_integrity_check(&mut file)?;
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

        master.retain(|pkg_name, kv| !kv.is_empty());
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
        let mut tcount = AtomicU32::new(0);

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
                    let decr = AtomicDecrDrop{val: &tcount};

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

                    let mut arch_filter = self.arch_filter.iter().map(|s| s.as_str()).collect::<Vec<&str>>();
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

        if let Ok(files) = std::fs::read_dir(cache_dir) {
            for file in files.flatten() {
                let typ = file.file_type();
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

        let pkg_name = pkgs[0].as_str();

        let pkg_args : Result<Vec<PkgArgType>, _> = pkgs.iter().map(|s| parse_pkg_arg(s)).collect();
        let mut pkg_args = pkg_args?;

        self.create_load_db()?;

        // --- first, do any that are direct file paths ---

        let filepaths : Vec<_> = pkg_args.extract_if(.., |arg| matches!(arg, PkgArgType::Filepath{..})).collect();
        for f in filepaths {
            match f {
                PkgArgType::Filepath { path, name, version, arch } => {
                    //println!("   copy {}", path);
                    tracing::info!("cache copy {}", path);
                    self.cache_store_file(&path)?;
                    let _ = self.cache_touch(&name, Some(&version.to_string()), None);
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
        let bars = MultiProgress::new();

        //let total_bar = indicatif::ProgressBar::new(ids.len() as u64)
        //    .with_style(indicatif::ProgressStyle::with_template("   Fetching {pos}/{len}   {elapsed} ").unwrap());
        //total_bar.enable_steady_tick(Duration::from_secs(1));
        //let total_bar = bars.add(total_bar);

        let jobs = std::cmp::min(self.config.cache_fetch_jobs as usize, ids.len());

        let ids = Mutex::new(ids);
        let mut touch : Mutex<Vec<_>> = Mutex::new(Vec::new());

        let mut had_error = std::sync::atomic::AtomicBool::new(false);

        std::thread::scope(|s| {
            for _tid in 0..jobs {
                let ids = &ids;
                let cache_dir = &self.config.cache_dir;
                let providers = &providers;
                let bars = &bars;
                let had_error = &had_error;
                //let total_bar = &total_bar;
                let touch = &touch;
                let _t = s.spawn(move || {
                    loop {
                        let id = match ids.lock().unwrap().pop() {
                            None => { break; },
                            Some(id) => id
                        };
                        if let Ok(cache_path) = Self::mt_cache_package_require(providers, cache_dir, &id, bars) {
                            tracing::trace!("cache fetched {}", cache_path);
                            touch.lock().unwrap().push(cache_path);
                        } else {
                            tracing::warn!("failed to fetched {}", id.name);
                            eprintln!("failed to fetch {}@{}", id.name, id.version);
                            had_error.store(true, std::sync::atomic::Ordering::SeqCst);
                        }
                    }
                });
            }
        });

        //total_bar.finish_and_clear();

        for path in touch.into_inner().unwrap() {
            if let Some(fname) = path.file_name() {
                self.db.cache_touch(fname, Some(self.config.cache_retention));
            }
        }
        self.save_db()?;

        if had_error.load(std::sync::atomic::Ordering::SeqCst) {
            anyhow::bail!("failed to fetch a package");
        }

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
                        // note: package files of any arch are all touched
                        if let Some((pname, pversion, arch)) = package::split_parts(fname) {

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

    /// LOOKUP a package in the cache OR FETCH a package and cache it.
    fn cache_package_require(&self, id: &PackageID) -> AResult<Utf8PathBuf> {
        let cached_file = match self.cache_package_lookup(id) {
            Some(cached_file) => cached_file,
            None => self.cache_fetch_cmd(id)?,
        };
        Ok(cached_file)
    }

    /// LOOKUP a package in the cache OR FETCH a package and cache it.
    fn mt_cache_package_require(providers: &[&provider::Provider], cache_dir: &Utf8Path, id: &PackageID, bars: &MultiProgress) -> AResult<Utf8PathBuf> {
        let cached_file = match Self::mt_cache_package_lookup(cache_dir, id) {
            Some(cached_file) => cached_file,
            None => Self::mt_cache_fetch_cmd(providers, cache_dir, id, bars)?,
        };
        Ok(cached_file)
    }

    fn mt_cache_fetch_cmd(providers: &[&provider::Provider], cache_dir: &Utf8Path, id: &PackageID, bars: &MultiProgress) -> AResult<Utf8PathBuf> {
        Self::mt_cache_fetch_cmd2(providers.iter().copied(), cache_dir, id, bars)
    }

    // note: this does not error on the first failure, it will try another provider
    fn mt_cache_fetch_cmd2<'a, T>(providers: T, cache_dir: &Utf8Path, id: &PackageID, bars: &MultiProgress) -> AResult<Utf8PathBuf>
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

                            let rand_str = Alphanumeric.sample_string(&mut rand::thread_rng(), 6);
                            let temp_name = format!("{}{}_{}_{}_{}", TEMP_DOWNLOAD_PREFX, &id.name, &id.version.to_string(), std::process::id(), rand_str);
                            let temp_path = join_path_utf8!(&cache_dir, &temp_name);
                            let mut file = File::create(&temp_path)?;

                            if let Ok(true) = file.try_lock() {
                                // good
                            } else {
                                tracing::warn!("couldn't get lock on temp download file {}", temp_name);
                            }

                            let final_path = join_path_utf8!(&cache_dir, "packages", &info.filename);

                            let result = provider.as_provide().fetch(&mut file, id, &info.uri, Some(bars));
                            match result {
                                Err(ref _e) => { tracing::warn!(uri=info.uri, "fetch failed"); }
                                Ok(_v)      => { tracing::debug!(uri=info.uri, "fetch success"); }
                            }
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
        }

        Err(anyhow::anyhow!("failed to fetch package"))
    }

    /// Directly copy a file into the cache dir
    pub fn cache_store_file(&self, path: &Utf8Path) -> AResult<Utf8PathBuf> {

        // ensure that we have the cache/packages dir
        std::fs::create_dir_all(join_path_utf8!(&self.config.cache_dir, "packages")).context("failed to create cache/packages dir")?;

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
                #[allow(clippy::literal_string_with_formatting_args)]
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
        Self::mt_cache_fetch_cmd2(self.filtered_providers(), &self.config.cache_dir, id, &MultiProgress::new())
    }

//    pub fn cache_fetch_cmd(&self, id: &PackageID) -> AResult<Utf8PathBuf> {
//        let providers : Vec<&provider::Provider> = self.filtered_providers().collect();
//        Self::mt_cache_fetch_cmd(&providers, &self.config.cache_dir, id, &MultiProgress::new())
//    }

//    pub fn cache_fetch_cmd(&self, id: &PackageID) -> AResult<Utf8PathBuf> {
//
//        tracing::debug!(pkg=id.name, version=id.version, "cache fetch");
//
//        // ensure that we have the cache/packages dir
//        std::fs::create_dir_all(join_path_utf8!(&self.config.cache_dir, "packages")).context("failed to create cache/packages dir")?;
//
//        let rand_str = Alphanumeric.sample_string(&mut rand::thread_rng(), 6);
//        let temp_name = format!("{}{}_{}_{}_{}", TEMP_DOWNLOAD_PREFX, id.name, id.version, std::process::id(), rand_str);
//        let temp_path = join_path_utf8!(&self.config.cache_dir, temp_name);
//        let mut file = File::create(&temp_path)?;
//
//        for provider in self.filtered_providers() {
//
//            if let Ok(provider::ProviderFile{packages, ..}) = provider.load_file() {
//                if let Some(pkg_info) = packages.packages.get(&id.name) {
//                    if let Some(vlist) = pkg_info.versions.get(&id.version.as_str().into()) {
//
//                        for info in vlist.iter().filter(|ent| package::ArchMatcher::from(id.arch.as_deref()).matches(ent.arch.as_deref())) {
//
//                            let final_path = join_path_utf8!(&self.config.cache_dir, "packages", &info.filename);
//
//                            let result = provider.as_provide().fetch(&mut file, id, &info.uri, None);
//                            if result.is_ok() {
//                                file.sync_all()?;
//                                drop(file);
//                                tracing::trace!("moving fetched package file from {} to {}", temp_path, final_path);
//                                std::fs::rename(&temp_path, &final_path).context("failed to move file")?;
//                                //println!("final_path {:?}", final_path);
//                                return Ok(final_path);
//                            }
//                        }
//                    }
//                }
//            }
//        }
//
//        Err(anyhow::anyhow!("failed to fetch package"))
//    }

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
