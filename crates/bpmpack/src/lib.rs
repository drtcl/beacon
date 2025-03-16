#![feature(let_chains)]
#![feature(iter_array_chunks)]

// take a list of files
// save them into a data tarball
// hash the tarball
// get the hash for each file
// save hashes into a meta file
// tar the hashes and data tarball into a single package tarball
// cleanup intermediate files

//  given:
//     a/
//       b/
//         c/
//           file.txt
//  and an ignore file of:
//  a/*
//  !a/b/c/file.txt
//
//  does this parent dirs: a, a/b, and a/b/c, need to be white listed and added to the data tar?
//

//! File attributes:
//!  A  added
//!  W  white listed -- explicitly not ignored
//!  I  ignored
//!  d  type is directory
//!  s  type is symlink
//!  v  volatile
//!  w  weak

use anyhow::{Context, Result};
use camino::{Utf8Path, Utf8PathBuf};
use ignore::gitignore::Gitignore;
use indicatif::ProgressStyle;
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::fs::File;
use std::io::{BufRead, Seek, BufWriter, BufReader, Read, Write};
use std::path::{PathBuf, Path};
use std::time::Duration;
use version::Version;

pub mod args;

const DEFAULT_ZSTD_LEVEL : i32 = 15;
const PKG_FORMAT_VERSION : &str = "1.0.0";

fn cwd() -> PathBuf {
    std::env::current_dir().expect("failed to get current dir")
}

/// determine how many threads to use.
/// 0 maps to MAX, where MAX is available_parallelism capped to 20
fn get_threads(n: u32) -> u32 {
    let avail = std::cmp::min(20, std::thread::available_parallelism().map_or(1, |v| v.get() as u32));
    if n == 0 || n > avail {
        avail
    } else {
        n
    }
}

#[derive(Clone, Debug)]
enum FileType {
    Dir,
    File,
    Link(Utf8PathBuf),
}

impl FileType {
    pub fn is_file(&self) -> bool {
        matches!(self, FileType::File)
    }
}

impl From<FileType> for package::FileType {
    fn from(ft: FileType) -> Self {
        match ft {
            FileType::Dir => package::FileType::Dir,
            FileType::File => package::FileType::File,
            FileType::Link(path) => package::FileType::Link(path.to_string()),
        }
    }
}

#[derive(Clone, Debug)]
struct IgnoreReason {
    file: String,
    pattern: String,
}

impl From<&ignore::gitignore::Glob> for IgnoreReason {
    fn from(glob: &ignore::gitignore::Glob) -> Self {
        IgnoreReason {
            file: glob.from().map_or(String::new(), |path| path.to_string_lossy().to_string()),
            pattern: glob.original().to_string(),
        }
    }
}

#[derive(Clone, Debug)]
struct FileEntry {
    full_path: Utf8PathBuf,
    pkg_path: Utf8PathBuf,
    file_type: FileType,
    hash: Option<String>,
    ignore: bool,
    ignore_reason: Option<IgnoreReason>,
    size: u64,
    modes: FileModes,
}

impl FileEntry {
    fn is_dir(&self) -> bool {
        matches!(self.file_type, FileType::Dir)
    }
    fn is_symlink(&self) -> bool {
        matches!(self.file_type, FileType::Link(_))
    }
}

#[derive(Clone, Debug)]
struct FileListing {
    files: Vec<FileEntry>,
}

#[derive(Debug, Default, Clone)]
struct FileModes {
    volatile: bool,
    weak: bool,
}

#[derive(Debug, Default)]
struct ModeGlob {
    modes: FileModes,
    ignore: bool,
    glob: String,
    source: String,
}

/// parse a single modes file into a list of ModeGlob
fn parse_modes_file(path: &Utf8Path) -> Result<Vec<ModeGlob>> {

    let file = BufReader::new(File::open(path)?);
    let mut globs = Vec::new();

    for (line_num, line) in file.lines().enumerate() {

        if let Ok(line) = line {

            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }

            let mut parts = line.trim().split(' ').filter(|s| !s.is_empty());

            let mut entry = ModeGlob::default();

            let modes = parts.next().context("missing modes")?;
            let glob = parts.next().context("missing path pattern")?;

            entry.glob = String::from(glob);

            for mode in modes.split(',').filter(|s| !s.is_empty()) {
                match mode {
                    "volatile" | "v" =>  {
                        entry.modes.volatile = true;
                    },
                    "weak" | "w" =>  {
                        entry.modes.weak= true;
                    },
                    "ignore" | "i" =>  {
                        entry.ignore = true;
                    },
                    _ => {
                        eprintln!("ignoring unrecognized file mode: {}:{} '{}'", path, line_num+1, mode);
                    }
                }
            }

            entry.source = format!("{}:{}", path, line_num+1);
            globs.push(entry);
        }
    }

    Ok(globs)
}

/// parse an ignore file into a list of ModeGlob
fn parse_ignore_file(path: &Utf8Path) -> Result<Vec<ModeGlob>> {

    let file = BufReader::new(File::open(path)?);
    let mut globs = Vec::new();

    for (line_num, line) in file.lines().enumerate() {

        if let Ok(line) = line {

            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }

            globs.push(ModeGlob {
                source: format!("{}:{}", path, line_num+1),
                ignore: true,
                glob: String::from(line),
                ..Default::default()
            });
        }
    }

    Ok(globs)
}

fn build_globs(modes: &Vec<Utf8PathBuf>, igfiles: &Vec<Utf8PathBuf>, igpatterns: Vec<String>) -> Result<Vec<ModeGlob>> {

    let mut globs = Vec::new();

    for file in modes {
        globs.extend(parse_modes_file(file)?);
    }

    for file in igfiles {
        globs.extend(parse_ignore_file(file)?);
    }

    globs.extend(igpatterns.into_iter().map(|pattern| {
        ModeGlob {
            ignore: true,
            source: format!("pattern:{pattern}"),
            glob: pattern,
            ..Default::default()
        }
    }).collect::<Vec<_>>());

    Ok(globs)
}

fn build_ignore_from_globs(globs: &Vec<ModeGlob>) -> Result<Option<Gitignore>> {

    let mut builder = ignore::gitignore::GitignoreBuilder::new(".");
    let mut empty = true;

    for glob in globs {

        if !glob.ignore {
            continue;
        }
        empty = false;

        builder.add_line(Some(glob.source.clone().into()), &glob.glob)?;
    }

    if empty {
        Ok(None)
    } else {
        Ok(Some(builder.build()?))
    }
}

#[derive(Debug)]
struct ModeMatcher {
    volatile: Gitignore,
    weak: Gitignore,
}

impl ModeMatcher {
    fn is_volatile(&self, path: &Utf8Path, is_dir: bool) -> bool {
        !matches!(self.volatile.matched(path, is_dir), ignore::Match::None)
    }
    fn is_weak(&self, path: &Utf8Path, is_dir: bool) -> bool {
        !matches!(self.weak.matched(path, is_dir), ignore::Match::None)
    }
}

fn build_mode_matcher(globs: &Vec<ModeGlob>) -> Result<ModeMatcher> {

    let mut volatile = ignore::gitignore::GitignoreBuilder::new(".");
    let mut weak = ignore::gitignore::GitignoreBuilder::new(".");

    for glob in globs {
        if glob.modes.volatile {
            volatile.add_line(Some(glob.source.clone().into()), &glob.glob)?;
        }
        if glob.modes.weak {
            weak.add_line(Some(glob.source.clone().into()), &glob.glob)?;
        }
    }

    Ok(ModeMatcher {
        volatile: volatile.build()?,
        weak: weak.build()?,
    })
}

/// Walk the filesystem, discovering all files from the given paths
fn file_discovery(paths: Vec<String>) -> Result<FileListing> {

    // make sure each given file path actually exists
    for path in &paths {
        if !Utf8Path::new(path).exists() {
            anyhow::bail!("path does not exist: {path}");
        }
    }

    let mut walker = ignore::WalkBuilder::new(paths.first().unwrap());
    for path in paths.iter().skip(1) {
        walker.add(path);
    }

    walker.standard_filters(false);
    walker.git_ignore(false);

    let mut files = Vec::new();

    let root = PathBuf::from(paths.first().unwrap());
    let root_parent = root.parent().unwrap();

    for entry in walker.build() {
        let entry = entry.unwrap();
        let full_path = PathBuf::from(entry.path());
        let full_path = Utf8PathBuf::from_path_buf(full_path);
        if full_path.is_err() {
            let _ = write!(std::io::stderr(), "warning: non-utf8 path, file not added. {}", full_path.unwrap_err().display());
            continue;
        }

        let full_path = full_path.unwrap();

        let pkg_path = full_path.strip_prefix(root_parent).unwrap().to_path_buf();
        let ent_file_type = entry.file_type().expect("can't determine file type");
        let full_path = canonicalize_no_symlink(&full_path).unwrap();

        let filesize = full_path.metadata().ok().map_or(0, |md| md.len());

        let file_type = {
            if ent_file_type.is_dir() {
                FileType::Dir
            } else if ent_file_type.is_file() {
                FileType::File
            } else if ent_file_type.is_symlink() {
                let link_to = std::fs::read_link(&full_path).unwrap();
                let link_to = Utf8PathBuf::from_path_buf(link_to);
                if link_to.is_err() {
                    let _ = write!(std::io::stderr(), "warning: non-utf8 symlink, file not added. {} -> {}", full_path, link_to.unwrap_err().display());
                    continue;
                }
                let link_to = link_to.unwrap();
                FileType::Link(link_to)
            } else {
                unreachable!("path is not a dir, file, or link")
            }
        };

        files.push(FileEntry {
            full_path,
            pkg_path,
            file_type,
            hash: None,
            ignore: false,
            ignore_reason: None,
            size: filesize,
            modes: FileModes::default(),
        });
    }

    Ok(FileListing { files })
}

/// Calls file_discovery(), then marks ignored files,
/// and optionally adds a wrapper root directory to all files
fn gather_files(paths: Vec<String>, wrap_dir: Option<&String>, ignore: &Option<Gitignore>, mode_matcher: &ModeMatcher) -> Result<FileListing> {

    let mut file_list = file_discovery(paths)?;

    let mut readd_parent_ignore = HashMap::new();

    // mark ignored files
    if let Some(ignore) = ignore {
        for file in file_list.files.iter_mut() {
            match ignore.matched_path_or_any_parents(&file.pkg_path, file.is_dir()) {
                ignore::Match::None => { }
                ignore::Match::Ignore(reason) => {
                    file.ignore = true;
                    file.ignore_reason = Some(reason.into());
                }
                ignore::Match::Whitelist(reason) => {
                    file.ignore_reason = Some(reason.into());

                    // add all parent directories to a set of dirs that must not be ingored
                    let mut path = file.pkg_path.as_path();
                    while let Some(parent_dir) = path.parent() {
                        if parent_dir != "" {
                            readd_parent_ignore.insert(parent_dir.to_path_buf(), file.ignore_reason.clone());
                        }
                        path = parent_dir;
                    }
                }
            }
        }
    }

    // re-add any parent dirs that were ignored, but contain white listed files
    if !readd_parent_ignore.is_empty() {
        for file in file_list.files.iter_mut() {
            if let Some(reason) = readd_parent_ignore.remove(&file.pkg_path)  {
                if file.ignore {
                    file.ignore = false;
                    file.ignore_reason = reason;
                }
            }
        }
    }

    // mark file modes
    for file in file_list.files.iter_mut() {
        file.modes.volatile = mode_matcher.is_volatile(&file.pkg_path, file.is_dir());
        file.modes.weak = mode_matcher.is_weak(&file.pkg_path, file.is_dir());
    }

    // add the wrap dir
    if let Some(wrap_dir) = wrap_dir {
        for ent in file_list.files.iter_mut() {
            let mut p = Utf8PathBuf::from(wrap_dir);
            p.push(&ent.pkg_path);
            ent.pkg_path = p;
            //let path = std::mem::replace(&mut ent.pkg_path, PathBuf::new());
        }
    }

    Ok(file_list)
}

pub fn subcmd_set_version(matches: &clap::ArgMatches) -> Result<()> {

    let require_semver = *matches.get_one::<bool>("semver").unwrap();
    let package_version = Version::new(matches.get_one::<String>("version").unwrap());
    let package_arch = matches.get_one::<String>("arch").map(|s| s.as_str());
    let package_filepath = matches.get_one::<String>("pkgfile").unwrap();
    let package_filepath = Utf8PathBuf::from_path_buf(PathBuf::from(package_filepath)).expect("failed to get file path");
    let package_filename = package_filepath.file_name().context("failed to get filename")?;

    if !package::is_valid_version(&package_version) {
        anyhow::bail!("invalid version")
    }
    if require_semver && !package_version.is_semver() {
        anyhow::bail!("Version is not a valid semver. A valid semver is required because the --semver option was used")
    }

    if !package_filepath.try_exists().unwrap_or(false) {
        anyhow::bail!("no such file");
    }

    if let Some((name, _version, arch)) = package::split_parts(package_filename) {

        if arch.is_some() && arch != package_arch {
            println!("warning: arch changing from '{}' to '{}'.", arch.unwrap(), package_arch.unwrap_or(""));
        }

        let package_arch = package_arch.or(arch);
        if !package::is_valid_arch(package_arch) {
            anyhow::bail!("invalid arch");
        }

        //if _version != "unversioned" {
            //anyhow::bail!("refusing to set the version of an already versioned package");
        //}

        let new_filename = package::make_packagefile_name(name, &package_version, package_arch);
        let new_filepath = package_filepath.with_file_name(new_filename);

        let out_file = std::fs::File::create(&new_filepath).context("failed to open file for writing")?;
        let mut out_tar = tar::Builder::new(out_file);

        let in_file = std::fs::File::open(&package_filepath).context("failed to open file for reading")?;
        let mut tar = tar::Archive::new(in_file);

        for mut entry in tar.entries().context("failed to read tar")?.flatten() {
            let path = entry.path()?.into_owned();

            let path_str = path.to_str().context("failed to stringify path")?;
            if path_str == package::META_FILE_NAME {

                // extract the MetaData struct
                let mut md = package::MetaData::from_reader(&mut entry).context("failed to extra metadata")?;

                // update the version, arch, and create a new uuid
                md.version = package_version.to_string();
                md.arch = package_arch.map(String::from);
                md.uuid = uuid::Uuid::new_v4().to_string();

                // re-serialize the struct and write that to the tar
                let mut md_file = Vec::new();
                md.to_writer(&mut md_file).context("failed to serialize metadata")?;

                let mut header = entry.header().clone();
                header.set_size(md_file.len() as u64);

                out_tar.append_data(&mut header, path_str, std::io::Cursor::new(&md_file)).context("failed to write to tar")?;
            } else {
                let header = entry.header().clone();
                out_tar.append(&header, &mut entry).context("failed to write to tar tar")?;
            }
        }

        // finish the output file
        let mut out_file = out_tar.into_inner()?;
        out_file.flush()?;
        drop(out_file);

        println!("Package created: {}", new_filepath);
    }

    Ok(())
}

pub fn subcmd_verify(path: &Path) -> Result<()> {

    if let Ok(true) = std::fs::exists(path) {
    } else {
        anyhow::bail!("file path does not exist");
    }

    let mut file = std::fs::File::open(path).context("failed to open package file")?;

    let filename = path.file_name().context("path has no filename")?.to_string_lossy();

    let check = package::package_integrity_check_full(&mut file, Some(&filename), None)?;
    if !check.good() {
        anyhow::bail!("package corrupt");
    }
    Ok(())
}

pub fn subcmd_list_files(path: &Path) -> Result<()> {

    let file = std::fs::File::open(path).context("failed to open package file")?;

    let mut tar = tar::Archive::new(file);
    let (data, _size) = package::seek_to_tar_entry(package::DATA_FILE_NAME, &mut tar)?;
    let zstd = zstd::Decoder::new(data)?;
    let mut tar = tar::Archive::new(zstd);
    for ent in tar.entries()? {
        let e = ent?;
        let path = e.path()?;
        println!("{}", path.display());
    }
    Ok(())
}

/// `bpmpack test-ignore`
pub fn subcmd_test_ignore(matches: &clap::ArgMatches) -> Result<()> {

    let wrap_with_dir = matches.get_one::<String>("wrap-with-dir");
    let verbose = *matches.get_one::<bool>("verbose").unwrap();

    let modes_files = matches.get_many::<String>("file-modes").map_or(Vec::new(), |paths| paths.map(Utf8PathBuf::from).collect());
    let ignore_files = matches.get_many::<String>("ignore-file").map_or(Vec::new(), |paths| paths.map(Utf8PathBuf::from).collect());
    let patterns = matches.get_many::<String>("ignore-pattern").map_or(Vec::new(), |patterns| patterns.map(String::from).collect());
    let globs = build_globs(&modes_files, &ignore_files, patterns)?;
    let ignore = build_ignore_from_globs(&globs)?;
    let mode_matcher = build_mode_matcher(&globs)?;

    let given_file_paths = matches.get_many::<String>("file").unwrap().cloned().collect();
    let file_list = gather_files(given_file_paths, wrap_with_dir, &ignore, &mode_matcher)?;

    let mut tw = tabwriter::TabWriter::new(std::io::stdout());
    let mut ignored_parents = HashSet::new();

    for file in file_list.files {

        if file.ignore {

            let mut parent_ignored = false;
            if let Some(parent) = file.pkg_path.parent() {
                if ignored_parents.contains(parent) {
                    parent_ignored = true;
                }
            }
            if verbose || !parent_ignored {
                let reason = file.ignore_reason.as_ref().map(|reason| format!("({} {})", reason.file, reason.pattern));
                let reason = reason.as_deref().unwrap_or("");
                writeln!(&mut tw, "I\t{}  {}", file.pkg_path, reason)?;
            }

            if file.is_dir() {
                ignored_parents.insert(file.pkg_path);
            }

        } else {

            let mut whitelisted = false;
            let mut w_reason = None;

            if let Some(reason) = &file.ignore_reason {
                whitelisted = true;
                w_reason = Some(format!("({} {})", reason.file, reason.pattern));
            }

            if whitelisted || verbose {
                writeln!(&mut tw, "A{}{}{}{}{}\t{}  {}",
                    if whitelisted         { "W" } else { "" },
                    if file.is_dir()       { "d" } else { "" },
                    if file.is_symlink()   { "s" } else { "" },
                    if file.modes.volatile { "v" } else { "" },
                    if file.modes.weak     { "w" } else { "" },
                    &file.pkg_path,
                    w_reason.as_deref().unwrap_or("")
                )?;
            }
        }
    }

    tw.flush()?;

    Ok(())
}

fn make_control_file() -> Result<Vec<u8>> {
    let mut data = vec![];
    writeln!(&mut data, "version = \"{}\"", PKG_FORMAT_VERSION)?;
    writeln!(&mut data, "compress = \"zstd\"")?;
    writeln!(&mut data, "hash = \"blake3\"")?;
    Ok(data)
}

pub fn make_package(matches: &clap::ArgMatches) -> Result<()> {

    let wrap_with_dir = matches.get_one::<String>("wrap-with-dir");
    let verbose = *matches.get_one::<bool>("verbose").unwrap();
    let mount = matches.get_one::<String>("mount");
    let require_semver = *matches.get_one::<bool>("semver").unwrap();

    let compress_level = *matches.get_one::<u32>("complevel").expect("expected compression level") as i32;
    let compress_level = if 0 == compress_level { DEFAULT_ZSTD_LEVEL } else { compress_level };

    let thread_count = get_threads(*matches.get_one::<u8>("threads").expect("expected thread count") as u32);

    let description = matches.get_one::<String>("description").cloned();

    let kv = matches.get_many::<String>("kv").map(|kv| {
        kv.array_chunks::<2>()
          .map(|kv| (kv[0].to_owned(), kv[1].to_owned()))
          .collect::<std::collections::BTreeMap<String, String>>()
    }).unwrap_or_default();

    let deps: Vec<(String, Option<String>)> = matches.get_many::<String>("depend")
        .map(|refs| refs.into_iter().map(|s| s.to_string()).collect::<Vec<_>>())
        .unwrap_or_default()
        .iter()
        .map(|s| {
            let mut iter = s.split('@');
            let name = iter.next().expect("package name required");
            if name.is_empty() {
                println!("nameless package dependency was given: '{}'", s);
                std::process::exit(2);
            }
            let version = iter.next().map(|s| s.to_string());
            (name.to_string(), version)
        })
        .collect();

    let output_dir = matches.get_one::<String>("output-dir").map_or(cwd(), PathBuf::from);
    if !output_dir.try_exists().ok().unwrap_or(false) {
        anyhow::bail!("output-dir path does not exist");
    }

    let modes_files = matches.get_many::<String>("file-modes").map_or(Vec::new(), |paths| paths.map(Utf8PathBuf::from).collect());
    let ignore_files = matches.get_many::<String>("ignore-file").map_or(Vec::new(), |paths| paths.map(Utf8PathBuf::from).collect());
    let patterns = matches.get_many::<String>("ignore-pattern").map_or(Vec::new(), |patterns| patterns.map(String::from).collect());
    let globs = build_globs(&modes_files, &ignore_files, patterns)?;
    let ignore = build_ignore_from_globs(&globs)?;
    let mode_matcher = build_mode_matcher(&globs)?;

    let package_name = matches.get_one::<String>("name").unwrap();
    if !package::is_valid_package_name(package_name) {
        anyhow::bail!("invalid package name. (must match [a-zA-Z][a-zA-Z0-9\\-]*)")
    }

    let unversioned = *matches.get_one::<bool>("unversioned").unwrap();

    // the version must be of the right semver format
    let package_version = if unversioned {
        Version::new("unversioned")
    } else {
        let package_version = Version::new(matches.get_one::<String>("version").unwrap());
        if !package::is_valid_version(&package_version) {
            anyhow::bail!("invalid version")
        }
        if require_semver && !package_version.is_semver() {
            anyhow::bail!("Version is not a valid semver. A valid semver is required because the --semver option was used")
        }
        package_version
    };

    let package_arch = matches.get_one::<String>("arch").map(|s| s.as_str());
    if !package::is_valid_arch(package_arch) {
        anyhow::bail!("invalid arch");
    }

    let given_file_paths = matches.get_many::<String>("file").unwrap().cloned().collect();
    let file_list = gather_files(given_file_paths, wrap_with_dir, &ignore, &mode_matcher)?;

    // file names
    // - data - all target file in the package
    // - meta - meta data about data files

    let package_filename = package::make_packagefile_name(package_name, package_version.as_str(), package_arch);
    let package_file_path = PathBuf::from(&output_dir).join(&package_filename);

    // scan the file list and verify symlinks
    let symlink_settings = SymlinkSettings {
        allow_dne: *matches.get_one::<bool>("allow-symlink-dne").unwrap(),
        allow_outside: *matches.get_one::<bool>("allow-symlink-outside").unwrap(),
    };
    verify_symlinks(&symlink_settings, &file_list)?;

    // -- progress bars --
    //                 _____________________________________________________________________
    //  size_bar       | .: packing files     5s/20s         32 MiB/s     120MiB / 1GiB    |
    //  count_bar      |    file 370/1000 [===========                                   ] |
    //  comp_bar       |    compression  [==            ] 5MiB / 120MiB                    |
    //  (then later)   |                                                                   |
    //  finish_bar     |    writing package [===============            ] 25MiB / 50MiB    |
    //                 ---------------------------------------------------------------------

    let file_size_sum = file_list.files.iter().filter(|ent| !ent.ignore).map(|ent| ent.size).sum::<u64>();
    let file_included_count = file_list.files.iter().filter(|ent| !ent.ignore).count() as u64;

    let status_mgr = bpmutil::status::global();
    let size_bar  = status_mgr.add_task(Some("packing"), Some(file_size_sum));
    let count_bar = status_mgr.add_task(Some("files"), Some(file_included_count));
    let comp_bar  = status_mgr.add_task(Some("compressing"), Some(0));
    size_bar.enable_steady_tick(Duration::from_millis(200));
    size_bar.set_style(ProgressStyle::with_template(
        " {spinner:.green} packing files   {elapsed}/{duration}   {bytes_per_sec}   {bytes}/{total_bytes} "
    ).unwrap());
    count_bar.set_style(ProgressStyle::with_template(
        #[allow(clippy::literal_string_with_formatting_args)]
        "   file {pos}/{len} {wide_bar:.green} ").unwrap()
    );
    comp_bar.set_style(ProgressStyle::with_template(
        #[allow(clippy::literal_string_with_formatting_args)]
        "   compression ratio {percent}%  {bytes} / {total_bytes} {bar:25} "
    ).unwrap());

    // layers of wrapping:
    // 1. raw file
    // 2. BufWriter
    // 3. HashingWriter
    // 4. zstd compressor
    // 5. CountingWriter (for uncompressed size)
    // 6. tar builder

    // 1
    let data_tar_file = tempfile::Builder::new().prefix(&format!("{}.{}.temp", package_name, package::DATA_FILE_NAME)).tempfile_in(&output_dir)?;

    let path = data_tar_file.path();
    tracing::debug!("using temp data file at {}", path.display());

    // 2
    let mut data_tar_bufwriter = BufWriter::with_capacity(1024 * 1024, data_tar_file);
    //let mut data_tar_compressed_size_writer = CountingWriter::new(data_tar_bufwriter);
    //let comp_bar_iter = comp_bar.wrap_write(&mut data_tar_compressed_size_writer);
    //let data_tar_hasher = HashingWriter::new(data_tar_compressed_size_writer, blake3::Hasher::new());

    let comp_bar_iter = comp_bar.wrap_write(&mut data_tar_bufwriter);
    // 3, 4
    let data_tar_hasher = HashingWriter::new(comp_bar_iter, blake3::Hasher::new());
    let mut data_tar_zstd = zstd::stream::write::Encoder::new(data_tar_hasher, compress_level)?;

    #[cfg(feature="mt")] {
        data_tar_zstd.multithread(thread_count)?;
        tracing::trace!("using {thread_count} threads");
    }

    // 5
    let data_tar_uncompressed_size_writer = CountingWriter::new(data_tar_zstd);

    // 6
    let mut data_tar_tar = tar::Builder::new(data_tar_uncompressed_size_writer);

    // don't follow symlink, look at symlinks as symlinks, not files
    data_tar_tar.follow_symlinks(false);

    // start creating the MetaData for this package
    let mut meta = package::MetaData::new(package::PackageID {
            name: package_name.clone(),
            version: package_version.to_string(),
            arch: package_arch.map(String::from),
        })
        .with_description(description)
        .with_kv(kv)
        .with_uuid(uuid::Uuid::new_v4().to_string());

    // insert dependencies
    for pair in &deps {
        meta.add_dependency(package::DependencyID{
            name: pair.0.clone(),
            version: Some(pair.1.as_ref().map_or("*".to_string(), |v| v.clone())),
        });
    }

    // keep track of parents that are ignored, don't report every single file that is ignored under
    // an already ignored directory
    let mut ignored_parents = HashSet::new();

    // create the target install files tar file
    for mut entry in file_list.files {

        if entry.ignore {
            if verbose {
                let mut parent_ignored = false;
                if let Some(parent) = entry.pkg_path.parent() {
                    if ignored_parents.contains(parent) {
                        parent_ignored = true;
                    }
                }
                if !parent_ignored {
                    status_mgr.suspend(|| println!("I \t{}", entry.pkg_path));
                }

                if entry.is_dir() {
                    ignored_parents.insert(entry.pkg_path);
                }
            }
            continue;
        }

        //std::thread::sleep_ms(1);
        count_bar.inc(1);
        size_bar.inc(entry.size);
        comp_bar.inc_length(entry.size);

        match entry.file_type {
            FileType::Dir => {
                data_tar_tar.append_dir(&entry.pkg_path, &entry.full_path).context("inserting dir")?;
            }
            FileType::File => {
                let fd = File::open(&entry.full_path).context("opening file")?;
                //let file_size = fd.metadata()?.len();

                let mut header = tar::Header::new_gnu();
                header.set_metadata_in_mode(
                    &fd.metadata().context("getting metadata")?,
                    tar::HeaderMode::Complete,
                );
                //header.set_size(file_size);
                //header.set_cksum();

                let mut reader = HashingReader::new(BufReader::new(fd), blake3::Hasher::new());

                data_tar_tar.append_data(&mut header, &entry.pkg_path, &mut reader).context("inserting file")?;
                let (_, hasher) = reader.into_parts();
                let hash = hasher.finalize();
                entry.hash = Some(hash.to_hex().to_string());
            }
            FileType::Link(ref link_path) => {
                let mut header = tar::Header::new_gnu();
                header.set_entry_type(tar::EntryType::Symlink);
                header.set_size(0);
                data_tar_tar.append_link(&mut header, &entry.pkg_path, link_path).context("inserting symlink")?;

                let mut hasher = blake3::Hasher::new();
                hasher.update(link_path.as_str().as_bytes());
                let hash = hasher.finalize().to_hex().to_string();
                entry.hash = Some(hash);
            }
        }

        if verbose {
            status_mgr.suspend(|| {
                println!("A{}{}{}{} \t{}",
                    if entry.is_dir()       { "d" } else { "" },
                    if entry.is_symlink()   { "s" } else { "" },
                    if entry.modes.volatile { "v" } else { "" },
                    if entry.modes.weak     { "w" } else { "" },
                    &entry.pkg_path)
            });
        }

        let mtime = bpmutil::get_mtime(entry.full_path.as_str());

        let size = if entry.file_type.is_file() {
            bpmutil::get_filesize(entry.full_path.as_str()).ok()
        } else {
            None
        };

        meta.add_file(sanitize_pathbuf(entry.pkg_path), package::FileInfo {
            filetype: entry.file_type.into(),
            hash: entry.hash,
            mtime,
            size,
            volatile: entry.modes.volatile,
        });
    }

    // unwrap all the layers of writers for the data tar file
    let unc_counter = data_tar_tar.into_inner()?;
    let uncompressed_size = unc_counter.count();
    let zstd = unc_counter.into_inner();

    let hashing_writer = zstd.finish()?;
    let compressed_size = hashing_writer.count();
    let (_comp_iter, hasher) = hashing_writer.into_parts();
    let data_tar_hash = hasher.finalize().to_hex().to_string();
    let mut data_tar_file = data_tar_bufwriter.into_inner()?;
    data_tar_file.flush()?;

    size_bar.finish_and_clear();
    count_bar.finish_and_clear();
    comp_bar.finish_and_clear();

    // fill in some more meta data and write the file
    meta.data_hash = Some(data_tar_hash.clone());
    meta.data_size = uncompressed_size;
    meta.mount = mount.cloned();
    let mut metafile = tempfile::Builder::new().prefix(&format!("{}.{}.temp", package_name, package::META_FILE_NAME)).tempfile_in(&output_dir)?;
    let path = metafile.path();
    tracing::debug!("using temp meta file at {}", path.display());
    meta.to_writer(&mut metafile)?;
    metafile.flush()?;
    let meta_data_size = metafile.stream_position().context("failed to get metadata file size")?;

    //debug: see output metafile
    //let mut b = Vec::<u8>::new();
    //meta.to_writer(&mut b);
    //println!("{}", String::from_utf8_lossy(&b));

    let finish_bar = status_mgr.add_task(Some("finish"), Some(compressed_size + meta_data_size));
    finish_bar.enable_steady_tick(Duration::from_millis(200));
    finish_bar.set_style(ProgressStyle::with_template(" {spinner:.green} writing package {wide_bar:.green/white} {bytes}/{total_bytes}").unwrap());

    // --- create a single tar package file ---
    let package_file = File::create(&package_file_path)?;
    let package_file = BufWriter::with_capacity(1024 * 1024, package_file);
    let hashing_writer = HashingWriter::new(package_file, blake3::Hasher::new());
    let mut package_tar = tar::Builder::new(hashing_writer);

    {
        let data = make_control_file().context("creating control file")?;
        let mut header = tar::Header::new_gnu();
        header.set_size(data.len() as u64);
        header.set_entry_type(tar::EntryType::Regular);
        package_tar.append_data(&mut header, "CONTROL", data.as_slice()).context("appending control file")?;
    }

    // metadata file
    let mut header = tar::Header::new_gnu();
    header.set_metadata_in_mode(
        &metafile.as_file().metadata().context("getting metadata")?,
        tar::HeaderMode::Complete
    );
    metafile.seek(std::io::SeekFrom::Start(0)).context("seeking in metadata")?;
    //package_tar.append_data(&mut header, package::META_FILE_NAME, &mut metafile)?;
    package_tar.append_data(&mut header, package::META_FILE_NAME, &mut finish_bar.wrap_read(&mut metafile))?;

    // data file
    let mut header = tar::Header::new_gnu();
    header.set_metadata_in_mode(
        &data_tar_file.as_file().metadata().context("getting metadata")?,
        tar::HeaderMode::Complete
    );
    data_tar_file.seek(std::io::SeekFrom::Start(0)).context("seeking in metadata")?;
    //package_tar.append_data(&mut header, package::DATA_FILE_NAME, &mut data_tar_file)?;
    package_tar.append_data(&mut header, package::DATA_FILE_NAME, &mut finish_bar.wrap_read(&mut data_tar_file))?;

    let hashing_writer = package_tar.into_inner()?;

    let package_size = hashing_writer.count;
    let (package_file, hasher) = hashing_writer.into_parts();
    let package_hash = hasher.finalize();
    package_file.into_inner()?.sync_all()?;

    finish_bar.finish_and_clear();

    // cleanup
    // the important files are now in the package tarball, can cleanup the intermediate ones
    if *matches.get_one::<bool>("no-cleanup").unwrap() {
        let _ = metafile.keep();
        let _ = data_tar_file.keep();
    }

    //println!("{:#?}", {let mut x = meta.clone(); x.files.clear(); x});

    //println!("package size:           {} ({}), {:0.3} %", humansize::format_size(package_size, humansize::BINARY), package_size, 100.0 * package_size as f64 / uncompressed_size as f64);
    println!("package name:           {}", package_name);
    println!("package version:        {}", package_version);
    //println!("package arch:           {}", package_arch.map_or("noarch", |s| s.as_str()));
    println!("package arch:           {}", package_arch.unwrap_or("noarch"));
    println!("package filename:       {}", package_filename);
    println!("package hash [blake3]:  {}", package_hash);
    println!("package size:           {:-10} ({})", humansize::format_size(package_size, humansize::BINARY), package_size);
    println!("data file count:        {}", file_included_count);
    println!("data size uncompressed: {:-10} ({})", humansize::format_size(uncompressed_size, humansize::BINARY), uncompressed_size);
    println!("data size compressed:   {:-10} ({})", humansize::format_size(compressed_size, humansize::BINARY), compressed_size);
    println!("data compression:       {:.4}, {:0.3} %", uncompressed_size as f64 / compressed_size as f64, 100.0 * compressed_size as f64 / uncompressed_size as f64);
    println!("data hash [blake3]:     {}", data_tar_hash);
    println!("package created at:     {}", std::fs::canonicalize(package_file_path.as_path())?.display());

    Ok(())
}

fn sanitize_path(path: &Utf8Path) -> Utf8PathBuf {
    if cfg!(windows) {
        Utf8PathBuf::from(&path.as_str().replace("\\", "/"))
    } else {
        path.to_path_buf()
    }
}

fn sanitize_pathbuf(mut path: Utf8PathBuf) -> Utf8PathBuf {
    if cfg!(windows) {
        path = sanitize_path(path.as_path());
    }
    path
}

fn canonicalize_no_symlink(path: &Utf8Path) -> Result<Utf8PathBuf> {
    if path.is_symlink() {
        return Ok(path
            .parent().context("path had no parent")?
            .canonicalize_utf8()?
            .join(path.file_name().unwrap()));
    }

    Ok(path.canonicalize_utf8()?)
}

struct HashingReader<Inner, Hasher> {
    inner: Inner,
    hasher: Hasher,
    count: u64,
}

//impl<T, H> HashingReader<T, H> {
//    fn count(&self) -> u64 {
//        self.count
//    }
//}

impl<Inner: Read, Hasher: Write> std::io::Read for HashingReader<Inner, Hasher> {
    fn read(&mut self, data: &mut [u8]) -> Result<usize, std::io::Error> {
        let ret = self.inner.read(data);
        if let Ok(n) = ret {
            self.hasher.write_all(&data[0..n])?;
            self.count += n as u64;
        }
        ret
    }
}

impl<Inner, Hasher> HashingReader<Inner, Hasher> {
    fn new(inner: Inner, hasher: Hasher) -> Self {
        Self { inner, hasher, count: 0 }
    }
    fn into_parts(self) -> (Inner, Hasher) {
        (self.inner, self.hasher)
    }
}

#[derive(Debug)]
struct CountingWriter<W: Write> {
    inner: W,
    count: u64,
}

impl<W: Write> CountingWriter<W> {
    fn new(w: W) -> Self {
        Self { inner: w, count: 0}
    }
    fn into_inner(self) -> W {
        self.inner
    }
    fn count(&self) -> u64 {
        self.count
    }
}

impl<W: Write> std::io::Write for CountingWriter<W> {
    fn write(&mut self, data: &[u8]) -> Result<usize, std::io::Error> {
        let ret = self.inner.write(data);
        if let Ok(n) = ret {
            self.count += n as u64;
        }
        ret
    }
    fn flush(&mut self) -> Result<(), std::io::Error> {
        self.inner.flush()
    }
}

//--------

#[derive(Debug)]
struct HashingWriter<Inner, Hasher> {
    inner: Inner,
    hasher: Hasher,
    count: u64,
}

impl<T, H> HashingWriter<T, H> {
    fn count(&self) -> u64 {
        self.count
    }
}

impl<Inner: Write, Hasher: Write> std::io::Write for HashingWriter<Inner, Hasher> {

    fn write(&mut self, data: &[u8]) -> Result<usize, std::io::Error> {
        let ret = self.inner.write(data);
        if let Ok(n) = ret {
            self.hasher.write_all(&data[0..n])?;
            self.count += n as u64;
        }
        ret
    }
    fn flush(&mut self) -> Result<(), std::io::Error> {
        self.inner.flush()
    }
}

impl<Inner, Hasher> HashingWriter<Inner, Hasher> {
    fn new(inner: Inner, hasher: Hasher) -> Self {
        Self {
            inner,
            hasher,
            count: 0,
        }
    }
    fn into_parts(self) -> (Inner, Hasher) {
        (self.inner, self.hasher)
    }
}

fn partial_canonicalize(path: &Utf8Path) -> Utf8PathBuf {
    for parent in path.ancestors().skip(1) {
        //println!("ancestor {} exists {:?}", parent, parent.try_exists());
        if let Ok(true) = parent.try_exists() {
            let rest = path.strip_prefix(parent);
            let parent = parent.canonicalize_utf8();
            //println!("  !! {:?}", parent);
            //println!("  rest {:?}", rest);
            if let (Ok(parent), Ok(rest)) = (parent, rest) {
                return parent.join(rest);
            }
            break;
        }
    }
    path.to_path_buf()
}

#[derive(Debug)]
enum SymlinkError {
    AbsolutePath(Utf8PathBuf, Utf8PathBuf),
    NonExistent(Utf8PathBuf, Utf8PathBuf),
    OutsidePackage(Utf8PathBuf, Utf8PathBuf),
}

impl std::fmt::Display for SymlinkError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::AbsolutePath(link, to) => {
                write!(f, "absolute path: {link} -> {to}")
            }
            Self::NonExistent(link, to) => {
                write!(f, "non-existent path: {link} -> {to}")
            }
            Self::OutsidePackage(link, to) => {
                write!(f, "links to outside of package: {link} -> {to}")
            }
        }
    }
}

#[derive(Debug, Default)]
struct SymlinkSettings {
    allow_outside: bool,
    allow_dne: bool,
}

// symlinks could:
// * point outside of the package
// * point to non-existent files
// * point to absolute paths
fn verify_symlinks(settings: &SymlinkSettings, listing: &FileListing) -> Result<()> {

    // a filter over files that removes ignored files
    let files = listing.files.iter().filter(|e| !e.ignore);

    let mut dirs = None;
    let mut contains_dir = |path: &Utf8Path| -> bool {
        if dirs.is_none() {
            dirs = Some(HashSet::new());
            for e in files.clone().filter(|e| e.is_dir()) {
                dirs.as_mut().unwrap().insert(e.full_path.clone());
            }
        }

        dirs.as_ref().unwrap().contains(path)
    };

    let errs : Vec<SymlinkError> = files.clone().filter(|e| e.is_symlink()).filter_map(|entry| {

        if let FileType::Link(to) = &entry.file_type {

            if !to.is_relative() {
                return Some(SymlinkError::AbsolutePath(entry.pkg_path.clone(), to.clone()));
            }

            let joined = entry.full_path.parent().expect("file path had no parent").join(to);
            let exists = joined.try_exists().ok().unwrap_or(false);

            if !exists && !settings.allow_dne {
                return Some(SymlinkError::NonExistent(entry.pkg_path.clone(), to.clone()));
            }

            // now check if it points to a file outside the package
            let outside = if exists {
                let joined = joined.canonicalize_utf8().expect("failed to canonicalize path");
                let outside = !files.clone().any(|e| e.full_path == joined);
                outside
            } else {
                let mut outside = || {
                    let joined = partial_canonicalize(&joined);
                    for parent in joined.ancestors().skip(1).take(to.components().count()) {
                        if contains_dir(parent) {
                            return false;
                        }
                    }
                    true
                };
                outside()
            };

            if outside && !settings.allow_outside {
                return Some(SymlinkError::OutsidePackage(entry.pkg_path.clone(), to.clone()));
            }
        }
        None
    })
    .collect();

    if !errs.is_empty() {
        for e in &errs {
            println!("Error: {e}");
        }
        anyhow::bail!("invalid symlinks")
    }

    Ok(())
}

pub fn main_cli(matches: &clap::ArgMatches) -> Result<()> {

    match matches.subcommand() {
        Some(("list-files", matches)) => {
            let file = matches.get_one::<String>("pkgfile").unwrap();
            subcmd_list_files(Path::new(file))?;
            std::process::exit(0);
        },
        Some(("verify", matches)) => {
            let file = matches.get_one::<String>("pkgfile").unwrap();
            subcmd_verify(Path::new(file))?;
            std::process::exit(0);
        },
        Some(("test-ignore", matches)) => {
            subcmd_test_ignore(matches)?;
            std::process::exit(0);
        },
        Some(("set-version", matches)) => {
            subcmd_set_version(matches)?;
            std::process::exit(0);
        },
        Some(_) => {
            unreachable!();
        }
        //Some((x, matches)) => {
            //dbg!(x);
            //dbg!(matches);
            //unreachable!();
        //}
        None => {
            make_package(matches)?;
        }
    }

    Ok(())
}

#[cfg(test)]
mod test {

    use super::*;

    /// paths should always be represented with forward slashes
    #[test]
    fn path_slashes() -> Result<()> {

        {
            let mut path = Utf8PathBuf::from("foo/bar");
            path.push("baz");
            path.push("qux");

            let parent = path.parent().unwrap();
            assert_eq!(parent, Utf8Path::new("foo/bar/baz"));

            let parent = sanitize_path(parent);
            let path = sanitize_path(&path);

            let obj = serde_json::json!{{
                "parent": parent,
                "path": path,
            }};
            let json = serde_json::to_string(&obj)?;
            assert_eq!(json, "{\"parent\":\"foo/bar/baz\",\"path\":\"foo/bar/baz/qux\"}");

        }

        {
            let mut path = Utf8PathBuf::from("foo");
            path.push("bar");

            let parent = sanitize_path(path.parent().unwrap());
            assert_eq!(parent, Utf8Path::new("foo"));

            let path = sanitize_path(&path);

            let json = serde_json::to_string(&path)?;
            assert_eq!(json, "\"foo/bar\"");

            let obj = serde_json::json!{{
                "path": path,
            }};
            let json = serde_json::to_string(&obj)?;
            assert_eq!(json, "{\"path\":\"foo/bar\"}");
        }

        Ok(())
    }
}
