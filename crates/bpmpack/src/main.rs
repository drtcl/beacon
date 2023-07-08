//TODO
// - should refuse to overwrite existing package files
// - use semver for the version number, validate it?
// - dependencies
// - package recipies
//   - ignore files
//   - package descriptions
//   - README / docs for packages

//#![allow(dead_code)]
//#![allow(unused_imports)]
//#![allow(unused_variables)]

use anyhow::{Context, Result};
use camino::Utf8Path;
use camino::Utf8PathBuf;
use indicatif::ProgressBar;
//use semver::Version;
use std::collections::HashSet;
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufWriter, BufReader, Read, Write};
use std::path::Path;
use std::path::PathBuf;
use std::time::Duration;
use version::Version;

use std::io::BufRead;
use std::io::Seek;

use ignore::gitignore::Gitignore;

mod args;

const DEFAULT_ZSTD_LEVEL : i32 = 10;

fn get_threads() -> u32 {
    let t = std::thread::available_parallelism().map_or(1, |v| v.get() as u32);

    //println!("using {t} threads");
    std::cmp::min(20, t)
}

// take a list of files
// save them into a data tarball
// hash the tarball
// get the hash for each file
// save hashes into a meta file
// tar the hashes and data tarball into a single package tarball
// cleanup intermediate files
//

//TODO given:
//     a/
//       b/
//         c/
//           file.txt
//  and an ignore file of:
//  a/*
//  !a/b/c/file.txt
//
//  does this parent dirs: a, a/b, and a/b/c, need to be white listed and added to the data tar?

#[derive(Clone, Debug)]
enum FileType {
    Dir,
    File,
    Link(Utf8PathBuf),
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

//impl FileListing {
//    fn new() -> Self {
//        FileListing { files: Vec::new() }
//    }
//    fn extend(&mut self, other: Self) {
//        self.files.extend(other.files.into_iter());
//    }
//}

fn cwd() -> PathBuf {
    std::env::current_dir().expect("failed to get current dir")
}

fn main() -> Result<()> {

    let cli = args::get_args();
    let matches = cli.get_matches();

    match matches.subcommand() {
        Some(("list", matches)) => {
            let file = matches.get_one::<String>("pkgfile").unwrap();
            subcmd_list_files(Path::new(file))?;
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
        None => {
            make_package(&matches)?;
        }
    }

    Ok(())
}

/// Build a GitIgnore using a list of ignore files
fn build_ignore(paths: Vec<Utf8PathBuf>, patterns: Vec<String>) -> Result<Option<Gitignore>> {

    if paths.is_empty() && patterns.is_empty() {
        return Ok(None);
    }

    let mut builder = ignore::gitignore::GitignoreBuilder::new(".");

    for ignore_file in paths {
        let ignore_file = Path::new(&ignore_file);
        if !ignore_file.exists() {
            anyhow::bail!("ignore-file does not exist");
        }

        let read = std::io::BufReader::new(File::open(ignore_file)?);
        let mut line_num = 1;
        for line in read.lines() {
            let mut from = ignore_file.to_path_buf();
            from.set_file_name(format!("{}:{}", from.file_name().unwrap().to_str().unwrap(), line_num));
            builder.add_line(Some(from), &line?)?;
            line_num += 1;
        }

    }

    for pattern in patterns {
        builder.add_line(None, &pattern)?;
    }

    let ignore = builder.build()?;
    Ok(Some(ignore))
}

/// Walk the filesystem, discovering all files from the given paths
fn file_discovery(paths: Vec<String>) -> FileListing {

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
        //dbg!(&entry);
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
        });
    }

    FileListing { files }
}

/// Calls file_discovery(), then marks ignored files,
/// and optionally adds a wrapper root directory to all files
fn gather_files(paths: Vec<String>, wrap_dir: Option<&String>, ignore: &Option<Gitignore>) -> FileListing {

    let mut file_list = file_discovery(paths);

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

    // add the wrap dir
    if let Some(wrap_dir) = wrap_dir {
        for ent in file_list.files.iter_mut() {
            let mut p = Utf8PathBuf::from(wrap_dir);
            p.push(&ent.pkg_path);
            ent.pkg_path = p;
            //let path = std::mem::replace(&mut ent.pkg_path, PathBuf::new());
        }
    }

    file_list
}

fn subcmd_set_version(matches: &clap::ArgMatches) -> Result<()> {

    let require_semver = *matches.get_one::<bool>("semver").unwrap();
    let package_version = Version::new(matches.get_one::<String>("version").unwrap());
    let package_filepath = matches.get_one::<String>("pkgfile").unwrap();
    let package_filepath = Utf8PathBuf::from_path_buf(PathBuf::from(package_filepath)).expect("failed to get file path");
    let package_filename = package_filepath.file_name().context("failed to get filename")?;

    if !package::is_version_string(&package_version) {
        anyhow::bail!("invalid version")
    }
    if require_semver && !package_version.is_semver() {
        anyhow::bail!("Version is not a valid semver. A valid semver is required because the --semver option was used")
    }

    if !package_filepath.try_exists().unwrap_or(false) {
        anyhow::bail!("no such file");
    }

    if let Some((name, _version)) = package::split_parts(package_filename) {

        //if _version != "unversioned" {
            //anyhow::bail!("refusing to set the version of an already versioned package");
        //}

        let new_filename = package::make_packagefile_name(name, &package_version);
        let new_filepath = package_filepath.with_file_name(&new_filename);

        let out_file = std::fs::File::create(&new_filepath).context("failed to open file for writing")?;
        let mut out_tar = tar::Builder::new(out_file);

        let in_file = std::fs::File::open(package_filepath).context("failed to open file for reading")?;
        let mut tar = tar::Archive::new(in_file);

        for mut entry in tar.entries().context("failed to read tar")?.flatten() {
            let path = entry.path()?.into_owned();

            let path_str = path.to_str().context("failed to stringify path")?;
            if path_str == package::META_FILE_NAME {

                // extract the MetaData struct
                let mut md = package::MetaData::from_reader(&mut entry).context("failed to extra metadata")?;

                // update the version
                md.version = package_version.to_string();

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
    }

    Ok(())
}

fn subcmd_test_ignore(matches: &clap::ArgMatches) -> Result<()> {

    let wrap_with_dir = matches.get_one::<String>("wrap-with-dir");
    let verbose = *matches.get_one::<bool>("verbose").unwrap();

    let patterns = matches.get_many::<String>("pattern").map_or(Vec::new(), |patterns| patterns.map(String::from).collect());
    let ignore_files = matches.get_many::<String>("ignore-file").map_or(Vec::new(), |paths| paths.map(Utf8PathBuf::from).collect());
    let ignore = build_ignore(ignore_files, patterns)?;

    let given_file_paths = matches.get_many::<String>("file").unwrap().cloned().collect();
    let file_list = gather_files(given_file_paths, wrap_with_dir, &ignore);

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
                let reason = if let Some(reason) = &file.ignore_reason {
                    format!("({}:{})", reason.file, reason.pattern)
                } else {
                    String::new()
                };
                //println!("I  {}    {}", file.pkg_path, reason);
                writeln!(&mut tw, "I   {}\t{}", file.pkg_path, reason)?;
            }
            if file.is_dir() {
                ignored_parents.insert(file.pkg_path);
            }
        } else if let Some(reason) = file.ignore_reason {
            let reason = format!("({} {})", reason.file, reason.pattern);
            //println!("Aw {}    {}", file.pkg_path, reason);
            writeln!(&mut tw, "Aw  {}\t{}", file.pkg_path, reason)?;
        } else if verbose {
            //println!("A  {}", file.pkg_path);
            writeln!(&mut tw, "A   {}\t", file.pkg_path)?;
        }
    }

    tw.flush()?;

    Ok(())
}

fn subcmd_list_files(file: &Path) -> Result<()> {

    let mut file = std::fs::File::open(file).context("failed to open package file")?;

    package::package_integrity_check(&mut file)?;

    file.rewind()?;

    let mut tar = tar::Archive::new(file);
    let data = package::seek_to_tar_entry(package::DATA_FILE_NAME, &mut tar)?;
    let zstd = zstd::Decoder::new(data)?;
    let mut tar = tar::Archive::new(zstd);
    for ent in tar.entries()? {
        let e = ent?;
        let path = e.path()?;
        println!("{}", path.display());
    }
    Ok(())
}

fn make_package(matches: &clap::ArgMatches) -> Result<()> {

    let wrap_with_dir = matches.get_one::<String>("wrap-with-dir");
    let verbose = *matches.get_one::<bool>("verbose").unwrap();
    let mount = matches.get_one::<String>("mount").unwrap();
    let require_semver = *matches.get_one::<bool>("semver").unwrap();

    let compress_level = *matches.get_one::<u32>("compress-level").expect("expected compression level") as i32;
    let compress_level = if 0 == compress_level { DEFAULT_ZSTD_LEVEL } else { compress_level };

    let deps: Vec<(String, Option<String>)> = matches.get_many::<String>("depend")
        .map(|refs| refs.into_iter().map(|s| s.to_string()).collect())
        .unwrap_or(Vec::new())
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

    let ignore_files = matches.get_many::<String>("ignore-file").map_or(Vec::new(), |paths| paths.map(Utf8PathBuf::from).collect());
    let ignore = build_ignore(ignore_files, Vec::new())?;

    let package_name = matches.get_one::<String>("name").unwrap();
    if !package::is_package_name(package_name) {
        anyhow::bail!("invalid package name. (must match [a-zA-Z][a-zA-Z0-9\\-]*)")
    }

    let unversioned = *matches.get_one::<bool>("unversioned").unwrap();

    // the version must be of the right semver format
    let package_version = if unversioned {
        Version::new("unversioned")
    } else {
        let package_version = Version::new(matches.get_one::<String>("version").unwrap());
        if !package::is_version_string(&package_version) {
            anyhow::bail!("invalid version")
        }
        if require_semver && !package_version.is_semver() {
            anyhow::bail!("Version is not a valid semver. A valid semver is required because the --semver option was used")
        }
        package_version
    };

    let given_file_paths = matches.get_many::<String>("file").unwrap().cloned().collect();
    let file_list = gather_files(given_file_paths, wrap_with_dir, &ignore);

    // file names
    // - data - all target file in the package
    // - meta - meta data about data files

    let mut tarball_file_path = PathBuf::from(&output_dir);
    tarball_file_path.push(package::DATA_FILE_NAME);

    let mut meta_file_path = PathBuf::from(&output_dir);
    meta_file_path.push(package::META_FILE_NAME);

    let mut package_file_path = PathBuf::from(&output_dir);
    package_file_path.push(package::make_packagefile_name(package_name, package_version.as_str()));

    // scan the file list and verify symlinks
    let mut symlink_settings = SymlinkSettings::default();
    symlink_settings.allow_dne = *matches.get_one::<bool>("allow-symlink-dne").unwrap();
    symlink_settings.allow_outside = *matches.get_one::<bool>("allow-symlink-outside").unwrap();
    verify_symlinks(&symlink_settings, &file_list)?;

    // layers of wrapping:
    // 1. raw file
    // 2. BufWriter
    // 3. CountingWriter (for compressed size)
    // 4. HashingWriter
    // 5. zstd compressor
    // 6. CountingWriter (for uncompressed size)
    // 7. tar builder

    let data_tar_file = File::create(tarball_file_path.as_path())?;
    let data_tar_bufwriter = BufWriter::with_capacity(1024 * 1024, data_tar_file);
    let data_tar_compressed_size_writer = CountingWriter::new(data_tar_bufwriter);
    let data_tar_hasher = HashingWriter::new(data_tar_compressed_size_writer, blake3::Hasher::new());
    let mut data_tar_zstd = zstd::stream::write::Encoder::new(data_tar_hasher, compress_level)?;

    #[cfg(feature="mt")]
    data_tar_zstd.multithread(get_threads())?;

    let data_tar_uncompressed_size_writer = CountingWriter::new(data_tar_zstd);

    let mut data_tar_tar = tar::Builder::new(data_tar_uncompressed_size_writer);
    data_tar_tar.follow_symlinks(false);

    let mut meta = package::MetaData::new(package::PackageID {
        name: package_name.clone(),
        version: format!("{}", package_version),
    });
    for pair in &deps {
        meta.add_dependency(package::DependencyID{
            name: pair.0.clone(),
            //version: pair.1.clone(),
            version: Some(pair.1.as_ref().map_or("*".to_string(), |v| v.clone())),
        });
    }

    //TODO subtract ignore_count?
    let pb = ProgressBar::new(file_list.files.len() as u64);
    pb.set_style(
        indicatif::ProgressStyle::with_template(
            "{spinner} {wide_bar:.blue/white} {pos}/{len} {elapsed} {eta}",
        )
        .unwrap()
        //.progress_chars("█▇▆▅▄▃▂▁ █")
    );
    pb.enable_steady_tick(Duration::from_millis(200));

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
                    pb.suspend(|| println!("I \t{}", entry.pkg_path));
                }

                if entry.is_dir() {
                    ignored_parents.insert(entry.pkg_path);
                }
            }
            pb.inc(1);
            continue;
        }

        match entry.file_type {
            FileType::Dir => {
                data_tar_tar.append_dir(&entry.pkg_path, &entry.full_path).context("inserting dir")?;
                if verbose {
                    //println!("Ad\t{}/", entry.pkg_path.display());
                    pb.suspend(|| println!("Ad\t{}/", entry.pkg_path));
                }
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

                if verbose {
                    pb.suspend(|| {
                        println!(
                            //"A\t{:-10}\t{}",
                            "A\t{:-10}",
                            entry.pkg_path,
                            //hex_string(hash.as_slice())
                        )
                    });
                    //println!("A\t{:-10}\t{}", entry.pkg_path.display(), hex_string(hash.as_slice()));
                    //println!("A\t{}", entry.pkg_path.display());
                }
            }
            FileType::Link(ref link_path) => {
                //dbg!(&entry);
                let mut header = tar::Header::new_gnu();
                header.set_entry_type(tar::EntryType::Symlink);
                header.set_size(0);
                data_tar_tar.append_link(&mut header, &entry.pkg_path, link_path).context("inserting symlink")?;

                let mut hasher = blake3::Hasher::new();
                hasher.update(link_path.as_str().as_bytes());
                let hash = hasher.finalize().to_hex().to_string();
                entry.hash = Some(hash);

                if verbose {
                    pb.suspend(|| println!("As\t{}/", entry.pkg_path));
                    //println!("A l\t{}/", entry.pkg_path.display());
                }
            }
        }

        meta.add_file(entry.pkg_path, package::FileInfo {
            hash: entry.hash,
            filetype: entry.file_type.into(),
        });

        pb.inc(1);
    }

    // unwrap all the layers of writers for the data tar file
    let unc_counter = data_tar_tar.into_inner()?;
    let uncompressed_size = unc_counter.count;
    let zstd = unc_counter.into_inner();
    let hasher = zstd.finish()?;
    let (comp_counter, hasher) = hasher.into_parts();
    let compressed_size = comp_counter.count;
    let data_tar_hash = hasher.finalize().to_hex().to_string();
    let bufwriter =  comp_counter.into_inner();
    let mut file = bufwriter.into_inner()?;
    file.flush()?;
    drop(file);

    pb.finish_and_clear();

    // fill in some more meta data and write the file
    meta.data_hash = Some(data_tar_hash.clone());
    meta.mount = Some(mount.clone());
    {
        let mut metafile = BufWriter::new(File::create(&meta_file_path)?);
        meta.to_writer(&mut metafile)?;
    }

    // --- create a single tar package file ---
    let package_file = File::create(&package_file_path)?;
    let package_file = BufWriter::with_capacity(1024 * 1024, package_file);
    let hashing_writer = HashingWriter::new(package_file, blake3::Hasher::new());
    let mut package_tar = tar::Builder::new(hashing_writer);

    package_tar.append_path_with_name(&meta_file_path, package::META_FILE_NAME)?;
    package_tar.append_path_with_name(&tarball_file_path, package::DATA_FILE_NAME)?;
    let hashing_writer = package_tar.into_inner()?;

    let package_size = hashing_writer.count;
    let (package_file, hasher) = hashing_writer.into_parts();
    let package_hash = hasher.finalize();
    package_file.into_inner()?.sync_all()?;

    // cleanup
    // the important files are now in the package tarball, can cleanup the intermediate ones
    if !matches.get_one::<bool>("no-cleanup").unwrap() {
        std::fs::remove_file(meta_file_path.as_path())?;
        std::fs::remove_file(tarball_file_path.as_path())?;
    }

    println!("data size uncompressed: {} ({})", humansize::format_size(uncompressed_size, humansize::BINARY), uncompressed_size);
    println!("data size compressed:   {} ({})", humansize::format_size(compressed_size, humansize::BINARY), compressed_size);
    println!("data compression:       {:.4}, {:0.3} %", uncompressed_size as f64 / compressed_size as f64, 100.0 * compressed_size as f64 / uncompressed_size as f64);
    println!("package size:           {} ({}), {:0.3} %", humansize::format_size(package_size, humansize::BINARY), package_size, 100.0 * package_size as f64 / uncompressed_size as f64);
    println!("data hash [blake3]:     {}", data_tar_hash);
    println!("package hash [blake3]:  {}", package_hash);
    println!("Package created at {}", std::fs::canonicalize(package_file_path.as_path())?.display());

    Ok(())
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

#[derive(Debug)]
struct HashingWriter<Inner, Hasher> {
    inner: Inner,
    hasher: Hasher,
    count: u64,
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
