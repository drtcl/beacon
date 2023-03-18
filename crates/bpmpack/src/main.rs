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
use semver::Version;
use std::collections::HashSet;
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufWriter, BufReader, Read, Write};
use std::path::Path;
use std::path::PathBuf;
use std::time::Duration;

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
    //Link(String),
}

impl From<FileType> for package::FileType {
    fn from(ft: FileType) -> Self {
        match ft {
            FileType::Dir => package::FileType::Dir,
            FileType::File => package::FileType::File,
            FileType::Link(path) => package::FileType::Link(format!("{}", path)),
            //FileType::Link(path) => package::FileType::Link{to: format!("{}", path)},
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
    pub fn is_dir(&self) -> bool {
        matches!(self.file_type, FileType::Dir)
    }
    //pub fn is_symlink(&self) -> bool {
    //    matches!(self.file_type, FileType::Link(_))
    //}
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

fn cwd() -> PathBuf {
    std::env::current_dir().expect("failed to get current dir")
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

fn main() -> Result<()> {
    let matches = args::get_args().get_matches();

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
        Some(_) => {
            unreachable!();
        }
        None => {
            main_make_package(&matches)?;
        }
    }

    Ok(())
}

/// Build a GitIgnore using a list of ignore files
fn build_ignore(paths: Vec<Utf8PathBuf>) -> Result<Option<Gitignore>> {

    if paths.is_empty() {
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
    let ignore = builder.build()?;
    Ok(Some(ignore))
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

fn subcmd_test_ignore(matches: &clap::ArgMatches) -> Result<()> {

    let given_file_paths = matches.get_many::<String>("file").unwrap().cloned().collect();

    let wrap_with_dir = matches.get_one::<String>("wrap-with-dir");
    let verbose = *matches.get_one::<bool>("verbose").unwrap();

    let ignore_files = matches.get_many::<String>("ignore-file").map_or(Vec::new(), |paths| paths.map(Utf8PathBuf::from).collect());
    let ignore = build_ignore(ignore_files)?;

    let file_list = gather_files(given_file_paths, wrap_with_dir, &ignore);

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
                    format!("({} {})", reason.file, reason.pattern)
                } else {
                    String::new()
                };
                println!("I  {}    {}", file.pkg_path, reason);
            }
            if file.is_dir() {
                ignored_parents.insert(file.pkg_path);
            }
        } else if let Some(reason) = file.ignore_reason {
            let reason = format!("({} {})", reason.file, reason.pattern);
            println!("Aw {}    {}", file.pkg_path, reason);
        } else {
            if verbose {
                println!("A  {}", file.pkg_path);
            }
        }
    }

    Ok(())
}

fn main_make_package(matches: &clap::ArgMatches) -> Result<()> {

    let mount = matches.get_one::<String>("mount").unwrap();

    // the version must be of the right semver format
    let package_version_str = matches.get_one::<String>("version").unwrap();
    let package_version = Version::parse(package_version_str).expect("invalid version");

    let package_name = matches.get_one::<String>("name").unwrap();

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

    let compress_level = *matches.get_one::<u32>("compress-level").expect("expected compression level") as i32;
    let compress_level = if 0 == compress_level { DEFAULT_ZSTD_LEVEL } else { compress_level };

    let output_dir = match matches.get_one::<String>("output-dir") {
        None => cwd(),
        Some(path) => {
            let path = PathBuf::from(path);
            if !path.exists() {
                anyhow::bail!("output-dir path does not exist");
            }
            path
        }
    };

    let given_file_paths = matches.get_many::<String>("file").unwrap().cloned().collect();

    let wrap_with_dir = matches.get_one::<String>("wrap-with-dir");
    let verbose = *matches.get_one::<bool>("verbose").unwrap();

    let ignore_files = matches.get_many::<String>("ignore-file").map_or(Vec::new(), |paths| paths.map(Utf8PathBuf::from).collect());
    let ignore = build_ignore(ignore_files)?;

    let file_list = gather_files(given_file_paths, wrap_with_dir, &ignore);

    // file names
    // - data - all target file in the package
    // - meta - meta data about data files

    let mut tarball_file_path = PathBuf::from(&output_dir);
    tarball_file_path.push(package::DATA_FILE_NAME);

    let mut meta_file_path = PathBuf::from(&output_dir);
    meta_file_path.push(package::META_FILE_NAME);

    let mut package_file_path = PathBuf::from(&output_dir);
    package_file_path.push(format!("{}-{}{}", package_name, package_version, package::DOTTED_PKG_FILE_EXTENSION));

    // scan the file list and verify symlinks
    //verify_symlinks(&file_list);

    let data_tar_file = BufWriter::with_capacity(1024 * 1024, File::create(tarball_file_path.as_path())?);
    let mut data_tar_hasher = HashingWriter::new(data_tar_file, blake3::Hasher::new());
    let mut data_tar_file = zstd::stream::write::Encoder::new(&mut data_tar_hasher, compress_level)?;

    #[cfg(feature="mt")]
    data_tar_file.multithread(get_threads())?;

    let mut data_tar_file = data_tar_file.auto_finish();
    let mut data_tar = tar::Builder::new(&mut data_tar_file);
    data_tar.follow_symlinks(false);

    let mut meta = package::MetaData::new(package::PackageID {
        name: package_name.clone(),
        version: format!("{}", package_version),
    });
    // for entry in file_list.files {
    //     meta.add_file(format!("{}", entry.pkg_path.display()), package::FileInfo {
    //         hash: entry.hash,
    //         filetype: entry.file_type.into(),
    //     });
    // }
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
                data_tar.append_dir(&entry.pkg_path, &entry.full_path).context("inserting dir")?;
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

                data_tar.append_data(&mut header, &entry.pkg_path, &mut reader).context("inserting file")?;
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
                data_tar.append_link(&mut header, &entry.pkg_path, link_path).context("inserting symlink")?;

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

    data_tar.finish()?;
    drop(data_tar);
    data_tar_file.flush()?;
    drop(data_tar_file);

    pb.finish_and_clear();

    let (_, hasher) = data_tar_hasher.into_parts();
    //let data_tar_hash = hex_string(hasher.finalize().as_slice());
    let data_tar_hash = hasher.finalize().to_hex().to_string();
    meta.data_hash = Some(data_tar_hash);
    meta.mount = Some(mount.clone());

    {
        let mut metafile = BufWriter::new(File::create(&meta_file_path)?);
        meta.to_writer(&mut metafile)?;
    }

    // --- create a single tar package file ---
    let package_file = File::create(&package_file_path)?;
    let package_file = BufWriter::with_capacity(1024 * 1024, package_file);
    let mut hasher = HashingWriter {
        hasher: blake3::Hasher::new(),
        inner: package_file,
    };
    let mut package_tar = tar::Builder::new(&mut hasher);

    package_tar.append_path_with_name(&meta_file_path, package::META_FILE_NAME)?;
    package_tar.append_path_with_name(&tarball_file_path, package::DATA_FILE_NAME)?;
    package_tar.finish()?;
    drop(package_tar);

    hasher.flush()?;
    let (package_file, hasher) = hasher.into_parts();
    println!("package hash (blake3): {}", hasher.finalize());

    package_file.into_inner()?.sync_all()?;

    // cleanup
    // the important files are now in the package tarball, can cleanup the intermediate ones
    if !matches.get_one::<bool>("no-cleanup").unwrap() {
        std::fs::remove_file(meta_file_path.as_path())?;
        std::fs::remove_file(tarball_file_path.as_path())?;
    }

    println!(
        "Package created at {}",
        std::fs::canonicalize(package_file_path.as_path())?.display()
    );

    Ok(())
}

fn canonicalize_no_symlink(path: &Utf8Path) -> Result<Utf8PathBuf> {
    if path.is_symlink() {
        return Ok(path
            .parent()
            .unwrap()
            .canonicalize_utf8()?
            .join(path.file_name().unwrap()));
    }

    Ok(path.canonicalize_utf8()?)
}


struct HashingReader<Inner, Hasher> {
    inner: Inner,
    hasher: Hasher,
}

impl<Inner: Read, Hasher: Write> std::io::Read for HashingReader<Inner, Hasher> {
    fn read(&mut self, data: &mut [u8]) -> Result<usize, std::io::Error> {
        let ret = self.inner.read(data);
        if let Ok(n) = ret {
            self.hasher.write_all(&data[0..n])?;
        }
        ret
    }
}

impl<Inner, Hasher> HashingReader<Inner, Hasher> {
    fn new(inner: Inner, hasher: Hasher) -> Self {
        Self { inner, hasher }
    }
    fn into_parts(self) -> (Inner, Hasher) {
        (self.inner, self.hasher)
    }
}

struct HashingWriter<Inner, Hasher> {
    inner: Inner,
    hasher: Hasher,
}

impl<Inner: Write, Hasher: Write> std::io::Write for HashingWriter<Inner, Hasher> {
    fn write(&mut self, data: &[u8]) -> Result<usize, std::io::Error> {
        let ret = self.inner.write(data);
        if let Ok(n) = ret {
            self.hasher.write_all(&data[0..n])?;
        }
        ret
    }
    fn flush(&mut self) -> Result<(), std::io::Error> {
        self.inner.flush()
    }
}

impl<Inner, Hasher> HashingWriter<Inner, Hasher> {
    fn new(inner: Inner, hasher: Hasher) -> Self {
        Self { inner, hasher }
    }
    fn into_parts(self) -> (Inner, Hasher) {
        (self.inner, self.hasher)
    }
}

//fn normalize_path(path: PathBuf) -> PathBuf {
//    let mut ret = PathBuf::new();
//    for i in path.iter() {
//        dbg!(&i);
//        if i == ".." {
//            ret.pop();
//        } else if i == "." {
//        } else {
//            ret.push(i);
//        }
//    }
//    dbg!(&ret);
//    ret
//}

//fn verify_symlinks(FileListing { files }: &FileListing) -> Result<()> {
//    dbg!(files);
//
//    for file_entry in files {
//        if let FileType::Link(ref link_content) = file_entry.file_type {
//            let link_pkg_path = if link_content.is_relative() {
//                file_entry
//                    .pkg_path
//                    .parent()
//                    .unwrap()
//                    .join(link_content)
//                    .canonicalize()?
//            } else {
//                link_content
//                    .canonicalize()
//                    .context("failed to canonicalize link path")?
//            };
//
//            println!(
//                "LINK {:?} [{:?}] -> {:?}",
//                file_entry.pkg_path, link_content, link_pkg_path
//            );
//            //let link_pkg_path = normalize_path(link_pkg_path);
//            //println!("?LINK {:?} [{:?}] -> {:?}", file_entry.pkg_path, link_content, link_pkg_path);
//
//            let iter = files.iter().find(|v| {
//                println!("checking {:?} == {:?}", link_content, v);
//                &v.pkg_path == &link_pkg_path
//            });
//
//            if iter.is_none() {
//                println!(
//                    "WARNING: symlink {:?} -> {:?} points outsize of package",
//                    file_entry.pkg_path, link_pkg_path
//                );
//            }
//        }
//    }
//    Ok(())
//}
