//TODO
// - should refuse to overwrite existing package files
// - use semver for the version number, validate it?
// - dependencies
// - package recipies
//   - ignore files
//   - package descriptions
//   - README / docs for packages

#![allow(dead_code)]
#![allow(unused_imports)]
#![allow(unused_variables)]

use anyhow::{Context, Result};
use blake2::{Blake2b, Digest};
use clap::{Arg, ArgAction};
use indicatif::ProgressBar;
use semver::Version;
use std::io::{BufWriter, BufReader, BufRead, Read, Write};
use std::path::{Path, PathBuf};
use std::time::Duration;

const ZSTD_LEVEL : i32 = 15;

fn get_threads() -> u32 {
    let t = std::thread::available_parallelism().map_or(1, |v| v.get() as u32);
    let t = std::cmp::min(20, t);
    //println!("using {t} threads");
    t
}

// take a list of files
// save them into a data tarball
// hash the tarball
// get the hash for each file
// save hashes into a meta file
// tar the hashes and data tarball into a single package tarball
// cleanup intermediate files
//

#[derive(Clone, Debug)]
enum FileType {
    Dir,
    File,
    Link(PathBuf),
    //Link(String),
}

impl From<FileType> for package::FileType {
    fn from(ft: FileType) -> Self {
        match ft {
            FileType::Dir => package::FileType::Dir,
            FileType::File => package::FileType::File,
            FileType::Link(path) => package::FileType::Link(format!("{}", path.display())),
        }
    }
}

#[derive(Clone, Debug)]
struct FileEntry {
    full_path: PathBuf,
    pkg_path: PathBuf,
    file_type: FileType,
    hash: Option<String>,
}

#[derive(Clone, Debug)]
struct FileListing {
    files: Vec<FileEntry>,
}

impl FileListing {
    fn new() -> Self {
        FileListing { files: Vec::new() }
    }
    fn extend(&mut self, other: Self) {
        self.files.extend(other.files.into_iter());
    }
}

fn file_discovery(paths: Vec<String>) -> FileListing {

    let mut walker = ignore::WalkBuilder::new(paths.first().unwrap());
    for path in paths.iter().skip(1) {
        walker.add(path);
    }

    walker.standard_filters(false);
    walker.git_ignore(false);
    walker.git_ignore(true);

    let mut files = Vec::new();

    let root = PathBuf::from(paths.first().unwrap());
    let root_parent = root.parent().unwrap();

    for entry in walker.build() {
        let entry = entry.unwrap();
        //dbg!(&entry);
        let full_path = PathBuf::from(entry.path());
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
        });
    }

    FileListing { files }
}

fn cwd() -> PathBuf {
    std::env::current_dir().expect("failed to get current dir")
}

fn main() -> Result<()> {
    let matches = clap::Command::new("bpm-pack")
        .version("0.1.0")
        .about("Bryan's Package Manager : bpm-pack : package creation utility")
        .author("Bryan Splitgerber")
        .disable_version_flag(true)
        .arg(
            Arg::new("no-cleanup")
                .long("no-cleanup")
                .action(ArgAction::SetTrue),
        )
        .arg(
            Arg::new("name")
                .short('n')
                .long("name")
                .required(true)
                .help("The name of the package"),
        )
        .arg(
            Arg::new("version")
                .long("version")
                .required(true)
                .help("The pacakges's version"),
        )
        .arg(
            Arg::new("verbose")
                .long("verbose")
                .required(false)
                .action(ArgAction::SetTrue), //.default_value("0")
                                             //.help("verbosity level"), //.validator(is_int)
        )
        .arg(
            Arg::new("ignore-file")
                .long("ignore-file")
                .value_hint(clap::ValueHint::FilePath)
                .value_name("path")
                .required(false), //.default_value("")
        )
        .arg(Arg::new("mount").long("mount").required(true))
        .arg(
            Arg::new("output-dir")
                .long("output-dir")
                .short('o')
                .value_name("dir")
                .required(false), //.default_value("`cwd`")
        )
        .arg(
            Arg::new("depend")
                .long("depend")
                .action(clap::ArgAction::Append)
                .value_name("pkg[@version]")
                .help("Add a dependency"),
        )
        .arg(
            Arg::new("file")
                .action(clap::ArgAction::Append)
                .required(true),
        )
        .get_matches();

    let mount = matches.get_one::<String>("mount").unwrap();

    // the version must be of the right semver format
    let package_version_str = matches.get_one::<String>("version").unwrap();
    let package_version = Version::parse(package_version_str).expect("invalid version");

    let package_name = matches.get_one::<String>("name").unwrap();

    let verbose = *matches.get_one::<bool>("verbose").unwrap();

    //let root_dir = matches.get_one::<String>("root").unwrap();
    //let root_dir = std::fs::canonicalize(root_dir).expect("failed to canonicalize root dir");
    //dbg!(&root_dir);

//    // create the cache directory if it doesn't exist
//    let cache_dir = Path::new("./bpm_cache/");
//    if !cache_dir.exists() {
//        std::fs::create_dir(cache_dir)?; // error: failed to create cache directory
//    }

//    let ignore_file = matches.get_one::<String>("ignore-file");
//    if let Some(ignore_file) = ignore_file {
//        let ignore_file = Path::new(ignore_file);
//        if !ignore_file.exists() {
//            anyhow::bail!("ignore-file does not exist");
//        }
//        //let ig = ignore::gitignore::GitignoreBuilder::new(
//        //let read = std::io::BufReader::new(std::fs::File::open(ignore_file)?);
//        //for line in read.lines() {
//        //    let line = line?;
//        //    //println!("read line {line}");
//        //}
//    }

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

    // the file names
    // - data - all target file in the package
    // - meta - meta data about data files

    let mut tarball_file_path = PathBuf::new();
    tarball_file_path.push(&output_dir);
    tarball_file_path.push("data.tar.zst");

    let mut meta_file_path = PathBuf::new();
    meta_file_path.push(&output_dir);
    meta_file_path.push(package::META_FILE_NAME);

    let mut package_file_path = PathBuf::new();
    package_file_path.push(&output_dir);
    package_file_path.push(format!("{}-{}.bpm.tar", package_name, package_version));

    //    let mut file_listing = FileListing::new();
    //    for path in matches.get_many::<String>("file").unwrap() {
    //        file_listing.extend(file_discovery(&PathBuf::from(path)));
    //    }

    // -- begin work --

    let file_listing = file_discovery(
        matches
            .get_many::<String>("file")
            .unwrap()
            .cloned()
            .collect(),
    );
    //dbg!(&file_listing);

    // scan the file list and verify symlinks
    //verify_symlinks(&file_listing);

    let data_tar_file = BufWriter::new(std::fs::File::create(tarball_file_path.as_path())?);
    let mut data_tar_hasher = HashingWriter::new(data_tar_file, Blake2b::new());
    let mut data_tar_file =
        zstd::stream::write::Encoder::new(&mut data_tar_hasher, ZSTD_LEVEL)?;

    #[cfg(feature="mt")]
    data_tar_file.multithread(get_threads())?;

    let mut data_tar_file = data_tar_file.auto_finish();
    let mut data_tar = tar::Builder::new(&mut data_tar_file);
    data_tar.follow_symlinks(false);

    let deps: Vec<(String, String)> = Vec::new();
    //let deps = vec![("foo", "3.1.4"), ("honk", "4.0.1")];

    let mut meta = package::MetaData::new(package::PackageID {
        name: package_name.clone(),
        version: format!("{}", package_version),
    });
   // for entry in file_listing.files {
   //     meta.add_file(format!("{}", entry.pkg_path.display()), package::FileInfo {
   //         hash: entry.hash,
   //         filetype: entry.file_type.into(),
   //     });
   // }
    for pair in &deps {
        meta.add_dependency(package::PackageID {
            name: pair.0.clone(),
            version: pair.1.clone(),
        });
    }

    let pb = ProgressBar::new(file_listing.files.len() as u64);
    pb.set_style(
        indicatif::ProgressStyle::with_template(
            "{spinner} {wide_bar:.blue/white} {pos}/{len} {elapsed} {eta}",
        )
        //.progress_chars("█▇▆▅▄▃▂▁ █")
        .unwrap(),
    );
    pb.enable_steady_tick(Duration::from_millis(200));

    let slow = file_listing.files.len() < 20;
    let slow = false;

    // create the target install files tar file
    for mut entry in file_listing.files {
        match entry.file_type {
            FileType::Dir => {
                data_tar.append_dir(&entry.pkg_path, &entry.full_path).context("inserting dir")?;
                if verbose {
                    //println!("Ad\t{}/", entry.pkg_path.display());
                    pb.suspend(|| println!("Ad\t{}/", entry.pkg_path.display()));
                }
            }
            FileType::File => {
                let fd = std::fs::File::open(&entry.full_path).context("opening file")?;
                //let file_size = fd.metadata()?.len();

                let mut header = tar::Header::new_gnu();
                header.set_metadata_in_mode(
                    &fd.metadata().context("getting metadata")?,
                    tar::HeaderMode::Complete,
                );
                //header.set_size(file_size);
                //header.set_cksum();

                //let fd = BufReader::new(fd);
                let mut reader = HashingReader::new(BufReader::new(fd), Blake2b::new());

                data_tar.append_data(&mut header, &entry.pkg_path, &mut reader).context("inserting file")?;
                let (_, hasher) = reader.into_parts();
                let hash = hasher.finalize();
                entry.hash = Some(hex_string(hash.as_slice()));

                if verbose {
                    pb.suspend(|| {
                        println!(
                            //"A\t{:-10}\t{}",
                            "A\t{:-10}",
                            entry.pkg_path.display(),
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

                let mut hasher = Blake2b::new();
                hasher.update(link_path.to_string_lossy().as_bytes());
                let hash = hex_string(hasher.finalize().as_slice());
                entry.hash = Some(hash);

                if verbose {
                    pb.suspend(|| println!("As\t{}/", entry.pkg_path.display()));
                    //println!("A l\t{}/", entry.pkg_path.display());
                }
            }
        }

        meta.add_file(format!("{}", entry.pkg_path.display()), package::FileInfo {
            hash: entry.hash,
            filetype: entry.file_type.into(),
        });

        pb.inc(1);
        if slow {
            std::thread::sleep(Duration::from_millis(50));
        } //TODO dd
    }

    data_tar.finish()?;
    drop(data_tar);
    data_tar_file.flush()?;
    drop(data_tar_file);

    pb.finish_and_clear();

    let (_, hasher) = data_tar_hasher.into_parts();
    let data_tar_hash = hex_string(hasher.finalize().as_slice());
    meta.data_hash = Some(data_tar_hash);
    meta.mount = Some(mount.clone());

    {
        let mut metafile = std::io::BufWriter::new(std::fs::File::create(&meta_file_path)?);
        meta.to_writer(&mut metafile)?;
    }

    // --- create a single tar package file ---
    let pkg_file = std::fs::File::create(&package_file_path)?;
    let pkg_file = BufWriter::new(pkg_file);
    let mut hasher = HashingWriter {
        hasher: Blake2b::new(),
        inner: pkg_file,
    };
    let mut pkg_file = tar::Builder::new(&mut hasher);

    pkg_file.append_path_with_name(&meta_file_path, package::META_FILE_NAME)?;
    pkg_file.append_path_with_name(&tarball_file_path, "data.tar.zst")?;
    pkg_file.finish()?;
    drop(pkg_file);

    //let final_tar_hash = hex_string(hasher.hasher.finalize().as_slice());
    //println!("tar hash {:?}", final_tar_hash);

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

fn hex_string(data: &[u8]) -> String {
    let mut s = String::new();
    for byte in data {
        s += &format!("{:02x}", byte);
    }
    s
}

struct HashingReader<Inner, Hasher> {
    hasher: Hasher,
    inner: Inner,
}

impl<Inner: Read, Hasher: Digest> std::io::Read for HashingReader<Inner, Hasher> {
    fn read(&mut self, data: &mut [u8]) -> Result<usize, std::io::Error> {
        let ret = self.inner.read(data);
        if let Ok(n) = ret {
            self.hasher.update(&data[0..n]);
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
    hasher: Hasher,
    inner: Inner,
}

impl<Inner: Write, Hasher: Digest> std::io::Write for HashingWriter<Inner, Hasher> {
    fn write(&mut self, data: &[u8]) -> Result<usize, std::io::Error> {
        let ret = self.inner.write(data);
        if let Ok(n) = ret {
            self.hasher.update(&data[0..n]);
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

fn normalize_path(path: PathBuf) -> PathBuf {
    let mut ret = PathBuf::new();
    for i in path.iter() {
        dbg!(&i);
        if i == ".." {
            ret.pop();
        } else if i == "." {
        } else {
            ret.push(i);
        }
    }
    dbg!(&ret);
    ret
}

fn canonicalize_no_symlink(path: &Path) -> Result<PathBuf> {
    if path.is_symlink() {
        return Ok(path
            .parent()
            .unwrap()
            .canonicalize()?
            .join(path.file_name().unwrap()));
    }

    Ok(path.canonicalize()?)
}

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

//    // --- write toml data ---
//    {
//        // now create a file that lists all included file paths and their hash
//        let metafile = std::fs::File::create(&meta_file_path)?;
//        let mut metafile = BufWriter::new(metafile);
//
//        let mut map = toml::map::Map::new();
//
//        let mut package_table = toml::map::Map::new();
//        package_table.insert(
//            "name".into(),
//            toml::value::Value::String(package_name.clone()),
//        );
//        package_table.insert(
//            "version".into(),
//            toml::value::Value::String(format!("{}", package_version)),
//        );
//        package_table.insert(
//            "data_cksum".into(),
//            toml::value::Value::String(data_tar_hash),
//        );
//        package_table.insert(
//            "mount".into(),
//            toml::value::Value::String(mount.clone())
//        );
//
//        map.insert("package".into(), toml::value::Value::Table(package_table));
//
////        if !deps.is_empty() {
////            let mut depends_table = toml::map::Map::new();
////            for pair in deps {
////                depends_table.insert(pair.0.into(), pair.1.into());
////            }
////            map.insert("depends".into(), toml::value::Value::Table(depends_table));
////        } else {
////            if verbose {
////            }
////        }
//
//        writeln!(&mut metafile, "{}", toml::ser::to_string(&map).unwrap())?;
//        writeln!(&mut metafile, "[files]")?;
//        for entry in &mut file_listing.files {
//            if let Some(ref hash) = entry.hash {
//                //println!("\"{}\" = \"{}\"\n", &entry.pkg_path.display(), hash);
//                write!(
//                    &mut metafile,
//                    "\"{}\" = \"{}:{}\"\n",
//                    &entry.pkg_path.display(),
//                    match entry.file_type {
//                        FileType::Dir => 'd',
//                        FileType::File => 'f',
//                        FileType::Link(_) => 's',
//                    },
//                    hash
//                )?;
//            } else {
//                //println!("file {} has no hash", entry.pkg_path.display());
//                if !matches!(entry.file_type, FileType::Dir) {
//                    unreachable!("{}", format!("non-dir file '{}' with no hash", entry.pkg_path.display()));
//                }
//            }
//        }
//    }
