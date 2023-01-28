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

use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use blake2::{Blake2b, Digest};
use clap::Arg;
use semver::{Error as SemVerError, Version};

#[derive(Debug)]
struct Error {
    error: String,
    kind: std::io::ErrorKind,
}

impl From<&str> for Error {
    fn from(error: &str) -> Self {
        Self {
            error: error.to_string(),
            kind: std::io::ErrorKind::Other,
        }
    }
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Self {
            error: e.to_string(),
            kind: e.kind(),
        }
    }
}

impl From<SemVerError> for Error {
    fn from(e: SemVerError) -> Self {
        Self {
            error: e.to_string(),
            kind: std::io::ErrorKind::Other,
        }
    }
}

//
// take a list of files
// save them into a data tarball
// hash the tarball
// get the hash for each file
// save hashes into a meta file
// tar the hashes and data tarball into a single package tarball
// cleanup intermediate files
//

//fn is_int(v: String) -> Result<(), String> {
//    if v.parse::<u32>().is_ok() {
//        return Ok(());
//    } else {
//        return Err(String::from("integer value required"));
//    }
//}

//fn rebase_path<P>(root: P, path: P) -> PathBuf
//where
//    P: AsRef<Path>,
//{
//    //print!("  rebase {:?} {:?}", root, &path);
//    let path = path.as_ref().strip_prefix(root).unwrap().to_path_buf();
//    //println!(" -> {:?}", &path);
//    PathBuf::from(path)
//}

#[derive(Debug)]
struct FileEntry {
    full_path: PathBuf,
    rel_path: PathBuf,
    dir: bool,
    hash: Option<String>,
}

#[derive(Debug)]
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

fn file_discovery(root_dir: &Path) -> FileListing {
    let path_prefix = root_dir.parent();

    let mut files = Vec::new();
    for entry in walkdir::WalkDir::new(root_dir).sort_by_file_name() {
        let entry = entry.unwrap();
        let full_path = PathBuf::from(entry.path());
        let rel_path = full_path
            .strip_prefix(path_prefix.unwrap())
            .unwrap()
            .to_path_buf();
        files.push(FileEntry {
            full_path,
            rel_path,
            dir: entry.file_type().is_dir(),
            hash: None,
        });
    }

    return FileListing { files };
}

fn main() -> Result<(), Error> {

    let matches = clap::Command::new("bpm-pack")
        .version("0.1.0")
        .about("Bryan's Package Manager : bpm-pack : package creation utility")
        .author("Bryan Splitgerber")
        .disable_version_flag(true)
        .arg(
            Arg::new("no-cleanup")
                .long("no-cleanup")
                .action(clap::ArgAction::SetTrue),
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
                .default_value("0")
                .help("verbosity level"), //.validator(is_int)
        )
        //.arg(
        //    Arg::new("depend")
        //        .long("--depend")
        //        .action(clap::ArgAction::Append)
        //        .help("define a dependency"),
        //)
        .arg(
            Arg::new("file")
                .action(clap::ArgAction::Append)
                .required(true),
        )
        .get_matches();

    // the version must be of the right semver format
    let package_version_str = matches.get_one::<String>("version").unwrap();
    let package_version = Version::parse(package_version_str).expect("invalid version");

    let package_name = matches.get_one::<String>("name").unwrap();

    //let verbosity = matches.get_one("verbose").unwrap().parse::<u32>().unwrap();

    //let root_dir = matches.get_one::<String>("root").unwrap();
    //let root_dir = std::fs::canonicalize(root_dir).expect("failed to canonicalize root dir");
    //dbg!(&root_dir);

    let cache_dir = Path::new("./bpm_cache/");

    // create the cache directory if it doesn't exist
    if !cache_dir.exists() {
        std::fs::create_dir(cache_dir)?; // error: failed to create cache directory
    }

    // the file names
    // - data - all target file in the package
    // - meta - meta data about data files

    let mut tarball_file_path = PathBuf::new();
    tarball_file_path.push(cache_dir);
    tarball_file_path.push("data.tar.zst");

    let mut meta_file_path = PathBuf::new();
    meta_file_path.push(cache_dir);
    meta_file_path.push("meta.toml");

    let mut package_file_path = PathBuf::new();
    package_file_path.push(cache_dir);
    package_file_path.push(format!("{}-{}.bpm.tar", package_name, package_version));

    let mut file_listing = FileListing::new();
    for path in matches.get_many::<String>("file").unwrap() {
        file_listing.extend(file_discovery(&PathBuf::from(path)));
    }

    let data_tar_file = std::fs::File::create(tarball_file_path.as_path())?;
    let data_tar_file = std::io::BufWriter::new(data_tar_file);
    let mut data_tar_hasher = HashingWriter {
        inner: data_tar_file,
        hasher: Blake2b::new(),
    };
    let mut data_tar_file = zstd::stream::write::Encoder::new(&mut data_tar_hasher, 21)?;
    let mut data_tar = tar::Builder::new(&mut data_tar_file);

    // create the target install files tar file
    for entry in &mut file_listing.files {
        if entry.dir {
            data_tar.append_dir(&entry.rel_path, &entry.full_path)?;
        } else {
            let fd = std::fs::File::open(&entry.full_path)?;
            //let file_size = fd.metadata()?.len();

            let mut header = tar::Header::new_gnu();
            header.set_metadata_in_mode(&fd.metadata()?, tar::HeaderMode::Complete);
            //header.set_size(file_size);
            //header.set_cksum();

            let hasher = Blake2b::new();
            let mut reader = HashingReader {
                inner: std::io::BufReader::new(fd),
                hasher,
            };

            data_tar.append_data(&mut header, &entry.rel_path, &mut reader)?;
            let hash = reader.hasher.finalize();
            entry.hash = Some(hex_string(hash.as_slice()));
        }
    }

    data_tar.finish()?;
    drop(data_tar);
    data_tar_file.flush()?;
    drop(data_tar_file);

    let data_tar_hash = hex_string(data_tar_hasher.hasher.finalize().as_slice());
    //println!("data tar hash {}", &data_tar_hash);

    let deps: Vec<(String, String)> = Vec::new();
    //let deps = vec![("foo", "3.1.4"), ("honk", "4.0.1")];

    // --- write toml data ---
    {
        // now create a file that lists all included file paths and their hash
        let metafile = std::fs::File::create(&meta_file_path)?;
        let mut metafile = std::io::BufWriter::new(metafile);

        let mut map = toml::map::Map::new();

        let mut package_table = toml::map::Map::new();
        package_table.insert(
            "name".into(),
            toml::value::Value::String(package_name.clone()),
        );
        package_table.insert(
            "version".into(),
            toml::value::Value::String(format!("{}", package_version)),
        );
        package_table.insert(
            "data_cksum".into(),
            toml::value::Value::String(data_tar_hash),
        );

        map.insert("package".into(), toml::value::Value::Table(package_table));

        if !deps.is_empty() {
            let mut depends_table = toml::map::Map::new();
            for pair in deps {
                depends_table.insert(pair.0.into(), pair.1.into());
            }
            map.insert("depends".into(), toml::value::Value::Table(depends_table));
        }

        writeln!(&mut metafile, "{}", toml::ser::to_string(&map).unwrap())?;
        writeln!(&mut metafile, "[files]")?;
        for entry in &mut file_listing.files {
            if let Some(ref hash) = entry.hash {
                //println!("\"{}\" = \"{}\"\n", &entry.rel_path.display(), hash);
                write!(
                    &mut metafile,
                    "\"{}\" = \"{}\"\n",
                    &entry.rel_path.display(),
                    hash
                )?;
            }
        }
    }

    // --- create a single tar package file ---
    let pkg_file = std::fs::File::create(&package_file_path)?;
    let pkg_file = std::io::BufWriter::new(pkg_file);
    let mut hasher = HashingWriter {
        hasher: Blake2b::new(),
        inner: pkg_file,
    };
    let mut pkg_file = tar::Builder::new(&mut hasher);

    pkg_file.append_path_with_name(&meta_file_path, "meta.toml")?;
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
        return ret;
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
        return ret;
    }
    fn flush(&mut self) -> Result<(), std::io::Error> {
        self.inner.flush()
    }
}
