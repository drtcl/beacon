//! Package
//!
//! utility functions around package files
//!
//! Package File Naming:
//! package naming follows the format of:
//! <name>_<version>_<reserved>.bpm
//! The reserved portion is for future possibility of including architecture or package types in
//! the filename. This format mostly follows that of .deb file packages.

#![feature(let_chains)]

use camino::{Utf8Path, Utf8PathBuf};
use anyhow::Context;
use anyhow::Result;
use bpmutil::*;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs::File;
use std::io::{Read, Write, Seek};

pub const PKG_FILE_EXTENSION: &str = "bpm";
pub const DOTTED_PKG_FILE_EXTENSION: &str = ".bpm";

pub type PkgName = String;
pub type Version = String;
pub type FilePath = Utf8PathBuf;

/// Map with ordering
type OrderedMap<K, V> = BTreeMap<K, V>;

pub const META_FILE_NAME: &str = "meta.json";

pub const DATA_FILE_NAME: &str = "data.tar.zst";

const FILE_ATTR: char = 'f';
const DIR_ATTR:  char = 'd';
const SYMLINK_ATTR: char = 's';
const VOLATILE_ATTR: char = 'v';

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
#[serde(tag="filetype")]
pub enum FileType {
    Dir,
    File,
    Link(String),
}

impl FileType {
    pub fn is_dir(&self) -> bool {
        matches!(self, FileType::Dir)
    }
    pub fn is_file(&self) -> bool {
        matches!(self, FileType::File)
    }
    pub fn is_link(&self) -> bool {
        matches!(self, FileType::Link(_to))
    }
    pub fn get_link(&self) -> Option<String> {
        match self {
            FileType::Link(to) => Some(to.clone()),
            _ => None
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
#[serde(try_from = "FileInfoString")]
#[serde(into = "FileInfoString")]
pub struct FileInfo {
    //#[serde(rename = "type")]
    pub filetype: FileType,

    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hash: Option<String>,

    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mtime: Option<u64>,

    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size: Option<u64>,

    pub volatile: bool,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct PackageID {
    pub name: String,
    pub version: Version,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct DependencyID {
    pub name: String,
    pub version: Option<Version>,
}


/// Information about a package.
/// - package name and version
/// - what mount it will be installed to
/// - a list of dependencies
/// - a list of included files
/// - the hash and size of the data file
/// - an arbitrary key-value store
/// - a build-time UUID
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct MetaData {
    //#[serde(flatten)]
    //pub id: PackageID,
    pub name: String,
    pub version: Version,
    pub mount: Option<String>,
    pub data_hash: Option<String>,

    /// size of uncompressed tar of all data files
    /// (approximate size of install on disk, tar overhead vs disk overhead)
    pub data_size: u64,

    #[serde(default)]
    #[serde(skip_serializing_if = "OrderedMap::is_empty")]
    pub dependencies: OrderedMap<PkgName, Option<Version>>,

    pub files: OrderedMap<FilePath, FileInfo>,

    //note: `#[serde(default)]` mean use default value if it isn't present at deserialize

    #[serde(default)]
    #[serde(skip_serializing_if = "OrderedMap::is_empty")]
    pub kv: OrderedMap<String, String>,

    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,

    // package build-time UUID
    pub uuid: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
struct FileInfoString(String);
// strings look like this:
// d
// f:hash:mtime:size
// s:link_to:hash

/// FileInfo -> FileInfoString
impl From<FileInfo> for FileInfoString {
    fn from(info: FileInfo) -> Self {
        FileInfoString::from(&info)
    }
}


/// &FileInfo -> FileInfoString
impl From<&FileInfo> for FileInfoString {
    fn from(info: &FileInfo) -> Self {

        let mut ret = String::new();

        // file type
        match &info.filetype {
            FileType::Dir => ret.push(DIR_ATTR),
            FileType::File => ret.push(FILE_ATTR),
            FileType::Link(_to) => ret.push(SYMLINK_ATTR),
        }

        // volatile
        if info.volatile {
            ret.push(VOLATILE_ATTR);
        }

        // symlink to
        if let FileType::Link(to) = &info.filetype {
            ret.push(':');
            ret.push_str(to);
        }

        // hash
        ret.push(':');
        if let Some(hash) = &info.hash {
            ret.push_str(hash);
        }

        // mtime
        ret.push(':');
        if info.filetype.is_file() {
            if let Some(mtime) = info.mtime {
                ret.push_str(&format!("{mtime}"))
            }
        }

        // size
        ret.push(':');
        if let Some(size) = info.size {
            ret.push_str(&format!("{size}"))
        }

        // trim any trailing :
        while ret.ends_with(':') {
            ret.truncate(ret.len() - 1);
        }

        Self(ret)
    }
}

/// FileInfoString > FileInfo
impl TryFrom<FileInfoString> for FileInfo {
    type Error = String;
    fn try_from(s: FileInfoString) -> Result<Self, Self::Error> {
        let s = s.0;
        let mut iter = s.split(':');

        let mut volatile = false;
        let mut ft = None;
        if let Some(file_chars) = iter.next() {
            let mut file_chars = file_chars.chars();

            ft = match file_chars.next() {
                Some(FILE_ATTR) => Some(FileType::File),
                Some(DIR_ATTR) => Some(FileType::Dir),
                Some(SYMLINK_ATTR) => {
                    let to = iter.next().map(str::to_owned);
                    to.map(FileType::Link)
                }
                Some(_) | None => None,
            };

            if let Some(v) = file_chars.next() && v == VOLATILE_ATTR {
                volatile = true;
            }
        }

        if ft.is_none() {
            return Err(s);
        }

        let mut hash = iter.next().map(str::to_owned);
        if hash.as_deref() == Some("") {
            hash = None;
        }

        let mtime = iter.next().and_then(|v| v.parse::<u64>().ok());
        let size = iter.next().and_then(|v| v.parse::<u64>().ok());

        let ret = Self {
            filetype: ft.unwrap(),
            hash,
            mtime,
            volatile,
            size,
        };

        Ok(ret)
    }
}

impl MetaData {
    pub fn new(id: PackageID) -> Self {
        Self {
            //id,
            name: id.name,
            version: id.version,
            mount: None,
            data_hash: None,
            data_size: 0,
            dependencies: OrderedMap::new(),
            files: OrderedMap::new(),
            uuid: uuid::Uuid::nil().to_string(),
            kv: OrderedMap::new(),
            description: None,
        }
    }

    pub fn with_description(mut self, desc: Option<String>) -> Self {
        self.description = desc;
        self
    }

    pub fn with_kv(mut self, kv: OrderedMap<String, String>) -> Self {
        self.kv = kv;
        self
    }

    pub fn with_uuid(mut self, uuid: String) -> Self {
        self.uuid = uuid;
        self
    }

    pub fn id(&self) -> PackageID {
        PackageID {
            name: self.name.clone(),
            version: self.version.clone(),
        }
    }

    pub fn to_writer<W: Write>(&self, w: &mut W) -> Result<()> {
        //serde_json::to_writer(w, self)?;
        serde_json::to_writer_pretty(w, self)?;
        Ok(())
    }

    pub fn from_reader<R: Read>(r: &mut R) -> Result<Self> {
        let ret: Self = serde_json::from_reader(r)?;
        Ok(ret)
    }

    pub fn add_dependency(&mut self, id: DependencyID) {
        self.dependencies.insert(id.name, id.version);
    }

    pub fn add_file(&mut self, path: FilePath, info: FileInfo) {
        self.files.insert(path, info);
    }

    pub fn files(&self) -> &OrderedMap<FilePath, FileInfo> {
        &self.files
    }

}

pub fn seek_to_tar_entry<'a, R>(needle: &str, tar: &'a mut tar::Archive<R>) -> Result<(tar::Entry<'a, R>, u64)>
    where R: Read + Seek
{
    let needle = Utf8Path::new(needle);
    for entry in tar.entries_with_seek().context("failed to read tar archive")? {
        let entry = entry.context("failed to read tar archive")?;
        if let Ok(path) = entry.path() && path == needle {
            let size = entry.size();
            return Ok((entry, size));
        }
        //if let Ok(path) = entry.as_ref().unwrap().path() && path == needle {
            //return Ok(entry?);
        //}
    }
    Err(anyhow::anyhow!("path {} not found in tar archive", needle))
}

pub fn package_integrity_check_path(path: &Utf8Path) -> Result<(bool, MetaData)> {
    let mut file = File::open(path).context("reading file")?;
    package_integrity_check(&mut file)
}

/// Check that package file is self consistent. The metadata file list and data hash matches.
pub fn package_integrity_check(mut pkg_file: &mut File) -> Result<(bool, MetaData)> {

    let pbar = indicatif::ProgressBar::new(1);
    pbar.enable_steady_tick(std::time::Duration::from_millis(200));
    pbar.set_style(indicatif::ProgressStyle::with_template(
        " {spinner:.green} verifying package"
    ).unwrap());

    // it can take a while to parse the file list for large packages, do that in a separate thread
    // while we move on to hashing the package
    let metadata = read_metadata(pkg_file).context("error reading package metadata")?;
    let meta_thread = std::thread::spawn(move || -> Result<MetaData> {
        MetaData::from_reader(&mut std::io::Cursor::new(&metadata))
    });

    pkg_file.rewind()?;
    let computed_sum = {

        let mut tar = tar::Archive::new(&mut pkg_file);
        let (mut data, size) = seek_to_tar_entry(DATA_FILE_NAME, &mut tar)?;

        pbar.set_length(size);
        pbar.set_style(indicatif::ProgressStyle::with_template(
            #[allow(clippy::literal_string_with_formatting_args)]
            " {spinner:.green} verifying package {wide_bar:.green} {bytes_per_sec}  {bytes}/{total_bytes} "
        ).unwrap());

        blake3_hash_reader(&mut pbar.wrap_read(&mut data))?
    };

    let metadata = meta_thread.join().unwrap()?; // unwrapping an error is only possible on a panic

    let described_sum = metadata.data_hash.as_ref().context("metadata has no data hash").unwrap();
    let matches = &computed_sum == described_sum;
    tracing::debug!("computed data hash {} matches:{}", computed_sum, matches);

    //pbar.finish_and_clear();

    if !matches {
        tracing::error!("package file data hash mismatch {} != {}", computed_sum, described_sum);
        return Ok((false, metadata));
    }

    let mut meta_filelist = metadata.files.clone();

    // check the file list
    pkg_file.rewind()?;
    {

        pbar.set_position(0);
        pbar.set_length(meta_filelist.len() as u64);
        //let pbar = indicatif::ProgressBar::new(meta_filelist.len() as u64);
        #[allow(clippy::literal_string_with_formatting_args)]
        pbar.set_style(indicatif::ProgressStyle::with_template(
            " {spinner:.green} verifying files {wide_bar:.green} {pos}/{len} "
        ).unwrap());

        let mut outer_tar = tar::Archive::new(pkg_file);
        let (data_tar_zst, _size) = seek_to_tar_entry(DATA_FILE_NAME, &mut outer_tar)?;
        let zstd = zstd::Decoder::new(data_tar_zst)?;
        let mut tar = tar::Archive::new(zstd);
        for ent in tar.entries()? {
            let ent = ent?;
            let path = ent.path()?;
            let path = path.to_string_lossy().to_string();
            let path = Utf8Path::new(&path);
            if meta_filelist.remove(path).is_none() {
                tracing::error!("a file in the tar was not listed in the metadata {}", path);
                return Ok((false, metadata));
            }
            // TODO also check that the file type matches
            pbar.inc(1);
        }

        pbar.finish_and_clear();

        // if there are any remaining files, those were not in the tar
        if !meta_filelist.is_empty() {
            let path = meta_filelist.pop_first().unwrap().0;
            tracing::error!("a file in the metadata was not in the data tar: {}", path);
            return Ok((false, metadata));
        }
    }

    tracing::trace!("package passes integrity check");

    Ok((true, metadata))
}

pub fn read_metadata(pkg_file: &mut File) -> Result<Vec<u8>> {
    pkg_file.rewind()?;
    let mut tar = tar::Archive::new(pkg_file);
    let (mut meta, _size) = seek_to_tar_entry(META_FILE_NAME, &mut tar)?;
    let mut contents = Vec::new();
    meta.read_to_end(&mut contents)?;
    Ok(contents)
}

pub fn get_metadata(pkg_file: &mut File) -> Result<MetaData> {
    pkg_file.rewind()?;
    let mut tar = tar::Archive::new(pkg_file);
    let (mut meta, _size) = seek_to_tar_entry(META_FILE_NAME, &mut tar)?;
    let metadata = MetaData::from_reader(&mut meta)?;
    Ok(metadata)
}

pub fn get_filelist(pkg_file: &mut File) -> Result<OrderedMap<FilePath, FileInfo>> {
    let metadata = get_metadata(pkg_file)?;
    Ok(metadata.files)
}

/// "foo_1.2.3" -> ("foo", "1.2.3")
/// "foo-bar_1.2.3-alpha.bpm" -> ("foo-bar", "1.2.3-alpha")
pub fn split_parts(filename: &str) -> Option<(&str, &str)> {

    let filename = filename.strip_suffix(DOTTED_PKG_FILE_EXTENSION).unwrap_or(filename);
    let mut parts = filename.split('_');

    let name = parts.next();
    let version = parts.next();
    let _reserved = parts.next();
    let _none = parts.next();

    if let (Some(name), Some(version)) = (name, version) {
        Some((name, version))
    } else {
        None
    }
}

/// Names like "foo-1.0.0.bpm" and "bar-0.2.1.bpm" are packagefile names
pub fn is_packagefile_name(text: &str) -> bool {
    if text.ends_with(DOTTED_PKG_FILE_EXTENSION) {
        if let Some((name, version)) = split_parts(text) {
            return is_valid_package_name(name) && is_valid_version(version)
        }
    }
    false
}

/// Names like "foo" and "bar" are package names
pub fn is_valid_package_name(text: &str) -> bool {
    // cannot be empty string
    // cannot contain underscore _
    // only alphanumeric or '-'
    // starts with [a-zA-Z]
    // matches [a-zA-Z][a-zA-Z0-9\-]*
    // does not end with -
    // does not have multiple consecutive -
    !text.is_empty()
        && !text.contains('_')
        && text.chars().next().unwrap().is_alphabetic()
        && text.chars().all(|c| {
            c.is_ascii() && c.is_alphanumeric() || c == '-'
        })
        && !text.ends_with('-')
        && !text.contains("--")
}

pub fn make_packagefile_name(pkg_name: &str, version: &str) -> String {
    format!("{pkg_name}_{version}{DOTTED_PKG_FILE_EXTENSION}")
}

/// strings like "1.2.3" and "0.0.1-alpha+linux" are version strings
pub fn is_valid_version(text: &str) -> bool {
    // cannot be empty string
    // cannot contain underscore _
    // cannot contain the file extension
    // must start with a number
    // must end with an alphanumeric
    // does not have multiple consecutive -, +, or .
    !text.is_empty()
        && !text.contains('_')
        && !text.contains(PKG_FILE_EXTENSION)
        && text.chars().next().unwrap().is_ascii_digit()
        && text.chars().last().unwrap().is_alphanumeric()
        && text.chars().all(|c| {
            c.is_ascii() && c.is_alphanumeric() || c == '-' || c == '+' || c == '.'
        })
        && text.as_bytes().windows(2).all(|ab| {
            let s = ['.', '-', '+'];
            !(s.contains(&(ab[0] as char)) && s.contains(&(ab[1] as char)))
        })
}

pub fn filename_match(filename: &str, id: &PackageID) -> bool {
    if let Some((name, version)) = split_parts(filename) {
        return name == id.name && version == id.version;
    }
    false
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn package_names() {

        assert_eq!(split_parts("foo_1.2.3.bpm"), Some(("foo", "1.2.3")));
        assert_eq!(split_parts("foo-bar_1.2.3-alpha.1.bpm"), Some(("foo-bar", "1.2.3-alpha.1")));

        assert_eq!(split_parts("foo-1.2.3.bpm"), None);

        assert!(is_packagefile_name("foo_1.2.3.bpm"));
        assert!(is_packagefile_name("foo-bar_1.2.3.bpm"));
        assert!(is_packagefile_name("foo-bar_1.2.3-alpha.bpm"));
        assert!(is_packagefile_name("foo-bar_1.2.3-alpha.0.bpm"));
        assert!(is_packagefile_name("foo-bar_1.2.3+linux.bpm"));
        assert!(is_packagefile_name("foo-bar_1.2.3-alpha+linux.bpm"));

        assert_eq!(make_packagefile_name("foo", "1.2.3"), "foo_1.2.3.bpm");
        assert!(is_packagefile_name(&make_packagefile_name("foo", "1.2.3")));
        assert_eq!(split_parts(&make_packagefile_name("foo", "1.2.3")), Some(("foo", "1.2.3")));
    }

    fn get_instance() -> MetaData {
        let mut meta = MetaData {
            //id: PackageID {
                name: "foo".into(),
                version: "1.2.3".into(),
            //},
            mount: Some("EXT".into()),
            data_hash: None,
            data_size: 0,
            dependencies: OrderedMap::new(),
            files: OrderedMap::new(),
            description: None,
            kv: BTreeMap::new(),
            uuid: "".into(),
        };

        meta.add_dependency(DependencyID {
            name: "bar".into(),
            version: Some("3.1.4".into()),
        });
        meta.add_dependency(DependencyID {
            name: "baz".into(),
            version: Some("0.7.1".into()),
        });

        meta.add_file(
            "foo".into(),
            FileInfo {
                filetype: FileType::Dir,
                hash: None,
                mtime: None,
                volatile: false,
            },
        );
        meta.add_file(
            "foo/a2.c".into(),
            FileInfo {
                filetype: FileType::File,
                hash: Some("2ffac14".into()),
                mtime: None,
                volatile: false,
            },
        );
        meta.add_file(
            "foo/a1.c".into(),
            FileInfo {
                filetype: FileType::File,
                hash: Some("1aef313".into()),
                mtime: None,
                volatile: false,
            },
        );
        meta.add_file(
            "foo/link".into(),
            FileInfo {
                filetype: FileType::Link("a1.c".into()),
                hash: Some("77af123".into()),
                mtime: None,
                volatile: false,
            },
        );

        meta
    }

    #[test]
    fn valid_names() {

        assert!(is_valid_package_name("foobar"));
        assert!(is_valid_package_name("foo-bar"));
        assert!(is_valid_package_name("FooBar"));

        assert!(!is_valid_package_name("foo_bar"));
        assert!(!is_valid_package_name("foo-"));
        assert!(!is_valid_package_name("foo--bar"));
    }

    #[test]
    fn valid_versions() {

        assert!(is_valid_version("0.0.0"));
        assert!(is_valid_version("0.0.1"));
        assert!(is_valid_version("1.2.3"));

        assert!(is_valid_version("0.0.0.0"));
        assert!(is_valid_version("0.0.0.1"));
        assert!(is_valid_version("1"));
        assert!(is_valid_version("1.2"));
        assert!(is_valid_version("1.2.3.4"));

        assert!(is_valid_version("1.2.3-rc1"));
        assert!(is_valid_version("1.2.3.4-rc1"));

        assert!(is_valid_version("1.2.3-1-rc1"));
        assert!(is_valid_version("1.2.3.4-1-rc1"));

        assert!(is_valid_version("1.2.3.4-7-ga1b2c3+foo-bar"));
        assert!(is_valid_version("1+2"));

        assert!(!is_valid_version("foo"));
        assert!(!is_valid_version("1.2.3.bpm"));
        assert!(!is_valid_version("1.2.3--1"));
        assert!(!is_valid_version("1.2.3++1"));
        assert!(!is_valid_version("1.2.3-+1"));
        assert!(!is_valid_version("1.2.3+-1"));
        assert!(!is_valid_version("1..2"));
        assert!(!is_valid_version("1.-2"));
        assert!(!is_valid_version("1.+2"));
    }

    #[test]
    fn ser_de() -> Result<()> {
        let meta = get_instance();

        let mut output: Vec<u8> = Vec::new();
        meta.to_writer(&mut output)?;
        println!("{}", String::from_utf8_lossy(&output));
        let ret = MetaData::from_reader(&mut std::io::Cursor::new(output.as_slice()))?;

        assert_eq!(meta, ret);
        Ok(())
    }

    #[test]
    fn fileinfo_file_no_hash() {
        let info = FileInfo {
            filetype: FileType::File,
            hash: None,
            mtime: Some(100),
            volatile: false,
        };

        let s = FileInfoString("f::100".into());
        let info_from = FileInfo::try_from(s.clone()).unwrap();
        let s_from = FileInfoString::from(&info);
        assert_eq!(s, s_from);
        assert_eq!(info, info_from);
    }

    #[test]
    fn fileinfo_file_volatile() {
        let info = FileInfo {
            filetype: FileType::File,
            hash: Some("a1b2".into()),
            mtime: Some(100),
            volatile: true,
        };

        let s = FileInfoString("fv:a1b2:100".into());
        let info_from = FileInfo::try_from(s.clone()).unwrap();
        let s_from = FileInfoString::from(&info);
        assert_eq!(s, s_from);
        assert_eq!(info, info_from);
    }

    #[test]
    fn fileinfo_dir() {

        let info = FileInfo {
            filetype: FileType::Dir,
            hash: None,
            mtime: None,
            volatile: false,
        };

        let s = FileInfoString("d".into());
        let info_from = FileInfo::try_from(s.clone()).unwrap();
        let s_from = FileInfoString::from(&info);
        assert_eq!(s, s_from);
        assert_eq!(info, info_from);
    }

    #[test]
    fn fileinfo_link() {

        let info = FileInfo {
            filetype: FileType::Link(String::from("foo/bar")),
            hash: None,
            mtime: None,
            volatile: false,
        };

        let s = FileInfoString("s:foo/bar".into());
        let info_from = FileInfo::try_from(s.clone()).unwrap();
        let s_from = FileInfoString::from(&info);
        assert_eq!(s, s_from);
        assert_eq!(info, info_from);
    }

    #[test]
    fn fileinfo_link_volatile() {

        let info = FileInfo {
            filetype: FileType::Link(String::from("foo/bar")),
            hash: None,
            mtime: None,
            volatile: true,
        };

        let s = FileInfoString("sv:foo/bar".into());
        let info_from = FileInfo::try_from(s.clone()).unwrap();
        let s_from = FileInfoString::from(&info);
        assert_eq!(s, s_from);
        assert_eq!(info, info_from);
    }

}
