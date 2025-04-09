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
    pub arch: Option<String>,
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
    pub arch: Option<String>,
    pub mount: Option<String>,
    pub data_hash: Option<String>,

    /// size of uncompressed tar of all data files
    /// (approximate size of install on disk, tar overhead vs disk overhead)
    pub data_size: u64,

    #[serde(default)]
    #[serde(skip_serializing_if = "OrderedMap::is_empty")]
    pub dependencies: OrderedMap<PkgName, Option<Version>>,

    pub files: OrderedMap<FilePath, FileInfo>,

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
            arch: id.arch,
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
            arch: self.arch.clone(),
        }
    }

    pub fn to_writer<W: Write>(&self, w: &mut W) -> Result<()> {
        //serde_json::to_writer(w, self)?;
        serde_json::to_writer_pretty(w, self)?;
        Ok(())
    }

    pub fn from_reader<R: Read>(r: &mut R) -> Result<Self> {
        #[cfg(feature="sonic_json")]
        let ret: Self = sonic_rs::from_reader(r)?;

        #[cfg(not(feature="sonic_json"))]
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
    }
    Err(anyhow::anyhow!("path {} not found in tar archive", needle))
}

#[derive(Default, Debug)]
pub struct CheckResult {
    pub file_hash: String,
    pub data_hash: String,
    meta_ok: bool,
    data_ok: bool,
    files_ok: bool,
    name_ok: bool,
}

impl CheckResult {
    pub fn good(&self) -> bool {
        self.meta_ok
            && self.data_ok
            && self.files_ok
            && self.name_ok
    }
}

pub fn package_integrity_check_full(
    mut pkg_file: &mut File,
    file_name: Option<&str>,
    known_hash: Option<&str>,
) -> Result<CheckResult> {

    pkg_file.rewind()?;

    let filesize = pkg_file.metadata().ok().map(|v| v.len());

    let status = bpmutil::status::global();
    let bar = status.add_task(Some("verify_package"), file_name, filesize);
    bar.set_style(indicatif::ProgressStyle::with_template(" {spinner:.green} verifying package {wide_bar:.blue} ").unwrap());

    let mut ret = CheckResult::default();

    // default state must be an integrity failure
    assert!(!ret.meta_ok);
    assert!(!ret.data_ok);
    assert!(!ret.files_ok);
    assert!(!ret.name_ok);

    let mut read = bpmutil::status::wrap_read(Some(&bar), &mut pkg_file);
    ret.file_hash = blake3_hash_reader(&mut read)?;
    bar.finish_and_clear();

    pkg_file.rewind()?;

    if file_name.is_none() && known_hash.is_some() {
        if known_hash.unwrap() == ret.file_hash {
            // full file hash matched, assume everything else is good
            ret.meta_ok = true;
            ret.data_ok = true;
            ret.files_ok = true;
            ret.name_ok = true;
            return Ok(ret);
        } else {
            // hash does not match, failure
            return Ok(ret);
        }
    }

    let metadata_raw = read_metadata(pkg_file).context("error reading package metadata")?;
    let mut metadata = MetaData::from_reader(&mut std::io::Cursor::new(&metadata_raw))?;
    ret.meta_ok = true;

    // if not testing, assume ok
    ret.name_ok = true;
    if let Some(file_name) = file_name {
        if let Some((name, version, arch)) = split_parts(file_name) {
            ret.name_ok = metadata.name == name && metadata.version == version && metadata.arch.as_deref() == arch;
        } else {
            ret.name_ok = false;
            return Ok(ret);
        }
    }

    if known_hash.is_some() {
        if known_hash.unwrap() == ret.file_hash {
            ret.data_hash = metadata.data_hash.context("metadata has no data hash")?;
            // assume the rest is good
            ret.data_ok = true;
            ret.files_ok = true;
            return Ok(ret);
        } else {
            return Ok(ret);
        }
    }

    // --- check data hash ---

    pkg_file.rewind()?;
    let computed_sum = {

        let mut tar = tar::Archive::new(&mut pkg_file);
        let (mut data, size) = seek_to_tar_entry(DATA_FILE_NAME, &mut tar)?;

        let bar = status.add_task(Some("verify_data"), file_name, Some(size));
        bar.set_style(indicatif::ProgressStyle::with_template(" {spinner:.green} verifying data    {wide_bar:.blue} ").unwrap());

        let mut read = bar.wrap_read(&mut data);
        let sum = blake3_hash_reader(&mut read)?;
        bar.finish_and_clear();
        sum
    };

    ret.data_ok = metadata.data_hash.as_ref() == Some(&computed_sum);

    let mut meta_filelist = std::mem::take(&mut metadata.files);
    ret.files_ok = true;

    // check the file list
    pkg_file.rewind()?;

    let bar = status.add_task(Some("verify_files"), file_name, Some(meta_filelist.len() as u64));
    bar.set_style(indicatif::ProgressStyle::with_template(" {spinner:.green} verifying files   {wide_bar:.blue} ").unwrap());

    let mut outer_tar = tar::Archive::new(pkg_file);
    let (data_tar_zst, _size) = seek_to_tar_entry(DATA_FILE_NAME, &mut outer_tar)?;
    let zstd = zstd::Decoder::new(data_tar_zst)?;
    let mut tar = tar::Archive::new(zstd);
    for ent in tar.entries()? {
        let ent = ent?;
        let path = ent.path()?;
        let path = path.to_string_lossy().to_string();
        let path = Utf8Path::new(&path);
        if let Some(meta_info) = meta_filelist.remove(path) {
            match (ent.header().entry_type(), meta_info.filetype) {
                (tar::EntryType::Directory, FileType::Dir) => {}
                (tar::EntryType::Regular,   FileType::File) => {}
                (tar::EntryType::Symlink,   FileType::Link(_)) => {}
                _ => {
                    ret.files_ok = false;
                    tracing::error!("a file in the tar was the incorrect file type {}", path);
                    break;
                }
            }
        } else {
            tracing::error!("a file in the tar was not listed in the metadata {}", path);
            ret.files_ok = false;
            break;
        }
        bar.inc(1);
    }

    // if there are any remaining files, those were not in the tar
    if !meta_filelist.is_empty() {
        let path = meta_filelist.pop_first().unwrap().0;
        tracing::error!("a file in the metadata was not in the data tar: {}", path);
        ret.files_ok = false;
    }

    bar.finish_and_clear();

    return Ok(ret);
}

/// Check that package file is self consistent. The metadata file list and data hash matches.
pub fn package_integrity_check(pkg_file: &mut File) -> Result<(bool, MetaData)> {
    let check = package_integrity_check_full(pkg_file, None, None)?;
    let md = get_metadata(pkg_file)?;
    Ok((check.good(), md))
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
    //let now = std::time::Instant::now();
    pkg_file.rewind()?;
    let mut tar = tar::Archive::new(pkg_file);
    let (mut meta, _size) = seek_to_tar_entry(META_FILE_NAME, &mut tar)?;
    let metadata = MetaData::from_reader(&mut meta)?;
    //println!("get metadata {:?}", now.elapsed());
    Ok(metadata)
}

/// "foo_1.2.3" -> ("foo", "1.2.3", None)
/// "foo_1.2.3_linux64" -> ("foo", "1.2.3", Some("linux64"))
/// "foo-bar_1.2.3-alpha.bpm" -> ("foo-bar", "1.2.3-alpha", None)
pub fn split_parts(filename: &str) -> Option<(&str, &str, Option<&str>)> {

    let filename = filename.strip_suffix(DOTTED_PKG_FILE_EXTENSION).unwrap_or(filename);

    let mut it = filename.splitn(3, '_');
    let name = it.next();
    let version = it.next();
    let arch = it.next();

    // require a name and version
    match (name, version, arch) {
        (Some(name), Some(version), arch) => Some((name, version, arch)),
        _ => None,
    }
}

pub fn make_packagefile_name(pkg_name: &str, version: &str, arch: Option<&str>) -> String {

    match arch {
        Some(arch) if !arch.is_empty() =>
            format!("{pkg_name}_{version}_{arch}{DOTTED_PKG_FILE_EXTENSION}"),
        _ =>
            format!("{pkg_name}_{version}{DOTTED_PKG_FILE_EXTENSION}")
    }
}

/// Names like "foo_1.0.0.bpm", "bar_0.2.1.bpm", "baz_1.2.3_linux64.bpm" are packagefile names
pub fn is_packagefile_name(text: &str) -> bool {
    if text.ends_with(DOTTED_PKG_FILE_EXTENSION) {
        if let Some((name, version, arch)) = split_parts(text) {
            return is_valid_package_name(name)
                && is_valid_version(version)
                && is_valid_arch(arch)
        }
    }
    false
}

/// Names like "foo", "foo9", and "foo-bar" are package names
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

/// strings like "", "noarch", "linux_x86_64", "linux-aarch64" are valid arch strings
pub fn is_valid_arch(text: Option<&str>) -> bool {

    // must be [a-zA-Z][a-zA-Z0-9_\-]*
    // must start with alphabetic
    // must end with alphanumeric
    // can contain singluar - or _
    match text {
        None | Some("") => true,
        Some(text) =>
            text.chars().all(|c| c.is_alphanumeric() || c == '_' || c == '-')
                && !text.contains("--")
                && !text.contains("__")
                && !text.contains("_-")
                && !text.contains("-_")
                && !text.starts_with('-')
                && !text.starts_with('_')
                && !text.ends_with('-')
                && !text.ends_with('_'),
    }
}

pub fn filename_match(filename: &str, id: &PackageID) -> bool {
    let mut ret = false;
    if let Some((name, version, arch)) = split_parts(filename) {
        ret = name == id.name
            && version == id.version
            && ArchMatcher::from(arch).matches(&id.arch);
    }
    //tracing::trace!("filename_match {} {:?} => {}", filename, id, ret);
    ret
}

#[derive(Debug)]
pub enum ArchMatcher {
    Any,
    None,
    Some(String),
}

impl From<&str> for ArchMatcher {
    fn from(value: &str) -> Self {
        match value {
            "*" => ArchMatcher::Any,
            "" | "noarch" => ArchMatcher::None,
            x => ArchMatcher::Some(x.into()),
        }
    }
}

impl From<&String> for ArchMatcher {
    fn from(value: &String) -> Self {
        ArchMatcher::from(value.as_str())
    }
}

impl From<&Option<String>> for ArchMatcher {
    fn from(value: &Option<String>) -> Self {
        ArchMatcher::from(value.as_deref())
    }
}

impl From<Option<&str>> for ArchMatcher {
    fn from(value: Option<&str>) -> Self {
        match value {
            None => ArchMatcher::None,
            Some(value) => ArchMatcher::from(value),
        }
    }
}

impl ArchMatcher {
    pub fn matches<T: Into<ArchMatcher>>(&self, other: T) -> bool {
        match (self, &other.into()) {
            (Self::Any, _) => true,
            (Self::None, Self::None) => true,
            (Self::Some(x), Self::Some(y)) if x == y => true,
            _ => false,
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn valid_arch() {
        assert!(is_valid_arch(None));
        assert!(is_valid_arch(Some("")));
        assert!(is_valid_arch(Some("noarch")));
        assert!(is_valid_arch(Some("linux")));
        assert!(is_valid_arch(Some("linux-x86")));
        assert!(is_valid_arch(Some("linux-x86_64")));
        assert!(is_valid_arch(Some("linux_x86-64")));

        assert!(!is_valid_arch(Some("linux--64")));
        assert!(!is_valid_arch(Some("linux__64")));
        assert!(!is_valid_arch(Some("linux-_64")));
        assert!(!is_valid_arch(Some("linux_-64")));
        assert!(!is_valid_arch(Some("-linux")));
        assert!(!is_valid_arch(Some("_linux")));
        assert!(!is_valid_arch(Some("linux-")));
        assert!(!is_valid_arch(Some("linux_")));
    }

    #[test]
    fn arch_match() {
        assert!(ArchMatcher::from("").matches(""));
        assert!(ArchMatcher::from("").matches(None));
        assert!(ArchMatcher::from(None).matches(""));
        assert!(ArchMatcher::from("*").matches(""));
        assert!(ArchMatcher::from("*").matches("foo"));
        assert!(ArchMatcher::from("*").matches(None));
        assert!(ArchMatcher::from("foo").matches("foo"));

        assert!(!ArchMatcher::from(None).matches("bar"));
        assert!(!ArchMatcher::from("").matches("bar"));
        assert!(!ArchMatcher::from("foo").matches("bar"));
    }

    #[test]
    fn package_names() {

        assert_eq!(split_parts("foo_1.2.3.bpm"), Some(("foo", "1.2.3", None)));
        assert_eq!(split_parts("foo-bar_1.2.3-alpha.1.bpm"), Some(("foo-bar", "1.2.3-alpha.1", None)));

        assert_eq!(split_parts("foo_1.2.3_linux-x86_64.bpm"), Some(("foo", "1.2.3", Some("linux-x86_64"))));

        assert_eq!(split_parts("foo-1.2.3.bpm"), None);

        assert!(is_packagefile_name("foo_1.2.3.bpm"));
        assert!(is_packagefile_name("foo-bar_1.2.3.bpm"));
        assert!(is_packagefile_name("foo-bar_1.2.3-alpha.bpm"));
        assert!(is_packagefile_name("foo-bar_1.2.3-alpha.0.bpm"));
        assert!(is_packagefile_name("foo-bar_1.2.3+linux.bpm"));
        assert!(is_packagefile_name("foo-bar_1.2.3-alpha+linux.bpm"));

        assert_eq!(make_packagefile_name("foo", "1.2.3", None), "foo_1.2.3.bpm");
        assert_eq!(make_packagefile_name("foo", "1.2.3", Some("")), "foo_1.2.3.bpm");

        assert!(is_packagefile_name(&make_packagefile_name("foo", "1.2.3", None)));
        assert_eq!(split_parts(&make_packagefile_name("foo", "1.2.3", None)), Some(("foo", "1.2.3", None)));
    }

    fn get_instance() -> MetaData {
        let mut meta = MetaData {
            //id: PackageID {
                name: "foo".into(),
                version: "1.2.3".into(),
                arch: None,
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
                size: None,
            },
        );
        meta.add_file(
            "foo/a2.c".into(),
            FileInfo {
                filetype: FileType::File,
                hash: Some("2ffac14".into()),
                mtime: None,
                volatile: false,
                size: None,
            },
        );
        meta.add_file(
            "foo/a1.c".into(),
            FileInfo {
                filetype: FileType::File,
                hash: Some("1aef313".into()),
                mtime: None,
                volatile: false,
                size: None,
            },
        );
        meta.add_file(
            "foo/link".into(),
            FileInfo {
                filetype: FileType::Link("a1.c".into()),
                hash: Some("77af123".into()),
                mtime: None,
                volatile: false,
                size: None,
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
            size: None,
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
            size: None,
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
            size: None,
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
            size: None,
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
            size: None,
        };

        let s = FileInfoString("sv:foo/bar".into());
        let info_from = FileInfo::try_from(s.clone()).unwrap();
        let s_from = FileInfoString::from(&info);
        assert_eq!(s, s_from);
        assert_eq!(info, info_from);
    }

}
