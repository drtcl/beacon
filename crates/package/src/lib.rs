#![feature(let_chains)]

//use serde::ser::SerializeMap;
use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::io::Read;
use std::io::Write;
use std::io::Seek;
use std::fs::File;
use std::path::Path;

pub const PKG_FILE_EXTENSION: &str = "bpm";
pub const DOTTED_PKG_FILE_EXTENSION: &str = ".bpm";

pub type PkgName = String;
pub type Version = String;
pub type FilePath = String;

/// Map with ordering
type OrderedMap<K, V> = BTreeMap<K, V>;

#[cfg(feature = "yaml")]
pub const META_FILE_NAME: &str = "meta.yaml";

#[cfg(feature = "toml")]
pub const META_FILE_NAME: &str = "meta.toml";

#[cfg(feature = "json")]
pub const META_FILE_NAME: &str = "meta.json";

const FILE_ATTR: &str = "f";
const DIR_ATTR: &str = "d";
const SYMLINK_ATTR: &str = "s";

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
#[serde(tag="filetype")]
pub enum FileType {
    Dir,
    File,
    Link(String),
    //Link{
    //    //#[serde(rename="link")]
    //    to: String
    //},
}

impl FileType {
    pub fn is_dir(&self) -> bool {
        matches!(self, FileType::Dir)
    }
    pub fn is_file(&self) -> bool {
        matches!(self, FileType::File)
    }
    pub fn is_link(&self) -> bool {
        matches!(self, FileType::Link(_))
        //matches!(self, FileType::Link{to})
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

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct MetaData {
    //#[serde(flatten)]
    //pub id: PackageID,
    pub name: String,
    pub version: Version,
    pub mount: Option<String>,
    pub data_hash: Option<String>,
    pub dependencies: OrderedMap<PkgName, Option<Version>>,
    pub files: OrderedMap<FilePath, FileInfo>,
}

#[derive(Serialize, Deserialize)]
#[serde(transparent)]
struct FileInfoString(String);

impl From<FileInfo> for FileInfoString {
    fn from(info: FileInfo) -> Self {
        let mut ret = String::new();
        match info.filetype {
            FileType::Dir => ret.push_str(DIR_ATTR),
            FileType::File => ret.push_str(FILE_ATTR),
            FileType::Link(to) => {
            //FileType::Link{to} => {
                ret.push_str(SYMLINK_ATTR);
                ret.push(':');
                ret.push_str(&to);
            }
        }
        if let Some(hash) = info.hash {
            ret.push(':');
            ret.push_str(&hash);
        }
        Self(ret)
    }
}

impl TryFrom<FileInfoString> for FileInfo {
    type Error = String;
    fn try_from(s: FileInfoString) -> Result<Self, Self::Error> {
        let s = s.0;
        let mut iter = s.split(':');
        let ft = iter.next();
        let ft = match ft {
            Some(FILE_ATTR) => Some(FileType::File),
            Some(DIR_ATTR) => Some(FileType::Dir),
            Some(SYMLINK_ATTR) => {
                let to = iter.next().map(str::to_owned);
                to.map(FileType::Link)
                //to.map(|s| FileType::Link{to: s})
            }
            Some(_) | None => None,
        };
        if ft.is_none() {
            return Err(s);
        }

        let hash = iter.next().map(str::to_owned);
        let ret = Self {
            filetype: ft.unwrap(),
            hash,
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
            dependencies: OrderedMap::new(),
            files: OrderedMap::new(),
        }
    }

    pub fn id(&self) -> PackageID {
        PackageID {
            name: self.name.clone(),
            version: self.version.clone(),
        }
    }

    #[cfg(feature = "toml")]
    pub fn to_writer<W: Write>(&self, w: &mut W) -> Result<()> {
        //let s = toml::to_string(&self)?;
        let s = toml::to_string_pretty(&self)?;
        w.write_all(s.as_bytes())?;
        Ok(())
    }

    #[cfg(feature = "toml")]
    pub fn from_reader<R: Read>(r: &mut R) -> Result<Self> {
        let mut contents = String::new();
        r.read_to_string(&mut contents)?;
        let meta = toml::from_str::<Self>(&contents)?;
        //dbg!(&meta);
        Ok(meta)
    }

    #[cfg(feature = "yaml")]
    pub fn to_writer<W: Write>(&self, w: &mut W) -> Result<()> {
        serde_yaml::to_writer(w, self)?;
        Ok(())
    }

    #[cfg(feature = "yaml")]
    pub fn from_reader<R: Read>(r: &mut R) -> Result<Self> {
        let ret: Self = serde_yaml::from_reader(r)?;
        Ok(ret)
    }

    #[cfg(feature = "json")]
    pub fn to_writer<W: Write>(&self, w: &mut W) -> Result<()> {
        //serde_json::to_writer(w, self)?;
        serde_json::to_writer_pretty(w, self)?;
        Ok(())
    }

    #[cfg(feature = "json")]
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

    //pub fn set_data_hash(&mut self, hash: String) {
    //self.data_hash = Some(hash);
    //}
}

pub fn seek_to_tar_entry<'a, R>(needle: &str, tar: &'a mut tar::Archive<R>) -> Result<tar::Entry<'a, R>>
    where R: Read + Seek
{
    for entry in tar.entries_with_seek()? {
        if let Ok(path) = entry.as_ref().unwrap().path() && path == Path::new(needle) {
            return Ok(entry?);
        }
    }
    Err(anyhow::anyhow!("path {} not found in tar archive", needle))
}

pub fn get_metadata(pkg_file: &mut File) -> Result<MetaData> {
    pkg_file.rewind()?;
    let mut tar = tar::Archive::new(pkg_file);
    let mut meta = seek_to_tar_entry(META_FILE_NAME, &mut tar)?;
    let metadata = MetaData::from_reader(&mut meta)?;
    Ok(metadata)
}

pub fn get_filelist(pkg_file: &mut File) -> Result<OrderedMap<FilePath, FileInfo>> {
    let metadata = get_metadata(pkg_file)?;
    Ok(metadata.files)
}

/// "foo-1.2.3" -> ("foo", "1.2.3")
/// "foo-1.2.3.bpm" -> ("foo", "1.2.3")
pub fn split_parts(filename: &str) -> Option<(&str, &str)> {
    filename.split_once('-').map(|(name, mut version)| {
        if version.ends_with(DOTTED_PKG_FILE_EXTENSION) {
            version = version.strip_suffix(DOTTED_PKG_FILE_EXTENSION).unwrap();
        }
        (name, version)
    })
}

/// Names like "foo-1.0.0.bpm" and "bar-0.2.1.bpm" are packagefile names
pub fn is_packagefile_name(text: &str) -> bool {
    if text.ends_with(DOTTED_PKG_FILE_EXTENSION) {
        if let Some((name, version)) = split_parts(text) {
            if is_package_name(name) && is_version_string(version) {
                return true;
            }
        }
    }
    false
}

/// Names like "foo" and "bar" are package names
pub fn is_package_name(text: &str) -> bool {
    // cannot contain a '.' or '-'
    // cannot be empty string
    !text.contains('.') && !text.contains('-') && !text.is_empty()
}

/// strings like "1.2.3" and "0.0.1-alpha+linux" are version strings
pub fn is_version_string(text: &str) -> bool {
    // cannot be empty string
    // cannot contain the file extension
    !text.is_empty() && !text.contains(PKG_FILE_EXTENSION)
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

    fn get_instance() -> MetaData {
        let mut meta = MetaData {
            //id: PackageID {
                name: "foo".into(),
                version: "1.2.3".into(),
            //},
            mount: Some("EXT".into()),
            data_hash: None,
            dependencies: OrderedMap::new(),
            files: OrderedMap::new(),
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
            },
        );
        meta.add_file(
            "foo/a2.c".into(),
            FileInfo {
                filetype: FileType::File,
                hash: Some("2ffac14".into()),
            },
        );
        meta.add_file(
            "foo/a1.c".into(),
            FileInfo {
                filetype: FileType::File,
                hash: Some("1aef313".into()),
            },
        );
        meta.add_file(
            "foo/link".into(),
            FileInfo {
                filetype: FileType::Link("a1.c".into()),
                hash: Some("77af123".into()),
            },
        );

        meta
    }

    #[test]
    fn ser_de() -> Result<()> {
        let meta = get_instance();

        let mut output: Vec<u8> = Vec::new();
        meta.to_writer(&mut output)?;
        println!("{}", String::from_utf8_lossy(&output));
        let ret = MetaData::from_reader(&mut std::io::Cursor::new(output.as_slice()))?;
        dbg!(&ret);
        assert_eq!(meta, ret);
        Ok(())
    }
}
