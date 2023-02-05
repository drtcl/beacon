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

pub type PkgName = String;
pub type Version = String;
pub type FilePath = String;

/// Map with ordering
type OrderedMap<K, V> = BTreeMap<K, V>;

#[cfg(feature = "yaml")]
pub const META_FILE_NAME: &str = "meta.yaml";

#[cfg(feature = "toml")]
pub const META_FILE_NAME: &str = "meta.toml";

const FILE_ATTR: &str = "f";
const DIR_ATTR: &str = "d";
const SYMLINK_ATTR: &str = "s";

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
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
        matches!(self, FileType::Link(_))
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
//#[serde(try_from = "FileInfoString")]
//#[serde(into = "FileInfoString")]
pub struct FileInfo {
    #[serde(rename = "type")]
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
pub struct MetaData {
    #[serde(flatten)]
    pub id: PackageID,
    pub mount: Option<String>,
    pub data_hash: Option<String>,
    pub dependencies: OrderedMap<PkgName, Version>,
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
            id,
            mount: None,
            data_hash: None,
            dependencies: OrderedMap::new(),
            files: OrderedMap::new(),
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
        dbg!(&meta);
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

    pub fn add_dependency(&mut self, id: PackageID) {
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
    println!("seek_to_tar_entry {}", needle);
    for entry in tar.entries_with_seek()? {
        println!("entry {:?}", entry.as_ref().unwrap().path());
        if let Ok(path) = entry.as_ref().unwrap().path() && path == Path::new(needle) {
            return Ok(entry?);
        }
    }
    return Err(anyhow::anyhow!("path not found in tar archive"));
}

pub fn get_metadata(pkg_file: &mut File) -> Result<MetaData> {
    pkg_file.rewind()?;
    let mut tar = tar::Archive::new(pkg_file);
    let mut meta = seek_to_tar_entry(META_FILE_NAME, &mut tar)?;
    let metadata = MetaData::from_reader(&mut meta)?;
    println!("get_metadata {:?}", metadata);
    Ok(metadata)
}

pub fn get_filelist(pkg_file: &mut File) -> Result<OrderedMap<FilePath, FileInfo>> {
    let metadata = get_metadata(pkg_file)?;
    Ok(metadata.files)
}

#[cfg(test)]
mod test {
    use super::*;

    fn get_instance() -> MetaData {
        let mut meta = MetaData {
            id: PackageID {
                name: "foo".into(),
                version: "1.2.3".into(),
            },
            mount: Some("EXT".into()),
            data_hash: None,
            dependencies: OrderedMap::new(),
            files: OrderedMap::new(),
        };

        meta.add_dependency(PackageID {
            name: "bar".into(),
            version: "3.1.4".into(),
        });
        meta.add_dependency(PackageID {
            name: "baz".into(),
            version: "0.7.1".into(),
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
        assert_eq!(meta, ret);
        Ok(())
    }
}
