use anyhow::Context;
use blake2::{Blake2b, Digest};
use crate::AResult;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::File;
use std::io::{Read, Seek};
use std::path::Path;
use std::path::PathBuf;

fn semver_ser<S>(version: &semver::Version, s: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    s.serialize_str(&version.to_string())
}

fn semver_de<'de, D>(d: D) -> Result<semver::Version, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s = String::deserialize(d)?;
    let ver = semver::Version::parse(&s);
    if let Ok(ver) = ver {
        return Ok(ver);
    }
    Ok(semver::Version::new(1, 2, 3))
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PackageID {
    pub name: String,

    #[serde(serialize_with = "semver_ser")]
    #[serde(deserialize_with = "semver_de")]
    pub version: semver::Version,
}

pub fn seek_to_tar_entry<'a, R: Read + Seek>(
    needle: &str,
    tar: &'a mut tar::Archive<R>,
) -> AResult<tar::Entry<'a, R>> {
    for entry in tar.entries_with_seek()? {
        if let Ok(path) = entry.as_ref().unwrap().path() && path == Path::new(needle) {
            return Ok(entry?);
        }
    }
    return Err(anyhow::anyhow!("path not found in tar archive"));
}

type FilePath = String;
type FileHash = String;
type FileHashes = HashMap<FilePath, FileHash>;

pub fn get_filelist(pkg_file: &mut File) -> AResult<FileHashes> {
    pkg_file.rewind()?;
    let mut tar = tar::Archive::new(pkg_file);
    let mut meta = seek_to_tar_entry("meta.toml", &mut tar)?;

    let mut contents = String::new();
    meta.read_to_string(&mut contents)?;

    let toml = toml::from_str::<toml::Value>(contents.as_str()).unwrap();
    let files = toml.get("files").context("no file listing in package")?;

    if let toml::Value::Table(files) = files {
        let mut ok = true;
        let files = files
            .iter()
            .map(|(k, v)| {
                if v.is_str() {
                    (k.to_string(), String::from(v.as_str().unwrap()))
                } else {
                    ok = false;
                    (k.to_string(), String::new())
                }
            })
            .collect();
        if !ok {
            return Err(anyhow::anyhow!(
                "package metadata contains an invalid file hash"
            ));
        }
        return Ok(files);
    } else {
        return Err(anyhow::anyhow!("no files listing in package"));
    }
}

pub fn get_datachecksum(pkg_file: &mut File) -> AResult<String> {
    pkg_file.rewind()?;
    let mut tar = tar::Archive::new(pkg_file);
    let mut meta = seek_to_tar_entry("meta.toml", &mut tar)?;

    let mut contents = String::new();
    meta.read_to_string(&mut contents)?;

    let toml = toml::from_str::<toml::Value>(contents.as_str())?;

    let sum = toml
        .try_into::<toml::value::Table>()
        .context("package metadata is malformed")?
        .remove("package")
        .context("package metadata is malformed")?
        .try_into::<toml::value::Table>()
        .context("package metadata is malformed")?
        .remove("data_cksum")
        .context("package metadata is missing data checksum")?
        .try_into::<String>()
        .context("package is missing data checksum")?;

    Ok(sum)
}

pub fn check_datachecksum(pkg_file: &mut File) -> AResult<bool> {
    let described_sum = get_datachecksum(pkg_file)?;

    pkg_file.rewind()?;
    let mut tar = tar::Archive::new(pkg_file);
    let mut data = seek_to_tar_entry("data.tar.zst", &mut tar)?;

    let computed_sum = blake2_hash_reader(&mut data)?;
    println!(
        "computed data hash {} matches:{}",
        computed_sum,
        computed_sum == described_sum
    );

    Ok(computed_sum == described_sum)
}

pub fn blake2_hash_reader<R: Read>(mut read: R) -> std::io::Result<String> {
    let mut space = [0u8; 1024];
    let mut blake2 = Blake2b::new();
    //let mut len = 0;
    loop {
        match read.read(&mut space) {
            Ok(n) if n > 0 => {
                blake2.update(&space[0..n]);
                //len += n;
            }
            Ok(_) => {
                break;
            }
            Err(e) => {
                return Err(e);
            }
        }
    }
    //println!("read {} bytes", len);
    let hash = blake2.finalize();
    let hash = hex_string(hash.as_slice());
    return Ok(hash);
}

pub fn hex_string(data: &[u8]) -> String {
    let mut s = String::new();
    for byte in data {
        s += &format!("{byte:02x}");
    }
    s
}

pub fn name_parts(name: &str) -> Option<(&str, semver::Version)> {
    const PKG_EXT: &str = ".bpm.tar";

    if !name.ends_with(PKG_EXT) {
        return None;
    }

    let name = name.strip_suffix(PKG_EXT).unwrap();
    let mut split = name.split('-');

    let pkg_name = split.next();
    let version = split.next();
    if split.next().is_some() {
        return None;
    }

    //dbg!(&name);
    //dbg!(&pkg_name);
    //dbg!(&version);

    match (pkg_name, version) {
        (Some(name), Some(ver_str)) => {
            if let Ok(ver) = semver::Version::parse(ver_str) {
                Some((name, ver))
            } else {
                None
            }
        }
        _ => None,
    }
}

/// check if a package file name is formed correctly
/// must be `{PKG_NAME}-{SEMVER}.bpm.tar`
///
pub fn named_properly(name: &str) -> bool {
    name_parts(name).is_some()
}

pub fn to_filename(pkg_name: &str, version: &semver::Version) -> String {
    format!("{pkg_name}-{version}.bpm.tar")
}

pub fn to_filepath(mut dir: PathBuf, pkg_name: &str, version: &semver::Version) -> PathBuf {
    dir.push(to_filename(pkg_name, version));
    dir
}
