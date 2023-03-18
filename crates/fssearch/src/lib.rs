//#![allow(dead_code)]
//#![allow(unused_variables)]

//#![feature(extract_if)]
#![feature(let_chains)]

use anyhow::Result;
use camino::Utf8PathBuf;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::path::Path;
use tracing::trace;

const CHANNEL_DIR_PREFIX : &str = "channel_";
const CHANNELS_FILE : &str = "channels.json";

type FilePath = String;
type PackageName = String;
type PackageVersion = String;
type PackagefileName = String;
type ChannelName = String;

// ChannelName -> [Version]
type ChannelList = HashMap<ChannelName, Vec<PackageVersion>>;

#[derive(Debug)]
pub struct VersionInfo {
    pub url: FilePath,
    pub filename: PackagefileName,
    pub channels: Vec<String>,
}

type VersionList = BTreeMap<PackageVersion, VersionInfo>;
type PackageList = BTreeMap<PackageName, VersionList>;

#[derive(Debug)]
struct Report {
    packages: PackageList,
}

impl Report {
    fn new() -> Self {
        Self {
            packages: PackageList::new(),
        }
    }
    fn add_version(&mut self, pkg_name: &str, version: &str, mut info: VersionInfo) {

        let mut channels = Vec::new();
        std::mem::swap(&mut info.channels, &mut channels);

        let entry = self.packages.entry(pkg_name.to_string())
            .or_default()
            .entry(version.to_string())
            .or_insert(info);

        for channel in channels {
            if !entry.channels.iter().any(|c| c == &channel) {
                entry.channels.push(channel.to_string());
            }
        }
    }
    fn add_channel_version(&mut self, pkg_name: &str, channel: &str, version: &str) {
        if let Some(vmap) = self.packages.get_mut(pkg_name) {
            if let Some(info) = vmap.get_mut(version) {
                if !info.channels.iter().any(|v| v == channel) {
                    info.channels.push(channel.to_string());
                }
            }
        }
    }
}

/// Scan a directory for packages
/// Accepted directory structures:
/// ```
/// 1) flat -- packages listed at root
///     pkg/
///         foo-1.0.0.bpm
///         foo-2.0.0.bpm
///         bar-0.1.0.bpm
///
/// 2) organized -- packages in directories of package name (allows for adding channels)
///     pkg/
///         foo/
///             foo-1.0.0.bpm
///             foo-2.0.0.bpm
///             channels.json      (optional)
///             channel_stable/    (optional)
///                 foo-3.0.0.bpm
///         bar/
///             bar-0.1.0.bpm
/// ```
pub fn full_scan(dir: &Path, filter_name: Option<&str>)-> Result<PackageList> {

    trace!(filter_name, dir=?dir, "full_scan");

    let mut report = Report::new();
    let mut overrides = ignore::overrides::OverrideBuilder::new(dir);

    if let Some(filter_name) = filter_name {
        overrides
            .add(&format!("/{}*.bpm", filter_name))?
            .add(&format!("/{}/{}*.bpm", filter_name, filter_name))?
            .add(&format!("/{}/channels.json", filter_name))?
            .add(&format!("/{}/channel_*/{}*.bpm", filter_name, filter_name))?;
    } else {
        overrides
            .add("/*.bpm").unwrap()
            .add("/*/*.bpm").unwrap()
            .add("/*/channels.json").unwrap()
            .add("/*/channel_*/*.bpm").unwrap();
    }

    let overrides = overrides.build()?;

    let walker = ignore::WalkBuilder::new(dir)
        .standard_filters(false)
        .hidden(true)
        .follow_links(true)
        .max_depth(Some(4))
        .skip_stdout(true)
        .overrides(overrides)
        .build();

    let mut channels_json_files = Vec::new();

    for entry in walker.into_iter().flatten() {

        let full_path = Utf8PathBuf::from_path_buf(entry.path().to_path_buf()).ok();
        if full_path.is_none() {
            continue;
        }
        let full_path = full_path.unwrap();

        let rel_path = full_path.strip_prefix(dir).ok();
        if rel_path.is_none() {
            continue;
        }
        let rel_path = rel_path.unwrap();

        let depth = entry.depth();
        let filename = full_path.file_name().expect("file had no filename");

        // skip directories
        if entry.file_type().unwrap().is_dir() {
            continue;
        }

        //println!("-- {}  {}", depth, rel_path);

        let is_valid_package_name = package::is_packagefile_name(filename);
        let is_channels_file = filename == CHANNELS_FILE;

        let parent_dir_path = full_path.parent();
        let parent_dir_name = parent_dir_path.and_then(|path| path.file_name());
        let in_channels_dir = parent_dir_name.map_or(false, |path| path.starts_with(CHANNEL_DIR_PREFIX));

        if depth == 2 && is_channels_file {

            // save channels.json files for later
            let pkg_name = parent_dir_name.unwrap();
            channels_json_files.push((pkg_name.to_string(), full_path.clone()));

        } else if let Some((pkg_name, pkg_version)) = package::split_parts(filename) {

            if depth == 1 && is_valid_package_name {

                // flat layout
                // pkg/foo-1.2.3.bpm

                report.add_version(pkg_name, pkg_version, VersionInfo{
                    url: full_path.to_string(),
                    filename: filename.to_string(),
                    channels: Vec::new(),
                });
                tracing::debug!("[f] found {}", rel_path);

            } else if depth == 2 && is_valid_package_name {

                // named layout
                // parent dir must be named the same
                // pkg/foo/foo-1.2.3.bpm

                if parent_dir_name == Some(pkg_name) {
                    report.add_version(pkg_name, pkg_version, VersionInfo{
                        url: full_path.to_string(),
                        filename: filename.to_string(),
                        channels: Vec::new(),
                    });
                    tracing::trace!("[n] found {}", rel_path);
                } else {
                    tracing::warn!("found package in wrong dir {}", full_path);
                }

            } else if depth == 3 && is_valid_package_name && in_channels_dir {

                // named layout with a channel_ dir
                // parent dir must be named the same
                // pkg/foo/channel_stable/foo-1.2.3.bpm

                let pkg_dir_name = parent_dir_path.and_then(|p| p.parent()).and_then(|p| p.file_name());
                let channel_name = parent_dir_name.and_then(|p| p.strip_prefix(CHANNEL_DIR_PREFIX));

                if let (Some(pkg_dir_name), Some(channel_name)) = (pkg_dir_name, channel_name) {
                    if pkg_dir_name == pkg_name {
                        report.add_version(pkg_name, pkg_version, VersionInfo{
                            url: full_path.to_string(),
                            filename: filename.to_string(),
                            channels: vec![channel_name.to_string()],
                        });
                        tracing::trace!("[c] found {}", rel_path);
                    } else {
                        tracing::warn!("found package in wrong dir {}", full_path);
                    }
                }
            }
        }
    }

    // now handle all the channels.json files
    // read the file contents, parse json, add channels to each listed version
    for (pkg_name, channels_json_path) in &channels_json_files {
        if let Ok(channels) = parse_channels(channels_json_path) {
            for (chan_name, versions) in channels {
                for v in versions {
                    report.add_channel_version(pkg_name, &chan_name, &v);
                }
            }
        }
    }

    //dbg!(&report);

    Ok(report.packages)
}

fn parse_channels<P: AsRef<Path>>(path: P) -> Result<ChannelList> {
    let contents = std::fs::read_to_string(path)?;
    let pkg_channels : ChannelList = serde_json::from_str(&contents)?;
    Ok(pkg_channels)
}
