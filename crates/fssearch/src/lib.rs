//#![allow(dead_code)]
//#![allow(unused_variables)]

//#![feature(extract_if)]
#![feature(let_chains)]

use std::collections::HashMap;
use anyhow::Result;
use tracing::trace;
use std::collections::BTreeMap;
use std::path::Path;

type FilePath = String;
type PackageName = String;
type PackageVersion = String;
type PackagefileName = String;
type ChannelName = String;

// ChannelName -> [Version]
type ChannelList = HashMap<ChannelName, Vec<PackageVersion>>;

/// PkgName -> ChannelList
type AllPackagesChannelList = HashMap<PackageName, ChannelList>;

#[derive(Debug)]
pub struct VersionInfo {
    pub url: FilePath,
    pub filename: PackagefileName,
    pub channels: Vec<String>,
}

type VersionList = BTreeMap<PackageVersion, VersionInfo>;
type PackageList = BTreeMap<PackageName, VersionList>;

/// Scan a directory for packages
/// Accepted directory structures:
/// ```
/// 1) flat -- packages listed at root
///     pkg/
///         foo-1.0.0.bpm
///         foo-2.0.0.bpm
///         bar-0.1.0.bpm
///
/// 2) organized -- packages in directories of package name
///     pkg/
///         foo/
///             foo-1.0.0.bpm
///             foo-2.0.0.bpm
///             channels.json
///         bar/
///             bar-0.1.0.bpm
///
/// 3) organized-versioned -- packages in named and versioned directories
///     pkg/
///         foo/
///             1.0.0/
///                 foo-1.0.0.bpm
///             2.0.0/
///                 foo-2.0.0.bpm
///         bar/
///             0.1.0/
///                 bar-0.1.0.bpm
/// ```
pub fn full_scan(dir: &Path)-> Result<PackageList> {

    trace!(dir=?dir, "full_scan");

    let walker = walkdir::WalkDir::new(dir)
        .max_depth(4)
        .follow_links(true)
        .sort_by_file_name();


    let mut packages = PackageList::new();
    let mut channels = AllPackagesChannelList::new();

    for entry in walker {

        let entry = entry?;

        let full_path = entry.path();
        let rel_path = full_path.strip_prefix(dir).ok();

        if let Some(rel_path) = rel_path && !rel_path.as_os_str().is_empty() {

            let depth = entry.depth();
            let filename = entry.file_name();
            let filename = filename.to_string_lossy().to_string();
            let is_file = entry.file_type().is_file();

            let is_valid_package_name = package::is_packagefile_name(&filename);
            let is_channels_file = filename == "channels.json";

            if is_channels_file {
                if depth == 2 {
                    if let Some(pkg_name) = rel_path.parent() {
                        if let Ok(pkg_channels) = parse_channels(full_path) {
                            channels.insert(pkg_name.to_string_lossy().into_owned(), pkg_channels);
                        }
                    }
                }
                continue;
            }

            let mut save = false;

            if let Some((pkg_name, pkg_version)) = package::split_parts(&filename) {

                if depth == 1 && is_file && is_valid_package_name {
                    // flat layout
                    tracing::debug!("[f] found {}", filename);
                    save = true;

                } else if depth == 2 && is_file && is_valid_package_name {
                    // named layout
                    // parent dir must be named the same
                    if let Some(parent_path) = full_path.parent() {
                        if let Some(parent_name) = parent_path.file_name() {
                            let parent_name = parent_name.to_string_lossy();
                            if pkg_name == parent_name {
                                tracing::debug!("[n] found {}", filename);
                                save = true;
                            }
                        }
                    }

                } else if depth == 3 && is_file && is_valid_package_name {
                    // versioned layout
                    // parent-parent dir must be named the same
                    // parent dir must be the same version
                    let mut parent_version = None;
                    let mut parent_name = None;

                    if let Some(version_parent_path) = full_path.parent() {
                        if let Some(version_parent_name) = version_parent_path.file_name() {
                            parent_version = Some(version_parent_name.to_string_lossy());

                            if let Some(named_parent_path) = version_parent_path.parent() {
                                if let Some(named_parent_name) = named_parent_path.file_name() {
                                    parent_name = Some(named_parent_name.to_string_lossy());
                                }
                            }
                        }
                    }

                    if Some(pkg_version) == parent_version.as_deref() && Some(pkg_name) == parent_name.as_deref() {
                        tracing::debug!("[v] found {}", filename);
                        save = true;
                    }
                }

                if save {
                    packages.entry(pkg_name.to_string())
                        .or_default()
                        .insert(pkg_version.to_string(), VersionInfo {
                            url: full_path.to_string_lossy().to_string(),
                            filename,
                            channels: Vec::with_capacity(0),
                        });
                }
            }

        }
    }

    apply_channels(&channels, &mut packages);

    Ok(packages)
}

pub fn parse_channels<P: AsRef<Path>>(path: P) -> Result<ChannelList> {

    let contents = std::fs::read_to_string(path)?;
    let pkg_channels : ChannelList = serde_json::from_str(&contents)?;
    Ok(pkg_channels)
}

fn apply_channels(channels: &AllPackagesChannelList, packages: &mut PackageList) {

    for (pkg_name, chans) in channels {
        if let Some(map) = packages.get_mut(pkg_name) {
            for (chan_name, versions) in chans {
                for version in versions {
                    if let Some(listing) = map.get_mut(version) {
                        if !listing.channels.contains(chan_name) {
                            listing.channels.push(chan_name.clone());
                        }
                    }
                }
            }
        }
    }
}
