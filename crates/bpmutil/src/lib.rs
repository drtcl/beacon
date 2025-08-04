use anyhow::Result;
use camino::Utf8Path;
use std::fs::File;
use std::io::{Write, Read};
use std::time::Duration;

use anyhow::Context;

pub mod status;

/// Read all bytes from a [Read] and return a blake3 hash
pub fn blake3_hash_reader<R: Read>(mut read: R) -> std::io::Result<String> {
    let mut hasher = blake3::Hasher::new();
    let _ = std::io::copy(&mut read, &mut hasher)?;
    let hash = hasher.finalize().to_hex().to_string();
    Ok(hash)
}

/// Get the mtime of a file.
/// If the file is a symlink, this gives info about the symlink, not the target
pub fn get_mtime(path: impl AsRef<str>) -> Option<u64> {

    std::fs::symlink_metadata(path.as_ref()).ok()
        .and_then(|md| md.modified().ok())
        .and_then(|mtime| mtime.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|mtime| mtime.as_secs())
}

/// Get the size of a file.
/// If the file is a symlink, this gives info about the symlink, not the target
pub fn get_filesize(path: &str) -> std::io::Result<u64> {
    std::fs::symlink_metadata(path).map(|f| f.len())
}

#[derive(Debug, Default)]
pub struct FileState {
    pub missing: bool,
    pub link_missing: bool,
    pub file: bool,
    pub link: bool,
    pub dir: bool,
    pub mtime: Option<u64>,
}

pub fn get_filestate(path: &Utf8Path) -> FileState {

    let mut state = FileState::default();

    if !path.try_exists().unwrap_or(false) {
        state.missing = true;
        state.link_missing = true;

        // missing is inaccurate for symlinks, missing will be updated if the metadata can be fetched
    }

    if let Ok(md) = std::fs::symlink_metadata(path) {
        state.missing = false;
        state.dir = md.is_dir();
        state.link = md.is_symlink();
        state.file = md.is_file();

        if state.file {
            state.mtime = md.modified().ok()
                .and_then(|mtime| mtime.duration_since(std::time::UNIX_EPOCH).ok())
                .map(|mtime| mtime.as_secs());
        }
    }

    //println!("path {}, state {:?}", path, state);
    state
}

pub struct SlowWriter<T: Write> {
    inner: T,
    duration: std::time::Duration,
    //count: usize,
}
impl<T:Write> SlowWriter<T> {
    pub fn new(t: T, duration: std::time::Duration) -> Self {
        Self {
            inner: t,
            duration,
            //count: 4 * 1024 * 1024,
        }
    }
}
impl<T:Write> Write for SlowWriter<T> {
    fn write(&mut self, data: &[u8]) -> std::io::Result<usize> {
        //if data.len() > self.count {
            //return Err(std::io::Error::new(std::io::ErrorKind::Other, "bail"));
        //}
        //self.count -= data.len();
        std::thread::sleep(self.duration);
        self.inner.write(data)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

/// open a file for use as a lockfile
/// write permission is required
/// will create parent directory if needed
pub fn open_lockfile(path: &Utf8Path) -> Result<File> {

    if let Some(parent) = path.parent() {
        if !parent.exists() {
            if std::fs::create_dir_all(parent).is_ok() {
                tracing::debug!("created lockfile parent dir {}", parent);
            } else {
                tracing::warn!("could not create missing lockfile parent dir {}", parent);
            }
        }
    }

    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(path)
        .context("opening lockfile")?;
    Ok(file)
}

// rust 1.88.0 changed the API of these functions
// from -> Result<bool>
// to   -> Result<(), TryLockError>
//
// I prefer to use the older API, so these wrappers convert the return type back to the older API.
// We're going to stay this way until File locking is in +stable
pub trait MyLockFns {
    fn my_try_lock(&self) -> Result<bool, std::io::Error>;
    fn my_try_lock_shared(&self) -> Result<bool, std::io::Error>;
}

impl MyLockFns for std::fs::File {
    fn my_try_lock(&self) -> Result<bool, std::io::Error> {
        match self.try_lock() {
            Ok(()) => Ok(true),
            Err(std::fs::TryLockError::WouldBlock) => Ok(false),
            Err(std::fs::TryLockError::Error(e)) => Err(e),
        }
    }
    fn my_try_lock_shared(&self) -> Result<bool, std::io::Error> {
        match self.try_lock_shared() {
            Ok(()) => Ok(true),
            Err(std::fs::TryLockError::WouldBlock) => Ok(false),
            Err(std::fs::TryLockError::Error(e)) => Err(e),
        }
    }
}

/// parse a string like "1h30m20s" into a duration
pub fn parse_duration(s: &str) -> Result<Duration> {

    if let Ok(s) = s.parse::<u64>() {
        Ok(Duration::from_secs(s))
    } else if let Ok(d) = humantime::parse_duration(s) {
        Ok(d)
    } else {
        anyhow::bail!("invalid time string");
    }
}

pub fn parse_duration_base(s: Option<&str>, base: Duration) -> Result<Duration> {
    if let Some(s) = s {
        if let Ok(n) = s.parse::<u32>() {
            let d = base * n;
            Ok(d)
        } else {
            Ok(parse_duration(s)?)
        }
    } else {
        Ok(Duration::ZERO)
    }
}

#[cfg(test)]
mod test {

    use super::*;

    #[test]
    fn durations() {

        // no base
        assert_eq!(Duration::from_secs(90), parse_duration("1m30s").unwrap());
        assert_eq!(Duration::from_secs(90), parse_duration("90s").unwrap());
        assert_eq!(Duration::from_secs(300), parse_duration("300s").unwrap());
        assert_eq!(Duration::from_secs(300), parse_duration("5m").unwrap());
        assert_eq!(Duration::from_secs(300), parse_duration("5min").unwrap());
        assert_eq!(Duration::from_secs(60 * 60 + 30 * 60), parse_duration("1h30m").unwrap());
        assert_eq!(Duration::from_secs(60 * 60 + 30 * 60), parse_duration("90m").unwrap());
        assert_eq!(Duration::from_millis(20), parse_duration("20ms").unwrap());

        // 1 second base
        assert_eq!(Duration::from_secs(90), parse_duration_base(Some("90"), Duration::from_secs(1)).unwrap());
        assert_eq!(Duration::from_secs(90), parse_duration_base(Some("1m30s"), Duration::from_secs(1)).unwrap());

        // None = zero time
        assert_eq!(Duration::ZERO, parse_duration_base(None, Duration::from_secs(1)).unwrap());
        assert_eq!(Duration::ZERO, Duration::from_secs(0));
        assert_eq!(Duration::ZERO, Duration::from_micros(0));

        // 1 minute/hour base
        assert_eq!(Duration::from_secs(60), parse_duration_base(Some("1"), Duration::from_secs(60)).unwrap());
        assert_eq!(Duration::from_secs(60 * 60), parse_duration_base(Some("1"), Duration::from_secs(60 * 60)).unwrap());

        // weird bases
        assert_eq!(Duration::from_secs(90), parse_duration_base(Some("1"), Duration::from_secs(90)).unwrap());
        assert_eq!(Duration::from_secs(120), parse_duration_base(Some("2"), Duration::from_secs(60)).unwrap());
        assert_eq!(Duration::from_secs(24), parse_duration_base(Some("2"), Duration::from_secs(12)).unwrap());
    }
}
