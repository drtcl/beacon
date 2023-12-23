use std::io::Read;
use camino::Utf8Path;

/// Read all bytes from a [Read] and return a blake3 hash
pub fn blake3_hash_reader<R: Read>(mut read: R) -> std::io::Result<String> {
    let mut hasher = blake3::Hasher::new();
    let _ = std::io::copy(&mut read, &mut hasher)?;
    let hash = hasher.finalize().to_hex().to_string();
    Ok(hash)
}

pub fn get_mtime(path: impl AsRef<str>) -> Option<u64> {

    std::fs::symlink_metadata(path.as_ref()).ok()
        .and_then(|md| md.modified().ok())
        .and_then(|mtime| mtime.duration_since(std::time::UNIX_EPOCH).ok())
        .and_then(|mtime| Some(mtime.as_secs()))
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
                .and_then(|mtime| Some(mtime.as_secs()));
        }
    }

    //println!("path {}, state {:?}", path, state);
    state
}
