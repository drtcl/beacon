use std::io::{Write, Read};
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
        .map(|mtime| mtime.as_secs())
}

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

//pub struct CustomProgress {
//    pub bar: indicatif::ProgressBar,
//    auto_clear: bool,
//}
//
//impl Drop for CustomProgress {
//    fn drop(&mut self) {
//        if self.auto_clear {
//            self.bar.finish_and_clear();
//        }
//    }
//}
//
//impl CustomProgress {
//    pub fn new_bar(bar: indicatif::ProgressBar) -> Self {
//        Self { bar, auto_clear: false }
//    }
//    pub fn new(len: u64) -> Self {
//        Self { bar: indicatif::ProgressBar::new(len), auto_clear: false }
//    }
//    pub fn new_style(len: u64, style: &str) -> Self {
//        let bar = indicatif::ProgressBar::new(len);
//        bar.set_style(indicatif::ProgressStyle::with_template(style).expect("bad progress style"));
//        Self { bar, auto_clear: false }
//    }
//    pub fn hide(&self) {
//        self.bar.set_draw_target(indicatif::ProgressDrawTarget::hidden());
//    }
//    pub fn show(&self) {
//        self.bar.set_draw_target(indicatif::ProgressDrawTarget::stderr());
//    }
//    pub fn auto_clear(&mut self, yes: bool) {
//        self.auto_clear = yes;
//    }
//}

pub struct SlowWriter<T: Write> {
    inner: T,
    duration: std::time::Duration,
}
impl<T:Write> SlowWriter<T> {
    pub fn new(t: T, duration: std::time::Duration) -> Self {
        Self {
            inner: t,
            duration,
        }
    }
}
impl<T:Write> Write for SlowWriter<T> {
    fn write(&mut self, data: &[u8]) -> std::io::Result<usize> {
        std::thread::sleep(self.duration);
        self.inner.write(data)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}
