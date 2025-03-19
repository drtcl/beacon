use std::sync::OnceLock;
use std::borrow::Cow;
use indicatif::MultiProgress;
use indicatif::ProgressBar;
use indicatif::ProgressBarIter;
use indicatif::ProgressStyle;
use std::sync::Arc;
use std::sync::atomic::AtomicU32;
use std::sync::atomic::Ordering::SeqCst;
use std::io::Write;
use std::io::Read;

#[allow(dead_code)]
#[derive(Debug)]
pub struct StatusMgr {
    bars: MultiProgress,
    text: bool,
    next_id: Arc<AtomicU32>,
    prefix: Option<String>,
}

#[allow(dead_code)]
#[derive(Debug)]
pub struct Task {
    inner: ProgressBar,
    text: bool,
    id: u32,

}

pub fn global() -> &'static StatusMgr {
    static INSTANCE : OnceLock<StatusMgr> = OnceLock::new();
    INSTANCE.get_or_init(|| {
        let json_mode = "1" == std::env::var("BPM_JSON_PROGRESS").ok().as_deref().unwrap_or("");
        let prefix = std::env::var("BPM_PROGRESS_PREFIX").ok();
        StatusMgr::new(json_mode, prefix)
    })
}

impl StatusMgr {
    pub fn new(textmode: bool, prefix: Option<String>) -> Self {
        Self {
            bars: MultiProgress::new(),
            text: textmode,
            next_id: Arc::new(AtomicU32::new(0)),
            prefix,
        }
    }
    pub fn add_task(&self, name: Option<impl Into<Cow<'static, str>>>, len: Option<u64>) -> Task {

        let bar = if let Some(len) = len {
            ProgressBar::new(len)
        } else {
            ProgressBar::no_length()
        };

        let bar = self.bars.add(bar);
        //bar.enable_steady_tick(std::time::Duration::from_millis(100));
        let id = self.next_id.fetch_add(1, SeqCst);

        let name = name.map(|v| v.into());

        if self.text {
            let style = indicatif::style::ProgressStyle::with_template("{bpm_custom_text_tracker}").unwrap()
                .with_key("bpm_custom_text_tracker", TextTracker{
                    id,
                    task: name,
                    time: std::time::Instant::now(),
                    prefix: self.prefix.clone(),
                });
            bar.set_style(style);
        }

        Task {
            id, inner: bar,
            text: self.text,
        }
    }

    pub fn remove(&self, task: &Task) {
        self.bars.remove(task.bar());
    }

    pub fn insert(&self, index: usize, task: &mut Task) {
        let bar = task.take_bar();
        let bar = self.bars.insert(index, bar);
        task.put_bar(bar);
    }

    pub fn suspend<F: FnOnce() -> R, R>(&self, f: F) -> R {
        self.bars.suspend(f)
    }
}

#[allow(dead_code)]
impl Task {
    fn take_bar(&mut self) -> ProgressBar {
        let bar = std::mem::replace(&mut self.inner, ProgressBar::hidden());
        bar
    }
    fn put_bar(&mut self, bar: ProgressBar) {
        let _old = std::mem::replace(&mut self.inner, bar);
    }
    fn bar(&self) -> &ProgressBar {
        &self.inner
    }
    pub fn id(&self) -> u32 {
        self.id
    }
    pub fn inc(&self, delta: u64) {
        self.inner.inc(delta);
    }
    pub fn set_message(&self, msg: impl Into<Cow<'static, str>>) {
        self.inner.set_message(msg);
    }
    pub fn set_prefix(&self, msg: impl Into<Cow<'static, str>>) {
        self.inner.set_prefix(msg);
    }
    pub fn set_position(&self, pos: u64) {
        self.inner.set_position(pos);
    }
    pub fn set_length(&self, len: u64) {
        self.inner.set_length(len);
    }
    pub fn inc_length(&self, delta: u64) {
        self.inner.inc_length(delta);
    }
    pub fn finish(&self) {
        self.inner.finish();
    }
    pub fn finish_and_clear(&self) {
        self.inner.finish_and_clear();
    }
    pub fn set_style(&self, style: ProgressStyle) {
        if !self.text {
            self.inner.set_style(style);
        }
    }
    pub fn enable_steady_tick(&self, interval: std::time::Duration) {
        self.inner.enable_steady_tick(interval)
    }
    pub fn disable_steady_tick(&self) {
        self.inner.disable_steady_tick()
    }
    pub fn wrap_write<W: Write>(&self, write: W) -> ProgressBarIter<W> {
        self.inner.wrap_write(write)
    }
    pub fn wrap_read<R: Read>(&self, read: R) -> ProgressBarIter<R> {
        self.inner.wrap_read(read)
    }
    pub fn wrap_iter<I: Iterator>(&self, it: I) -> ProgressBarIter<I> {
        self.inner.wrap_iter(it)
    }
}

impl Drop for Task {
    fn drop(&mut self) {
        if !self.inner.is_finished() {
            self.inner.finish_and_clear();
        }
    }
}

pub enum WrapWrite<W> {
    Bar(ProgressBarIter<W>),
    Raw(W),
}

impl<W: Write> Write for WrapWrite<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match self {
            Self::Bar(inner) => inner.write(buf),
            Self::Raw(inner) => inner.write(buf),
        }
    }
    fn flush(&mut self) -> std::io::Result<()> {
        match self {
            Self::Bar(inner) => inner.flush(),
            Self::Raw(inner) => inner.flush(),
        }
    }
}

pub enum WrapRead<R> {
    Bar(ProgressBarIter<R>),
    Raw(R),
}

impl<R: Read> Read for WrapRead<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self {
            Self::Bar(inner) => inner.read(buf),
            Self::Raw(inner) => inner.read(buf),
        }
    }
}

pub fn wrap_write<W: Write>(task: Option<&Task>, write: W) -> WrapWrite<W> {
    match task {
        None => WrapWrite::Raw(write),
        Some(task) => WrapWrite::Bar(task.wrap_write(write)),
    }
}

pub fn wrap_read<R: Read>(task: Option<&Task>, read: R) -> WrapRead<R> {
    match task {
        None => WrapRead::Raw(read),
        Some(task) => WrapRead::Bar(task.wrap_read(read)),
    }
}

#[derive(Clone)]
struct TextTracker {
    id: u32,
    task: Option<Cow<'static, str>>,
    time: std::time::Instant,
    prefix: Option<String>,
}

impl indicatif::style::ProgressTracker for TextTracker {
    fn clone_box(&self) -> Box<dyn indicatif::style::ProgressTracker> {
        Box::new(self.clone())
    }
    fn tick(&mut self, state: &indicatif::ProgressState, _now: std::time::Instant) {
        let task_id = self.id;
        let task_name = self.task.as_deref().unwrap_or("null");
        let pos = state.pos();
        let len = state.len().unwrap_or(0);
        let duration = state.duration().as_secs();
        let elapsed = state.elapsed().as_secs();
        let eta = state.eta().as_secs();
        //let per_sec = state.per_sec();
        let mut print = false;
        if self.time.elapsed() > std::time::Duration::from_millis(250) {
            print = true;
            self.time = _now;
        }
        if !print && (pos == len) {
            print = true;
        }
        if print {
            let json = json::object!{
                "id": task_id,
                "name": task_name,
                "pos": pos,
                "len": len,
                "eta": eta,
                "elapsed": elapsed,
                "duration": duration,
            };
            let prefix = self.prefix.as_deref().unwrap_or("");
            eprintln!("{}{}", prefix, json::stringify(json));
        }
    }
    fn reset(&mut self, _state: &indicatif::ProgressState, _now: std::time::Instant) {
    }
    fn write(&self, _state: &indicatif::ProgressState, _w: &mut dyn std::fmt::Write) {
    }
}

#[cfg(test)]
mod test {

    use super::*;

    #[allow(deprecated)]
    fn sleep(ms: u32) {
        std::thread::sleep_ms(ms);
    }

    fn bars(mgr: StatusMgr) {
        let bar1 = mgr.add_task(Some("one"), Some(100));
        let bar2 = mgr.add_task(Some("two"), Some(200));
        bar1.set_style(ProgressStyle::with_template("{spinner:.red} {wide_bar:.red/blue} {pos}/{len}").unwrap());
        bar2.set_style(ProgressStyle::with_template("{spinner:.green} {wide_bar:.blue/green} {pos}/{len}").unwrap());

        let t1 = std::thread::spawn(move || {
            for _ in 0..100 {
                sleep(20);
                bar1.inc(1);
            }
            bar1.finish_and_clear();
        });
        let t2 = std::thread::spawn(move || {
            for _ in 0..200 {
                sleep(20);
                bar2.inc(1);
            }
            bar2.finish_and_clear();
        });

        let _ = t1.join();
        let _ = t2.join();
    }

    #[test]
    fn multibar() {
        let mgr = StatusMgr::new(false, Some("not seen".into()));
        bars(mgr);
    }

    #[test]
    fn textprog() {
        let mgr = StatusMgr::new(true, Some("PROGRESS::".into()));
        bars(mgr);
    }
}
