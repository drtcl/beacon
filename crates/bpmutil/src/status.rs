use indicatif::MultiProgress;
use indicatif::ProgressBar;
use indicatif::ProgressBarIter;
use indicatif::ProgressStyle;
use indicatif::style::ProgressTracker;
use std::borrow::Cow;
use std::io::IsTerminal;
use std::io::Read;
use std::io::Write;
use std::sync::atomic::AtomicU32;
use std::sync::atomic::Ordering::SeqCst;
use std::sync::OnceLock;
use std::time::Duration;

const DEFAULT_PROGRESS_MODE_INTERACTIVE    : &str = "bars";
const DEFAULT_PROGRESS_MODE_NONINTERACTIVE : &str = "silent";
const DEFAULT_PROGRESS_PERIOD : Duration = Duration::from_millis(250);
const DEFAULT_PROGRESS_STREAM              : &str = "stderr";

#[allow(dead_code)]
#[derive(Debug)]
pub struct StatusMgr {
    bars: MultiProgress,
    stderr: bool,
    silent: bool,
    json: bool,
    prefix: Option<String>,
    next_id: AtomicU32,
    period: Duration,
}

#[allow(dead_code)]
#[derive(Debug)]
pub struct Task {
    inner: ProgressBar,
    json: bool,
    id: u32,
    stderr: bool,
}

#[allow(clippy::wildcard_in_or_patterns)]
pub fn global() -> &'static StatusMgr {

    static INSTANCE : OnceLock<StatusMgr> = OnceLock::new();
    INSTANCE.get_or_init(|| {

        let mut json = false;
        let mut silent = false;

        let stderr;
        let default_mode;
        match std::env::var("BPM_PROGRESS_STREAM").ok().as_deref().unwrap_or(DEFAULT_PROGRESS_STREAM) {
            "stdout" => {
                stderr = false;
                default_mode = if std::io::stdout().is_terminal() { DEFAULT_PROGRESS_MODE_INTERACTIVE } else { DEFAULT_PROGRESS_MODE_NONINTERACTIVE };
            }
            "stderr" | _ => {
                stderr = true;
                default_mode = if std::io::stderr().is_terminal() { DEFAULT_PROGRESS_MODE_INTERACTIVE } else { DEFAULT_PROGRESS_MODE_NONINTERACTIVE };
            }
        }

        match std::env::var("BPM_PROGRESS_MODE").ok().as_deref().unwrap_or(default_mode) {
            "bar"    |
            "bars"   => {}
            "text"   |
            "json"   => { json = true; }
            "silent" |
            "none"   |
            "off"    => { silent = true; }
            _        => { silent = true; }
        }

        let period = std::env::var("BPM_PROGRESS_PERIOD").ok()
            .and_then(|s| s.parse::<u64>().ok())
            .map(Duration::from_millis)
            .unwrap_or(DEFAULT_PROGRESS_PERIOD);

        let prefix = std::env::var("BPM_PROGRESS_PREFIX").ok();

        StatusMgr::new(stderr, silent, json, prefix, period)
    })
}

impl StatusMgr {
    pub fn new(stderr: bool, silent: bool, json: bool, prefix: Option<String>, period: Duration) -> Self {

        let bars = MultiProgress::new();

        if silent {
            bars.set_draw_target(indicatif::ProgressDrawTarget::hidden());
        } else if stderr {
            bars.set_draw_target(indicatif::ProgressDrawTarget::stderr());
        } else {
            bars.set_draw_target(indicatif::ProgressDrawTarget::stdout());
        }

        Self {
            bars,
            stderr,
            json,
            silent,
            next_id: AtomicU32::new(0),
            prefix,
            period,
        }
    }

    pub fn add_task<'a>(&self,
        name: Option<impl Into<Cow<'static, str>>>,
        package: Option<impl Into<Cow<'a, str>>>,
        len: Option<u64>
    ) -> Task {

        let bar = if let Some(len) = len {
            self.bars.add(ProgressBar::new(len))
        } else {
            self.bars.add(ProgressBar::no_length())
        };

        let id = self.next_id.fetch_add(1, SeqCst);

        let name = name.map(|v| v.into());

        if !self.silent && self.json {

            let tt = TextTracker{
                id,
                task: name,
                package: package.map(|p| p.into().into_owned()),
                time: std::time::Instant::now(),
                prefix: self.prefix.clone(),
                period: self.period,
                new: true,
                stderr: self.stderr,
            };

            let style = ProgressStyle::with_template("{bpm_custom_text_tracker}")
                .unwrap()
                .with_key("bpm_custom_text_tracker", tt);

            bar.set_style(style);
        }

        Task {
            id, inner: bar,
            json: self.json,
            stderr: self.stderr,
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
    pub fn bar(&self) -> &ProgressBar {
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
        if !self.json {
            self.inner.set_style(style);
        }
    }
    pub fn enable_steady_tick(&self, interval: Duration) {
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
    period: Duration,
    new: bool,
    stderr: bool,
    package: Option<String>,
}

impl ProgressTracker for TextTracker {

    fn clone_box(&self) -> Box<dyn ProgressTracker> {
        Box::new(self.clone())
    }

    fn tick(&mut self, state: &indicatif::ProgressState, now: std::time::Instant) {

        let mut print = false;
        if self.new {
            print = true;
        } else if self.time.elapsed() > self.period {
            print = true;
            self.time = now;
        }

        if !print {
            return;
        }

        let task_id = self.id;
        let task_name = self.task.as_deref().unwrap_or("null");
        let pos = state.pos();
        let len = state.len().unwrap_or(0);
        let duration = state.duration().as_secs();
        let elapsed = state.elapsed().as_secs();
        let eta = state.eta().as_secs();
        let rate = state.per_sec();

        let event = if self.new {
            self.new = false;
            "start"
        } else {
            "update"
        };

        let json = json::stringify(json::object!{
            "id": task_id,
            "event": event,
            "name": task_name,
            "package": self.package.as_deref().unwrap_or(""),
            "pos": pos,
            "len": len,
            "eta": eta,
            "elapsed": elapsed,
            "rate": rate,
            "duration": duration,
        });
        let prefix = self.prefix.as_deref().unwrap_or("");
        if self.stderr {
            eprintln!("{}{}", prefix, json);
        } else {
            println!("{}{}", prefix, json);
        }
    }

    fn reset(&mut self, _state: &indicatif::ProgressState, _now: std::time::Instant) {
    }

    fn write(&self, _state: &indicatif::ProgressState, _w: &mut dyn std::fmt::Write) {
    }
}

impl Drop for TextTracker {
    fn drop(&mut self) {
        let task_id = self.id;
        let prefix = self.prefix.as_deref().unwrap_or("");
        if self.stderr {
            eprintln!("{prefix}{{\"id\":{task_id},\"event\":\"end\"}}");
        } else {
            println!("{prefix}{{\"id\":{task_id},\"event\":\"end\"}}");
        }
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
        let bar1 = mgr.add_task(Some("one"), Some("foo"), Some(100));
        let bar2 = mgr.add_task(Some("two"), Some("bar"), Some(200));
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
        let mgr = StatusMgr::new(true, false, false, Some("not seen".into()), DEFAULT_PROGRESS_PERIOD);
        bars(mgr);
    }

    #[test]
    fn textprog() {
        let mgr = StatusMgr::new(true, false, true, Some("PROGRESS::".into()), DEFAULT_PROGRESS_PERIOD);
        bars(mgr);
    }
}
