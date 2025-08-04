use anyhow::Context;
use camino::Utf8PathBuf;
use camino::Utf8Path;
use crate::AResult;
use crate::provider::Provider;
use serde::{Serialize, Deserialize};
use std::collections::HashMap;
use std::fs::File;
use std::io::BufReader;
use std::io::Read;
use std::path::Path;
use std::sync::OnceLock;

// notes:
// 0.3.0 multi-path providers were removed

static CONFIG_PATH: OnceLock<Utf8PathBuf> = OnceLock::new();

pub fn store_config_path(path: Utf8PathBuf) {
    CONFIG_PATH.get_or_init(|| path);
}

pub fn get_config_path() -> Option<&'static Utf8PathBuf> {
    CONFIG_PATH.get()
}

pub fn get_config_dir() -> Option<&'static camino::Utf8Path> {
    if let Some(path) = get_config_path() {
        path.parent()
    } else {
        None
    }
}

/// the main config struct
#[derive(Debug)]
pub struct Config {
    pub db_file: Utf8PathBuf,
    pub lockfile: Option<Utf8PathBuf>,
    pub cache_dir: Utf8PathBuf,
    pub arch_filter: Option<Vec<String>>,
    pub cache_retention: std::time::Duration,
    pub cache_fetch_jobs: u8,
    pub cache_touch_on_uninstall: bool,
    pub cache_auto_clean: bool,
    pub scan_threads: u8,
    pub scan_debounce: std::time::Duration,
    pub providers: Vec<Provider>,
    pub mount: MountConfig,
}

#[derive(Debug)]
pub struct MountConfig {

    /// name of default mount
    pub default_target: Option<String>,

    /// [(name, path)]
    pub mounts: Vec<(String, PathType)>,
}

#[derive(Debug)]
pub enum MountPoint {

    /// specified by the package itself
    Specified(PathType),

    /// the default target
    Default(PathType),

    /// specific by user at install time
    User(PathType),

    /// package tried to use default target, but default is disabled
    DefaultDisabled,

    /// invalid mount, name not found
    Invalid {
        name: String,
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RelativeToAnchor {
    Bpm,
    Config,
}

impl RelativeToAnchor {
    pub fn path(&self) -> Option<Utf8PathBuf> {
        match self {
            Self::Bpm => {
                std::env::current_exe()
                    .map(|p| p.parent().map(Path::to_path_buf))
                    .ok()
                    .flatten()
                    .and_then(|p| Utf8PathBuf::from_path_buf(p).ok())

            },
            Self::Config => {
                get_config_dir().map(ToOwned::to_owned)
            }
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum PathType {
    Relative{
        path: Utf8PathBuf,
        relative_to: RelativeToAnchor,
    },
    #[serde(untagged)]
    Absolute(Utf8PathBuf),
}

impl PathType {
    pub fn full_path(&self) -> anyhow::Result<Utf8PathBuf> {
        match self {
            Self::Relative{path, relative_to} => {
                if let Some(rel) = relative_to.path() {
                    Ok(crate::join_path_utf8!(rel, path))
                } else {
                    anyhow::bail!("No relative path anchor")
                }
            }
            Self::Absolute(path) => Ok(path.to_owned())
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct ConfigToml {
    database: String,
    lockfile: Option<String>,
    providers: indexmap::IndexMap<String, ProviderToml>,
    mount: HashMap<String, MountToml>,
    cache: CacheToml,
    scan: Option<ScanToml>,
    arch: Option<ArchToml>,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum ArchToml {
    Single(String),
    Multiple(Vec<String>),
}

//impl ArchToml {
//    fn get(self) -> Vec<String> {
//        match self {
//            Self::Single(val) => vec![val],
//            Self::Multiple(vals) => vals,
//        }
//    }
//}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum MountToml {
    JustPath(String),
    Table {
        default: bool,
        path: String,
    },
}

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum ProviderToml {
    Path(String),
    Table {
        path: String,

        //#[serde(default)]
        note: Option<String>,
    },
}

#[derive(Debug, Deserialize)]
pub struct CacheToml {
    dir: String,

    retention: String,

    #[serde(default = "bool::default")]
    auto_clean: bool,

    #[serde(default = "bool::default")]
    touch_on_uninstall: bool,

    fetch_jobs: Option<u8>,
}

#[derive(Debug, Deserialize)]
pub struct ScanToml {

    /// max number of threads to use, 0 means cpu count
    threads: Option<u8>,

    /// default --debounce value. example strings "20m" == "20min", "1h30m", "30s"
    debounce: Option<String>,
}

impl Config {

    pub fn from_reader<R: Read>(mut read: R) -> AResult<Config> {

        let toml = {
            let mut contents = String::new();
            read.read_to_string(&mut contents)?;
            toml::from_str::<ConfigToml>(&contents).context("failed to parse config")?
        };

        let db_file = path_replace(toml.database)?.full_path()?;

        let lockfile = if let Some(lockfile) = toml.lockfile {
            Some(path_replace(lockfile)?.full_path()?)
        } else {
            None
        };

        let cache_dir = path_replace(toml.cache.dir)?.full_path()?;
        let cache_retention = humantime::parse_duration(&toml.cache.retention).context("invalid cache retention")?;
        let cache_fetch_jobs = toml.cache.fetch_jobs.unwrap_or(1);

        let scan_debounce = bpmutil::parse_duration_base(
            toml.scan.as_ref().and_then(|x| x.debounce.as_ref()).map(|s| s.as_str()),
            std::time::Duration::from_secs(1)
        )?;

        let mut arch = toml.arch.map(|e| {
            match e {
                ArchToml::Single(v) => vec![v],
                ArchToml::Multiple(v) => v,
            }
        }).unwrap_or(vec![String::from("noarch")]);
        // change "" into "noarch" and do config replacements
        for ent in &mut arch {
            if ent.is_empty() {
                *ent = "noarch".into();
            } else {
                config_replace(ent);
            }
        }
        // remove:
        //  - any ""
        //  - any duplicates while preserving order, first occurance is kept
        //  - any invalid arch string
        // an error is returned if, after removal, there are no valid archs in the list
        let mut idx = 0;
        while idx < arch.len() {
            if arch[idx].is_empty() || arch[0..idx].contains(&arch[idx]) || !package::is_valid_arch(Some(arch[idx].as_str())) {
                arch.remove(idx);
            } else {
                idx += 1;
            }
        }
        if arch.is_empty() {
            anyhow::bail!("configuration error: no valid arch");
        }

        let mut mounts = Vec::new();
        let mut default_target = None;
        for (name, mount) in toml.mount {

            let (path, is_default) = match mount {
                MountToml::JustPath(path) => (path, false),
                MountToml::Table { default, path } => (path, default),
            };

            let path = path_replace(path)?;

            if is_default {
                if default_target.is_some() {
                    anyhow::bail!("mutliple default mount points");
                }
                default_target = Some(name.clone());
            }

            let full_path = path.full_path()?;
            if !full_path.exists() {
                eprintln!("warning: mount '{}', path '{}' does not exist", name, full_path);
            }

            mounts.push((name, path));
        }

        let mut providers: Vec<Provider> = Vec::new();
        for (name, ent) in toml.providers {

            let path = match ent {
                ProviderToml::Path(p) => p,
                ProviderToml::Table { path, .. } => path,
            };

            let uri = provider_replace(&path)?;
            let provider = Provider::new(name, uri, &cache_dir)?;
            providers.push(provider);
        }

        let config = Config {
            db_file,
            lockfile,
            arch_filter: Some(arch),
            cache_dir,
            cache_retention,
            cache_fetch_jobs,
            cache_auto_clean: toml.cache.auto_clean,
            cache_touch_on_uninstall: toml.cache.touch_on_uninstall,
            scan_threads: toml.scan.and_then(|v| v.threads).unwrap_or(0),
            scan_debounce,
            providers,
            mount : MountConfig {
                default_target,
                mounts,
            },
        };

        //dbg!(&config);

        Ok(config)
    }

    pub fn from_path<P: AsRef<Path>>(path: P) -> AResult<Config> {
        let read = BufReader::new(File::open(path.as_ref())?);
        Self::from_reader(read)
    }

    /// Given a name (from a package definition) find
    /// and return the mount point from the config
    pub fn get_mountpoint(&self, name: Option<&str>) -> MountPoint {
        match name {
            None => {
                if let Some(name) = &self.mount.default_target {
                    for (n, path) in &self.mount.mounts {
                        if name == n {
                            return MountPoint::Default(path.clone());
                        }
                    }
                }
                MountPoint::DefaultDisabled
            },
            Some(name) => {
                for (n, path) in &self.mount.mounts {
                    if name == n {
                        return MountPoint::Specified(path.clone());
                    }
                }
                MountPoint::Invalid{name: name.to_string()}
            }
        }
    }

    pub fn get_mountpoint_user(&self, target: &str) -> MountPoint {

        if let Some(name) = target.strip_prefix("MOUNT:") {
            return self.get_mountpoint(Some(name));
        }
        if let Some(name) = target.strip_prefix("mount:") {
            return self.get_mountpoint(Some(name));
        }
        if let Some(name) = target.strip_prefix("TARGET:") {
            return self.get_mountpoint(Some(name));
        }
        if let Some(name) = target.strip_prefix("target:") {
            return self.get_mountpoint(Some(name));
        }

        let partial_canonicalize = |path: &Utf8Path| -> Utf8PathBuf {
            for parent in path.ancestors().skip(1) {
                if let Ok(true) = parent.try_exists() {
                    let rest = path.strip_prefix(parent);
                    let parent = parent.canonicalize_utf8();
                    if let (Ok(parent), Ok(rest)) = (parent, rest) {
                        return crate::join_path_utf8!(parent, rest);
                    }
                    break;
                }
            }
            return path.to_path_buf();
        };

        let mut path = Utf8PathBuf::from(target);
        if !path.is_absolute() {
            path = crate::join_path_utf8!(".", path);
        }
        path = partial_canonicalize(&path);

        MountPoint::User(PathType::Absolute(path))
    }
}

/// replace variables within a path
/// - `${BPM}` => dir of bpm binary, must be at the start of the path
/// - `${THIS}` => dir of the config file, must be at the start of the path
/// - and anything replaced by config_replace()
fn path_replace<S: Into<String>>(path: S) -> anyhow::Result<PathType> {

    let mut path : String = path.into();
    let mut relative = None;

    // ${BPM} must be the first thing in the string
    if path.starts_with("${BPM}/") {
        path = path.strip_prefix("${BPM}/").unwrap().into();
        relative = Some(RelativeToAnchor::Bpm);
    }

    if path.contains("${BPM}") {
        // more ${BPM} found in the middle of the path
        anyhow::bail!("Invalid ${{BPM}} substitution: '{path}'");
    }

    // ${THIS} must be the first thing in the string
    if path.starts_with("${THIS}/") {
        if let Some(dir) = get_config_dir() && dir.is_absolute() {
            path = path.strip_prefix("${THIS}/").unwrap().into();
            relative = Some(RelativeToAnchor::Config);
        } else {
            anyhow::bail!("Cannot substitute ${{THIS}} when config path is unknown");
        }
    }

    if path.contains("${THIS}") {
        // more ${THIS} found in the middle of the path
        anyhow::bail!("Invalid ${{THIS}} substitution: '{path}'");
    }

    config_replace(&mut path);

    let path = Utf8PathBuf::from(path);
    if relative.is_none() && path.is_relative() {
        anyhow::bail!("Invalid naked relative path '{path}' in config file");
    }

    if let Some(relative) = relative {
        Ok(PathType::Relative{
            path,
            relative_to: relative,
        })
    } else {
        Ok(PathType::Absolute(path))
    }
}

/// replace variables within a path
///
///  direct replacements:
/// - `${OS}`             => "linux", "windows", "darwin", "wasm", "unix", "unknown"
/// - `${ARCH3264}`       => "64"  OR "32"
/// - `${POINTER_WIDTH}`  => "64"  OR "32"
/// - `${ARCHX8664}`      => "x86" OR "x86_64" OR "arm" OR "aarch64"
///
///  basic true/false replacements that optionally take true and false strings:
///    ${VAR:true_value:false_value}
///
/// - `${windows}`        => "windows"  OR custom true/false values
/// - `${linux}`          => "linux",   OR custom true/false values
/// - `${macos}`          => "macos",   OR custom true/false values
/// - `${unix}`           => "unix",    OR custom true/false values
/// - `${bsd}`            => "bsd",     OR custom true/false values
/// - `${freebsd}`        => "freebsd", OR custom true/false values
/// - `${openbsd}`        => "openbsd", OR custom true/false values
/// - `${netbsd}`         => "netbsd",  OR custom true/false values
/// - `${wasm}`           => "wasm",    OR custom true/false values
/// - `${32}`             => "32",      OR custom true/false values
/// - `${64}`             => "64",      OR custom true/false values
/// - `${x86}`            => "x86",     OR custom true/false values
/// - `${x86_64}`         => "x86_64",  OR custom true/false values
/// - `${x86-64}`         => "x86-64",  OR custom true/false values
/// - `${amd64}`          => "amd64",   OR custom true/false values
/// - `${x64}`            => "x64",     OR custom true/false values
/// - `${aarch64}`        => "aarch64"  OR custom true/false values
/// - `${arm}`            => "arm",     OR custom true/false values
/// - `${gnu}`            => "gnu",     OR custom true/false values
/// - `${msvc}`           => "msvc",    OR custom true/false values
/// - `${musl}`           => "musl",    OR custom true/false values
///
///  environment variable replacements:
///    ${ENV(VAR)}, the value of the env var is the replacement string,
///    OR
///    ${ENV(VAR):true_value:false_value}
///    An env var is only considered false if it is not defined OR has the value "0"
fn config_replace(text: &mut String) {

    if text.contains("${OS}") {

        let os = if cfg!(target_os="windows") {
            "windows"
        } else if cfg!(target_os="linux") {
            "linux"
        } else if cfg!(target_os="macos") {
            "darwin"
        } else if cfg!(target_family="wasm") {
            "wasm"
        } else if cfg!(unix) {
            "unix"
        } else {
            "unknown"
        };

        *text = text.replace("${OS}", os);
    }

    if text.contains("${ARCH3264}") {

        let arch = if cfg!(target_pointer_width="32") {
            "32"
        } else if cfg!(target_pointer_width="64") {
            "64"
        } else {
            panic!("unhandled architecture");
        };

        *text = text.replace("${ARCH3264}", arch);
    }

    if text.contains("${POINTER_WIDTH}") {

        let arch = if cfg!(target_pointer_width="32") {
            "32"
        } else if cfg!(target_pointer_width="64") {
            "64"
        } else {
            panic!("unhandled pointer width (not 32 or 64)");
        };

        *text = text.replace("${POINTER_WIDTH}", arch);
    }

    if text.contains("${ARCHX8664}") {

        let arch = if cfg!(target_arch="x86") {
            "x86"
        } else if cfg!(target_arch="x86_64") {
            "x86_64"
        } else if cfg!(target_arch="arm") {
            "arm"
        } else if cfg!(target_arch="aarch64") {
            "aarch64"
        } else {
            panic!("unhandled architecture");
        };

        *text = text.replace("${ARCHX8664}", arch);
    }

    env_replace(text);

    custom_replace(text, "windows", "windows", "", cfg!(target_os="windows"));
    custom_replace(text, "linux",   "linux",   "", cfg!(target_os="linux"));
    custom_replace(text, "macos",   "macos",   "", cfg!(target_os="macos"));
    custom_replace(text, "unix",    "unix",    "", cfg!(unix));
    custom_replace(text, "freebsd", "freebsd", "", cfg!(target_os="freebsd"));
    custom_replace(text, "openbsd", "openbsd", "", cfg!(target_os="openbsd"));
    custom_replace(text, "netbsd",  "netbsd",  "", cfg!(target_os="netbsd"));
    custom_replace(text, "bsd",     "bsd",     "", cfg!(any(target_os="freebsd", target_os="openbsd", target_os="netbsd")));
    custom_replace(text, "wasm",    "wasm",    "", cfg!(target_family="wasm"));
    custom_replace(text, "32",      "32",      "", cfg!(target_pointer_width="32"));
    custom_replace(text, "64",      "64",      "", cfg!(target_pointer_width="64"));
    custom_replace(text, "x86",     "x86",     "", cfg!(target_arch="x86"));
    custom_replace(text, "x86_64",  "x86_64",  "", cfg!(target_arch="x86_64"));
    custom_replace(text, "x86-64",  "x86-64",  "", cfg!(target_arch="x86_64"));
    custom_replace(text, "amd64",   "amd64",   "", cfg!(target_arch="x86_64"));
    custom_replace(text, "x64",     "x64",     "", cfg!(target_arch="x86_64"));
    custom_replace(text, "aarch64", "aarch64", "", cfg!(target_arch="aarch64"));
    custom_replace(text, "arm",     "arm",     "", cfg!(target_arch="arm"));
    custom_replace(text, "gnu",     "gnu",     "", cfg!(target_env="gnu"));
    custom_replace(text, "msvc",    "msvc",    "", cfg!(target_env="msvc"));
    custom_replace(text, "musl",    "musl",    "", cfg!(target_env="musl"));
}

/// Replace `${KEY}` with KEY
///
/// AND
///
/// Replace `${KEY:true_value:false_value}` with either true_value or false_value based on if KEY is true
fn custom_replace(text: &mut String, key: &str, true_value: &str, false_value: &str, tf: bool) {

    // of form ${KEY}
    let mut key = format!("${{{key}}}");
    if text.contains(&key) {
        *text = text.replace(&key, if tf { true_value } else { false_value });
    }

    // of form ${KEY:true_value:false_value}
    key.pop();
    key.push(':');
    while let Some(idx) = text.find(&key) {
        if let Some(end_idx) = &text[idx..].find('}') {
            let all_words = &text[(idx+2)..=(idx+end_idx-1)];
            let mut words = all_words.split(':').skip(1);
            let tv = words.next().unwrap_or(true_value);
            let fv = words.next().unwrap_or(false_value);
            let rep = if tf { tv } else { fv };
            *text = format!("{}{}{}", &text[0..idx], rep, &text[idx+end_idx+1..]);
        }
    }
}

/// Replace ${ENV(VAR)} with the value of the env var
///
/// AND
///
/// Replace ${ENV(VAR):true_value:false_value}
///
/// NOTE: the ONLY false value is "0", A var that is defined but with no value (empty string) will
/// be considered true. This makes no attempt to look for words like "false" or "no" either.
fn env_replace(text: &mut String) {

    let mut too_many = 64i32;

    // of form ${ENV(NAME)}
    let key_start = "${ENV(";
    let key_end = ")}";
    while let Some(idx) = text.find(key_start) && let Some(end_idx) = text[idx..].find(key_end) {

        too_many -= 1;
        if too_many <= 0 {
            panic!("too many env replacements");
        }

        let full_match = &text[idx..=(idx + end_idx + key_end.len() - 1)];
        let mut rep = String::from("");
        if let Some(inner) = full_match.strip_prefix("${ENV(") {
            if let Some(var_name) = inner.strip_suffix(")}") {
                if let Ok(value) = std::env::var(var_name) {
                    rep = value;
                }
            }
        }
        *text = text.replace(full_match, &rep);
    }

    // of form ${ENV(NAME):true_value:false_value}
    let key_start = "${ENV(";
    let key_end = "}";
    while let Some(idx) = text.find(key_start) && let Some(end_idx) = text[idx..].find(key_end) {

        too_many -= 1;
        if too_many <= 0 {
            panic!("too many env replacements");
        }

        let full_match = &text[idx..=(idx + end_idx + key_end.len() - 1)];
        let mut rep = "";
        if let Some(inner) = full_match.strip_prefix("${ENV(") {
            if let Some(inner) = inner.strip_suffix("}") {
                let mut words = inner.split(':');
                let name = words.next().unwrap_or("");
                let tv = words.next().unwrap_or("");
                let fv = words.next().unwrap_or("");
                if let Some(name) = name.strip_suffix(')') {
                    let tf = std::env::var(name).as_deref().unwrap_or("0") != "0";
                    rep = if tf { tv } else { fv };
                }
            }
        }
        *text = text.replace(full_match, rep);
    }
}

fn provider_replace(uri: &str) -> anyhow::Result<String> {

    if uri.starts_with("file://") {

        let path = uri.strip_prefix("file://").unwrap();
        let path = path_replace(path)?.full_path()?;
        return Ok(format!("file://{path}"));

    } else if uri.starts_with("http://") || uri.starts_with("https://") {

        let mut uri = String::from(uri);
        config_replace(&mut uri);
        return Ok(uri);

    } else {

        eprintln!("warning: unrecognized provider type");
        let mut uri = String::from(uri);
        config_replace(&mut uri);
        return Ok(uri);
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[allow(clippy::literal_string_with_formatting_args)]
    #[test]
    fn replacements() {


        let mut txt = String::from("x x x ${OS} x x x");
        config_replace(&mut txt);
        if cfg!(target_os="linux") {
            assert_eq!(txt, "x x x linux x x x");
        } else if cfg!(target_os="windows") {
            assert_eq!(txt, "x x x windows x x x");
        }

        unsafe { std::env::set_var("BPM_TEST_VALUE", "foo"); }
        let mut txt = String::from("x x x ${ENV(BPM_TEST_VALUE)} x x x");
        config_replace(&mut txt);
        if cfg!(target_os="linux") {
            assert_eq!(txt, "x x x foo x x x");
        }
        println!();

        unsafe { std::env::set_var("BPM_TEST_VALUE", "foo"); }
        unsafe { std::env::set_var("BPM_TEST_VALUE2", "bar"); }
        let mut txt = String::from("x x x ${ENV(BPM_TEST_VALUE)} ${ENV(BPM_TEST_VALUE2)} x x x");
        config_replace(&mut txt);
        if cfg!(target_os="linux") {
            assert_eq!(txt, "x x x foo bar x x x");
        }
        println!();

        unsafe { std::env::set_var("BPM_TEST_VALUE", "foo"); }
        let mut txt = String::from("x x x ${ENV(BPM_TEST_MISSING)} x x x");
        config_replace(&mut txt);
        if cfg!(target_os="linux") {
            assert_eq!(txt, "x x x  x x x");
        }
        println!();

        unsafe { std::env::set_var("BPM_TEST_TRUE", ""); }
        unsafe { std::env::set_var("BPM_TEST_TRUE2", "1"); }
        unsafe { std::env::set_var("BPM_TEST_FALSE", "0"); }
        let mut txt = String::from("x x x ${ENV(BPM_TEST_TRUE):foo:bar}${ENV(BPM_TEST_TRUE2):FOO:BAR}${ENV(BPM_TEST_FALSE):baz:BAZ} x x x");
        config_replace(&mut txt);
        if cfg!(target_os="linux") {
            assert_eq!(txt, "x x x fooFOOBAZ x x x");
        }
        println!();

        unsafe { std::env::set_var("BPM_TEST_TRUE", ""); }
        unsafe { std::env::set_var("BPM_TEST_TRUE2", "1"); }
        unsafe { std::env::set_var("BPM_TEST_FALSE", "0"); }
        let mut txt = String::from("x x x ${ENV(BPM_TEST_TRUE)::bar}${ENV(BPM_TEST_TRUE2):FOO}${ENV(BPM_TEST_FALSE)::BAZ}${ENV(BPM_TEST_FALSE):BAZ} x x x");
        config_replace(&mut txt);
        if cfg!(target_os="linux") {
            assert_eq!(txt, "x x x FOOBAZ x x x");
        }
        println!();

        let mut txt = String::from("x x x ${linux} x x x");
        config_replace(&mut txt);
        if cfg!(target_os="linux") {
            assert_eq!(txt, "x x x linux x x x");
        }
        println!();

        let mut txt = String::from("x x x ${linux:LINUX} x x x");
        config_replace(&mut txt);
        if cfg!(target_os="linux") {
            assert_eq!(txt, "x x x LINUX x x x");
        }
        println!();

        let mut txt = String::from("x x x ${linux:something} x x x");
        config_replace(&mut txt);
        if cfg!(target_os="linux") {
            assert_eq!(txt, "x x x something x x x");
        }
        println!();

        let mut txt = String::from("x x x ${linux:this} ${linux:that} x x x");
        config_replace(&mut txt);
        if cfg!(target_os="linux") {
            assert_eq!(txt, "x x x this that x x x");
        }
        println!();

        let mut txt = String::from("x x x ${linux:something:other} x x x");
        config_replace(&mut txt);
        if cfg!(target_os="linux") {
            assert_eq!(txt, "x x x something x x x");
        }
        println!();

        let mut txt = String::from("x x x ${windows} x x x");
        config_replace(&mut txt);
        if cfg!(target_os="linux") {
            assert_eq!(txt, "x x x  x x x");
        }

        let mut txt = String::from("x x x ${windows:win:notwin} x x x");
        config_replace(&mut txt);
        if cfg!(target_os="linux") {
            assert_eq!(txt, "x x x notwin x x x");
        }

        // empty true
        let mut txt = String::from("x x x ${linux::notlinux} x x x");
        config_replace(&mut txt);
        if cfg!(target_os="linux") {
            assert_eq!(txt, "x x x  x x x");
        }

        let mut txt = String::from("x x x ${windows::something} x x x");
        config_replace(&mut txt);
        if cfg!(target_os="linux") {
            assert_eq!(txt, "x x x something x x x");
        }
    }
}

