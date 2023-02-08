use package::PackageID;
use crate::*;

//const DB_VERSION : u32 = 1;

type HashString = String;
type FilePath = String;

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
enum FileType {
    File,
    Link,
    Dir,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FileInfo {
    filetype: FileType,
    path: FilePath,
    hash: HashString,
    attrs: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DbPkg {
    pub location: Option<PathBuf>,

    /// Which channel was used to install this package, if any.
    /// Pinned to a specific version?
    pub versioning: Versioning,

    pub metadata: package::MetaData,

    //pub id: PackageID,
    //pub files: Vec<(FilePath, HashString)>,
    //pub files: Vec<FileInfo>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Db {
    //version: u32,
    pub installed: Vec<DbPkg>,
}

impl DbPkg {
    pub fn new(metadata: package::MetaData) -> Self {
        Self {
            metadata,
            location: None,
            versioning: Versioning::default(),
        }
    }
}

impl Db {
    pub fn new() -> Self {
        Self {
            installed: Vec::new(),
        }
    }

    pub fn add_package(&mut self, pkg: DbPkg) {
        self.installed.retain(|p| {
            p.metadata.name != pkg.metadata.name
        });
        self.installed.push(pkg);
    }

    pub fn remove_package(&mut self, pkg_id: PackageID) {
        self.installed.retain(|p| {
            !(p.metadata.name == pkg_id.name && p.metadata.version == pkg_id.version)
        });
    }

    pub fn write_to<W>(&self, w: &mut W) -> AResult<()>
    where
        W: std::io::Write,
    {
        //let s = toml::to_string_pretty(self)?;
        //write!(w, "{}", s)?;

        //rmp_serde::encode::write_named(w, self)?;

        serde_json::to_writer_pretty(w, self)?;

        //serde_yaml::to_writer(w, self)?;
        //ron::ser::to_writer_pretty(w, self, ron::ser::PrettyConfig::new())?;
        //ron::ser::to_writer(w, self)?;
        Ok(())
    }

    pub fn from_reader<R>(mut r: R) -> AResult<Self>
    where
        R: std::io::Read,
    {
        //let mut contents = String::new();
        //r.read_to_string(&mut contents)?;
        let mut contents = Vec::new();
        r.read_to_end(&mut contents)?;
        let db = if contents.is_empty() {
            Db::new()
        } else {
            //ron::from_str(&contents)?
            //serde_yaml::from_str(&contents)?
            //toml::from_str(&contents)?
            //rmp_serde::decode::from_read(&mut std::io::Cursor::new(&contents))?

            serde_json::from_reader(&mut std::io::Cursor::new(&contents))?
        };
        Ok(db)
    }

    //pub fn from_file<F: AsRef<Path>>(file: F) -> AResult<Self> {
    //    let file = std::fs::File::open(file.as_ref())?;
    //    Db::from_reader(file)
    //}
}
