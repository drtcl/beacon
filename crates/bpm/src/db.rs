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
    pub metadata: package::MetaData,
    //pub id: PackageID,
    pub location: Option<PathBuf>,
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
            //id,
            metadata,
            location: None,
            //files: Vec::new(),
        }
    }
}

impl Db {
    pub fn new() -> Self {
        Self {
            installed: Vec::new(),
        }
    }

//    pub fn add_package(&mut self, pkg: DbPkg) {
//        self.installed.retain(|p| {
//            p.id.name != pkg.id.name
//        });
//        self.installed.push(pkg);
//    }

    pub fn add_package(&mut self, pkg: DbPkg) {
        self.installed.retain(|p| {
            p.metadata.id.name != pkg.metadata.id.name
        });
        self.installed.push(pkg);
    }


    pub fn write_to<W>(&self, w: &mut W) -> AResult<()>
    where
        W: std::io::Write,
    {
        serde_yaml::to_writer(w, self)?;
        //ron::ser::to_writer_pretty(w, self, ron::ser::PrettyConfig::new())?;
        //ron::ser::to_writer(w, self)?;
        Ok(())
    }

    pub fn from_reader<R>(mut r: R) -> AResult<Self>
    where
        R: std::io::Read,
    {
        let mut contents = String::new();
        r.read_to_string(&mut contents)?;
        let db = if contents.is_empty() {
            Db::new()
        } else {
            //ron::from_str(&contents)?
            serde_yaml::from_str(&contents)?
        };
        Ok(db)
    }

    pub fn from_file<F: AsRef<Path>>(file: F) -> AResult<Self> {
        let file = std::fs::File::open(file.as_ref())?;
        Db::from_reader(file)
    }
}
