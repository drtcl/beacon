use crate::*;

type HashString = String;
type FilePath = String;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DbPkg {
    pub id: PackageID,
    pub location: PathBuf,
    pub files: Vec<(FilePath, HashString)>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Db {
    pub installed: Vec<DbPkg>,
}

impl DbPkg {
    pub fn new(id: PackageID) -> Self {
        Self {
            id,
            location: PathBuf::new(),
            files: Vec::new(),
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
            p.id.name != pkg.id.name
        });
        self.installed.push(pkg);
    }

    pub fn write_to<W>(&self, w: &mut W) -> AResult<()>
    where
        W: std::io::Write,
    {
        ron::ser::to_writer_pretty(w, self, ron::ser::PrettyConfig::new())?;
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
            ron::from_str(&contents)?
        };
        Ok(db)
    }

    pub fn from_file<F: AsRef<Path>>(file: F) -> AResult<Self> {
        let file = std::fs::File::open(file.as_ref())?;
        Db::from_reader(file)
    }
}
