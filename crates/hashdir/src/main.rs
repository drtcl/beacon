use anyhow::Result;
use blake2::{Blake2b, Digest};
use clap::Arg;
use jwalk::WalkDir;
use rayon::prelude::*;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;

struct Hasher {
    blake: Blake2b,
}

impl Write for Hasher {
    fn write(&mut self, buf: &[u8]) -> Result<usize, std::io::Error> {
        self.blake.update(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> Result<(), std::io::Error> {
        Ok(())
    }
}

impl Hasher {
    fn new() -> Self {
        Self {
            blake: Blake2b::new(),
        }
    }
    fn finish(self) -> String {
        let hash = self.blake.finalize();
        let mut s = String::with_capacity(hash.as_slice().len() * 2);
        for byte in hash.as_slice() {
            s.push_str(&format!("{byte:02x}"));
        }
        s
    }
}

fn remove_path_prefix<'a>(prefix: &Path, path: &'a Path) -> &'a Path {
    let path = match path.strip_prefix(prefix) {
        Ok(path) => path,
        Err(_) => path,
    };
    path
}

fn get_file_hash(path: &Path) -> Result<String> {
    let mut blake = Hasher::new();
    let file = std::fs::File::open(path)?;
    let mut file = std::io::BufReader::new(file);
    std::io::copy(&mut file, &mut blake)?;
    Ok(blake.finish())
}

fn get_dir_hash(dir: &Path, show_files: bool) -> Result<String> {
    let mut final_hasher = Hasher::new();

    let mut files = Vec::<(PathBuf, Option<String>)>::new();
    for entry in WalkDir::new(&dir).sort(true) {
        let entry = entry.expect("failed to get file netry");
        if entry.file_type().is_file() {
            let path = PathBuf::from(&entry.path());
            files.push((path, None));
        }
    }

    files.par_iter_mut().for_each(|(path, hash)| {
        *hash = Some(get_file_hash(path).expect("failed to get file hash"));
        let sub_path = remove_path_prefix(Path::new(&dir), &path);
        *path = PathBuf::from(sub_path);
    });

    for (path, hash) in files {
        if show_files {
            println!("{} {}", &hash.as_ref().unwrap(), path.display())
        }
        write!(&mut final_hasher, "{}", path.display())?;
        final_hasher.write(hash.unwrap().as_bytes())?;
    }
    Ok(final_hasher.finish())
}

fn main() -> Result<()> {

    let matches = clap::Command::new("hashdir")
        .version("0.1.0")
        .author("Bryan Splitgerber")
        .arg(Arg::new("dir").action(clap::ArgAction::Set).required(true))
        .arg(Arg::new("show files")
            .action(clap::ArgAction::SetTrue)
            .short('f')
            .long("files")
            .required(false)
        )
        .get_matches();

    let show_files = *matches.get_one::<bool>("show files").unwrap();
    let dir = Path::new(matches.get_one::<String>("dir").unwrap());

    let hash = get_dir_hash(dir, show_files)?;
    if show_files {
        println!("{} {}", hash, dir.display());
    } else {
        println!("{hash}");
    }

    Ok(())
}
