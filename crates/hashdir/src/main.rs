use anyhow::Result;
use clap::Arg;
use jwalk::WalkDir;
use rayon::prelude::*;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;

fn remove_path_prefix<'a>(prefix: &Path, path: &'a Path) -> &'a Path {
    let path = match path.strip_prefix(prefix) {
        Ok(path) => path,
        Err(_) => path,
    };
    path
}

fn get_file_hash(path: &Path) -> Result<String> {
    let mut hasher = blake3::Hasher::new();
    let file = std::fs::File::open(path)?;
    let mut file = std::io::BufReader::new(file);
    let _ = std::io::copy(&mut file, &mut hasher)?;
    let hash = hasher.finalize().to_hex().to_string();
    Ok(hash)
}

fn get_dir_hash(dir: &Path, show_files: bool) -> Result<String> {

    let mut final_hasher = blake3::Hasher::new();

    let mut files = Vec::<(PathBuf, Option<String>)>::new();
    for entry in WalkDir::new(dir).sort(true) {
        let entry = entry.expect("failed to get file netry");
        if entry.file_type().is_file() {
            let path = PathBuf::from(&entry.path());
            files.push((path, None));
        }
    }

    files.par_iter_mut().for_each(|(path, hash)| {
        *hash = Some(get_file_hash(path).expect("failed to get file hash"));
        let sub_path = remove_path_prefix(Path::new(&dir), path);
        *path = PathBuf::from(sub_path);
    });

    for (path, hash) in files {
        if show_files {
            println!("{} {}", &hash.as_ref().unwrap(), path.display())
        }
        write!(&mut final_hasher, "{}", path.display())?;
        final_hasher.write_all(hash.unwrap().as_bytes())?;
    }
    let final_hash = final_hasher.finalize().to_hex().to_string();
    Ok(final_hash)
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
