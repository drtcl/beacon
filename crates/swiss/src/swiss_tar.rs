use clap::Command;
use clap::arg;
use anyhow::Result;

use std::io::Read;
use std::io::Write;

const DEFAULT_ZSTD_LEVEL : i32 = 15;

pub fn args() -> Command {
    Command::new("tar")
        .about("Create/Extract/List tar archives")
        .arg(arg!(-l --list "List an archive")
            .required_unless_present_any(["extract", "create"])
            .conflicts_with("in_files")
        )
        .arg(arg!(extract: -x "Extract an archive")
            .required_unless_present_any(["create", "list"])
            .conflicts_with("in_files")
        )
        .arg(arg!(create: -c "Create an archive")
            .required_unless_present_any(["extract", "list"])
            .requires("in_files")
        )
        .arg(arg!(archive: -f <file> "The archive file to list, create, or extract")
            .required(true)
        )
        .arg(arg!(in_files: <files>...  "The files to add to an archive while creating")
            .required(false)
        )
        .arg(arg!(--zstd "Pass through zstd"))
}

pub fn create_tar(outfile: &str, files: Vec<String>, zstd: bool) -> Result<()> {

    let mut fd = std::fs::File::create(outfile)?;

    #[allow(unused_assignments)]
    let mut zfd = None;

    let cfd : &mut dyn Write;
    if zstd {
        let zstd = zstd::stream::write::Encoder::new(&mut fd, DEFAULT_ZSTD_LEVEL)?;
        let zstd = zstd.auto_finish();
        zfd = Some(zstd);
        cfd = zfd.as_mut().unwrap();
    } else {
        cfd = &mut fd;
    };

    let mut tar = tar::Builder::new(cfd);
    tar.follow_symlinks(false);

    for file in files {
        println!("A  {}", file);
        let meta = std::fs::metadata(&file)?;
        if meta.file_type().is_dir() {
            tar.append_dir_all(&file, &file)?;
        } else {
            tar.append_path(&file)?;
        }
        // TODO symlink?
    }

    tar.finish()?;

    Ok(())
}

pub fn list_tar(archive: &str, zstd: bool) -> Result<()> {

    let mut fd = std::fs::File::open(archive)?;

    #[allow(unused_assignments)]
    let mut zfd = None;

    let cfd : &mut dyn Read;
    if zstd {
        zfd = Some(zstd::Decoder::new(fd)?);
        cfd = zfd.as_mut().unwrap();
    } else {
        cfd = &mut fd;
    };

    let mut tar = tar::Archive::new(cfd);

    for ent in tar.entries()? {
        let e = ent?;
        let path = e.path()?;
        println!("{}", path.display());
    }

    Ok(())
}

pub fn extract_tar(archive: &str, zstd: bool) -> Result<()> {

    let cwd = std::env::current_dir()?;

    let mut fd = std::fs::File::open(archive)?;

    #[allow(unused_assignments)]
    let mut zfd = None;

    let cfd : &mut dyn Read;
    if zstd {
        zfd = Some(zstd::Decoder::new(fd)?);
        cfd = zfd.as_mut().unwrap();
    } else {
        cfd = &mut fd;
    };

    let mut tar = tar::Archive::new(cfd);

    tar.unpack(cwd)?;

    Ok(())
}

pub fn main(matches: &clap::ArgMatches) -> Result<()> {

    let create = *matches.get_one::<bool>("create").unwrap();
    let extract = *matches.get_one::<bool>("extract").unwrap();
    let list = *matches.get_one::<bool>("list").unwrap();
    let zstd = *matches.get_one::<bool>("zstd").unwrap();
    let archive = matches.get_one::<String>("archive").unwrap();

    if create {
        let files = matches.get_many::<String>("in_files").map_or(Vec::new(), |paths| paths.map(String::from).collect());
        return create_tar(archive, files, zstd);
    }

    if list {
        return list_tar(archive, zstd);
    }

    if extract {
        return extract_tar(archive, zstd);
    }

    anyhow::bail!("tar: no subcommand");
}
