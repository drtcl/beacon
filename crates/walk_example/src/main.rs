use anyhow::Result;
use std::io::Write;

fn main() -> Result<()> {

    let path = std::env::args().nth(1).unwrap_or(".".into());

    let mut tw = tabwriter::TabWriter::new(vec![]);
    tw.write_all("path\tutil\tis_dir\tis_file\tis_symlink\tpath_is_symlink\t\n".as_bytes())?;
    tw.write_all("---\t---\t---\t---\t---\t---\n".as_bytes())?;

    for ent in walkdir::WalkDir::new(path).sort_by_file_name() {
        let ent = ent?;

        let path = ent.path();
        let is_dir = ent.file_type().is_dir();
        let is_file = ent.file_type().is_file();
        let is_symlink = ent.file_type().is_symlink();

        tw.write_all(
            format!("{}\twalkdir\t{}\t{}\t{}\t{}\n", path.display(), is_dir, is_file, is_symlink, ent.path_is_symlink()).as_bytes()
        )?;

        let ent = std::fs::symlink_metadata(path)?;
        let is_dir = ent.file_type().is_dir();
        let is_file = ent.file_type().is_file();
        let is_symlink = ent.file_type().is_symlink();
        tw.write_all(
            format!("\tfs::symlink_metadata\t{}\t{}\t{}\t-\n", is_dir, is_file, is_symlink).as_bytes()
        )?;

        let ent = std::fs::metadata(path)?;
        let is_dir = ent.file_type().is_dir();
        let is_file = ent.file_type().is_file();
        let is_symlink = ent.file_type().is_symlink();
        tw.write_all(
            format!("\tfs::metadata\t{}\t{}\t{}\t-\n", is_dir, is_file, is_symlink).as_bytes()
        )?;

        let file = std::fs::File::open(path)?;
        let ent = file.metadata()?;
        let is_dir = ent.file_type().is_dir();
        let is_file = ent.file_type().is_file();
        let is_symlink = ent.file_type().is_symlink();
        tw.write_all(
            format!("\tFile::metadata\t{}\t{}\t{}\t-\n", is_dir, is_file, is_symlink).as_bytes()
        )?;

        tw.write_all(b"\t\t\t\t\t\n")?;
    }

    let output = tw.into_inner()?;
    let output = String::from_utf8_lossy(&output);

    #[cfg(feature="color")]
    let output = output.replace("false", "\x1b[33mfalse\x1b[0m");
    #[cfg(feature="color")]
    let output = output.replace("true",  "\x1b[32mtrue\x1b[0m");

    std::io::stdout().write_all(output.as_bytes())?;

    Ok(())
}
