
use clap::Command;
use clap::arg;
use anyhow::Result;

use anyhow::Context;

pub fn args() -> Command {
    Command::new("blake3")
        .about("Blake3 hashing")
        .visible_aliases(["b3sum"])
        .arg(arg!(files: [FILE]... "File(s) to blake3 hash"))
}

pub fn main(matches: &clap::ArgMatches) -> Result<()> {

    let files = matches.get_many::<String>("files").map(|v| v.into_iter().collect::<Vec<_>>());

    if let Some(paths) = files {

        for path in paths {
            let mut hasher = blake3::Hasher::new();
            let mut file = std::fs::File::open(path).context("failed to open file for reading")?;
            let _ = std::io::copy(&mut file, &mut hasher)?;
            let hash = hasher.finalize();
            println!("{}  {}", hash, path);
        }

    } else {

        // working on stdin
        let mut stdin = std::io::stdin();
        let mut hasher = blake3::Hasher::new();
        let _ = std::io::copy(&mut stdin, &mut hasher)?;
        let hash = hasher.finalize();
        println!("{}  -", hash);
    }

    Ok(())
}
