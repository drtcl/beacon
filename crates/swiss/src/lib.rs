#![feature(let_chains)]

use clap::Command;
use anyhow::Result;

mod version_compare;

#[cfg(feature="tar")]
mod swiss_tar;

#[cfg(feature="zstd")]
mod swiss_zstd;

#[cfg(feature="blake3")]
mod swiss_blake3;

pub fn get_cli() -> Command {
    build_cli(Command::new("swiss"))
}

pub fn build_cli(cmd: clap::Command) -> clap::Command {

    #[cfg(feature="tar")]
    let cmd = cmd.subcommand(swiss_tar::args());

    #[cfg(feature="zstd")]
    let cmd = cmd.subcommand(swiss_zstd::args());

    #[cfg(feature="blake3")]
    let cmd = cmd.subcommand(swiss_blake3::args());

    let cmd = cmd.subcommand(version_compare::args());

    cmd
}

pub fn main_cli(matches: &clap::ArgMatches) -> Result<()> {

    match matches.subcommand() {

        Some(("version-compare", matches)) => {
            return version_compare::main(matches);
        }

        #[cfg(feature="tar")]
        Some(("tar", matches)) => {
            return swiss_tar::main(matches);
        }

        #[cfg(feature="zstd")]
        Some(("zstd", matches)) => {
            return swiss_zstd::main(matches);
        }

        #[cfg(feature="blake3")]
        Some(("blake3", matches)) => {
            return swiss_blake3::main(matches);
        }

        _ =>  {
            anyhow::bail!("no subcmd");
        }
    }
}
