use clap::Command;
use clap::arg;
use anyhow::Result;

use std::io::IsTerminal;
use std::io::Read;
use std::io::Write;
use anyhow::Context;

const DEFAULT_ZSTD_LEVEL: &str = "15";

pub fn args() -> Command {
    Command::new("zstd")
        .about("Compress/Decompress with zstandard")
        .arg(arg!(-z --compress "Compress"))
        .arg(arg!(-d --decompress "Decompress")
            .overrides_with("compress")
        )
        .arg(arg!(file: [file] "The file to compress or decompress")
        )
        .arg(arg!(threads: -T <N> "Use N threads during encoding")
            .value_parser(clap::value_parser!(u32))
        )
        .arg(arg!(level: -C <level> "Compression level")
            .value_parser(clap::value_parser!(u32))
            .default_value(DEFAULT_ZSTD_LEVEL)
        )
        .arg(arg!(outfile: -o <OUTFILE> "Save results to OUTFILE"))
        .arg(arg!(--stdout "Write to stdout")
             .overrides_with("outfile")
        )
        .after_help(
r#"Examples:
    # compress a tar file with all available threads and compression level of 19
    # this will automatically write to foo.tar.zst if it does not already exist
    bpmzstd -T0 -C 19 foo.tar

    # decompress that file into foo2.tar
    bpmzstd -d foo.tar.zstd > foo2.tar
    # OR
    bpmzstd -d foo.tar.zstd -o foo2.tar
"#
        )
}

pub fn compress(instream: &mut impl Read, outstream: &mut impl Write, threads: u32, level: u32) -> Result<()> {
    let mut out = zstd::stream::write::Encoder::new(outstream, level as i32)?;
    out.multithread(threads).context("failed to enable multiple threads")?;
    let _ = std::io::copy(instream, &mut out).context("failed to compress")?;
    out.finish().context("failed to compress")?;
    Ok(())
}

pub fn decompress(instream: &mut impl Read, outstream: &mut impl Write) -> Result<()> {
    let mut dec = zstd::stream::read::Decoder::new(instream).context("failed to decode file")?;
    let _ = std::io::copy(&mut dec, outstream).context("failed to decompress")?;
    Ok(())
}

pub fn decomp_name(path: &str) -> Option<String> {
    let path = camino::Utf8PathBuf::from(path);
    if let Some(ext) = path.extension() && ext == "zst" && path.as_str().ends_with(".zst") {
        return Some(path.as_str().strip_suffix(".zst").unwrap().to_string());
    }
    None
}

pub fn comp_name(path: &str) -> Option<String> {
    let path = camino::Utf8PathBuf::from(path);
    if let Some(ext) = path.extension() && ext != "zst" {
        return Some(format!("{}.zst", path.as_str()));
    }
    None
}

pub fn main(matches: &clap::ArgMatches) -> Result<()> {

    let mut do_compress = matches.get_flag("compress");
    let do_decompress = matches.get_flag("decompress");
    let to_stdout = matches.get_flag("stdout");

    if !do_compress && !do_decompress {
        do_compress = true;
    }

    let level = *matches.get_one::<u32>("level").unwrap();

    // limit threads to 1..=cpu_count
    let core_count = std::thread::available_parallelism().map_or(1, |v| v.get() as u32);
    let mut threads = *matches.get_one::<u32>("threads").unwrap_or(&1);
    if threads == 0 {
        threads = core_count;
    } else {
        threads = std::cmp::min(core_count, std::cmp::max(1, threads));
    }

    let infile = matches.get_one::<String>("file");
    let outfile = matches.get_one::<String>("outfile");

    let mut instream : Box<dyn Read> = if let Some(infile) = infile {
        Box::new(std::fs::File::open(infile).context("failed to open file for reading")?)
    } else {
        let stdin = std::io::stdin();
        if stdin.is_terminal() {
            anyhow::bail!("input file expected");
        } else {
            Box::new(stdin)
        }
    };

    let mut outstream : Box<dyn Write> = if to_stdout {
        Box::new(std::io::stdout())
    } else if let Some(outfile) = outfile {
        Box::new(std::fs::File::create(outfile).context("failed to open output file for writing")?)
    } else {
        let stdout = std::io::stdout();
        if stdout.is_terminal() {

            let mut make_file = None;
            if let Some(infile) = infile {

                let outname = if do_compress {
                    comp_name(infile)
                } else if do_decompress {
                    decomp_name(infile)
                } else {
                    None
                };

                if let Some(outname) = outname {
                    if let Ok(false) = std::fs::try_exists(&outname) {
                        make_file = Some(outname);
                    } else {
                        anyhow::bail!("{} already exists", outname);
                    }
                }
            }

            if let Some(outname) = make_file {
                Box::new(std::fs::File::create(outname).context("failed to open outfile for writing")?)
            } else {
                anyhow::bail!("output file expected");
            }

        } else {
            Box::new(stdout)
        }
    };

    if do_compress {
        return compress(&mut instream, &mut outstream, threads, level);
    } else if do_decompress {
        return decompress(&mut instream, &mut outstream);
    }

    anyhow::bail!("zstd: no subcommand");
}

