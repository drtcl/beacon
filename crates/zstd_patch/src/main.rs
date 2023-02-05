use anyhow::Result;
use clap::Arg;
use indicatif::{ProgressStyle, ProgressBar};
use std::io::{Read, Write};

fn main() -> Result<()> {

    let matches = clap::Command::new("bpm")
        .arg(
            Arg::new("decompress")
                .short('d')
                .help("")
                .action(clap::ArgAction::SetTrue)
                .required(false),
        )
        .arg(
            Arg::new("oldfile")
                .action(clap::ArgAction::Set)
                .required(true),
        )
        .arg(
            Arg::new("newfile")
                .action(clap::ArgAction::Set)
                .required(true),
        )
        .arg(
            Arg::new("output")
                .short('o')
                .long("output")
                .help("")
                .action(clap::ArgAction::Set)
                .required(false),
        )
        .get_matches();

    // create a patch
    // zstdpatch <oldfile> <newfile> -o <patchfile>
    //
    // apply a patch
    // zstdpatch -d <oldfile> <patchfile> -o <newfile>

    let d = *matches.get_one::<bool>("decompress").unwrap();

    if d {
        let oldfile = matches.get_one::<String>("oldfile").unwrap();
        let patchfile = matches.get_one::<String>("newfile").unwrap();
        let output  = matches.get_one::<String>("output");
        if output.is_none() {
            todo!("output = None, NYI")
        }
        apply_patch(oldfile, patchfile, output)?;
    } else {
        let oldfile = matches.get_one::<String>("oldfile").unwrap();
        let newfile = matches.get_one::<String>("newfile").unwrap();
        let output  = matches.get_one::<String>("output");
        if output.is_none() {
            todo!("output = None, NYI")
        }
        make_patch(oldfile, newfile, output)?;
    }

    Ok(())
}

fn get_threads() -> u32 {
    let t = std::thread::available_parallelism().map_or(1, |v| v.get() as u32);
    let t = std::cmp::min(8, t);
    println!("using {t} threads");
    t
}

fn make_patch(oldfile: &str, newfile: &str, output: Option<&String>) -> Result<()> {

    let mut oldfile = std::fs::File::open(oldfile).expect("failed to open file");
    let mut newfile = std::fs::File::open(newfile).expect("failed to open file");
    let mut outfile = std::fs::File::create(output.expect("no out file")).expect("failed to open file");

    let mut dict_data = Vec::new();
    std::io::copy(&mut std::io::BufReader::new(&mut oldfile), &mut dict_data)?;

    let dict = zstd::dict::EncoderDictionary::new(&dict_data[..], 12);

    let file_sz = newfile.metadata()?.len();

    let mut compressor = zstd::stream::write::Encoder::with_prepared_dictionary(&mut outfile, &dict)?;
    compressor.long_distance_matching(true)?;
    //compressor.set_pledged_src_size(Some(file_sz))?;
    //compressor.window_log(31)?;
    compressor.set_parameter(zstd::zstd_safe::CParameter::WindowLog(31))?;
    //compressor.set_parameter(zstd::zstd_safe::CParameter::ForceMaxWindow(true))?;
    compressor.set_parameter(zstd::zstd_safe::CParameter::TargetLength(4096))?;
    //compressor.set_parameter(zstd::zstd_safe::CParameter::ChainLog(30))?;
    compressor.multithread(get_threads())?;
    let mut compressor = compressor.auto_finish();

    let pb = ProgressBar::new(file_sz)
        .with_style(ProgressStyle::with_template("{elapsed} {eta} {wide_bar:.blue/white} {bytes}/{total_bytes}").unwrap());

    let mut buf = [0u8; 4096];
    loop {
        let n = newfile.read(&mut buf[..])?;
        if n == 0 {
            break;
        }
        compressor.write(&buf[0..n])?;
        pb.inc(n as u64);
    }

    pb.finish_and_clear();

    Ok(())
}

fn apply_patch(oldfile: &str, patchfile: &str, output: Option<&String>) -> Result<()> {

    let mut oldfile = std::fs::File::open(oldfile).expect("failed to open file");
    let patchfile = std::fs::File::open(patchfile).expect("failed to open file");
    let mut outfile = std::fs::File::create(output.expect("no out file")).expect("failed to open file");

    let mut dict_data = Vec::new();
    std::io::copy(&mut std::io::BufReader::new(&mut oldfile), &mut dict_data)?;

    let dict = zstd::dict::DecoderDictionary::new(&dict_data[..]);

    let mut patchfile = std::io::BufReader::new(patchfile);

    let mut decompressor = zstd::stream::read::Decoder::with_prepared_dictionary(&mut patchfile, &dict)?;
    decompressor.set_parameter(zstd::zstd_safe::DParameter::WindowLogMax(31))?;

    let pb = ProgressBar::new_spinner()
        .with_style(ProgressStyle::with_template("{bytes}  {spinner}").unwrap());

    let mut buf = [0u8; 4096];
    loop {
        let n = decompressor.read(&mut buf[..])?;
        if n == 0 {
            break;
        }
        outfile.write(&buf[0..n])?;
        pb.inc(n as u64);
    }

    outfile.flush()?;
    pb.finish();

    Ok(())
}
