use anyhow::Result;
use clap::Arg;
use indicatif::{ProgressStyle, ProgressBar};
use std::io::{Read, Write};
use std::path::Path;
use std::io::BufReader;

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
    //let t = std::cmp::min(8, t);
    t
}

fn make_patch(oldfile: &str, newfile: &str, output: Option<&String>) -> Result<()> {

    let old_compressed = Some(String::from("zst")) == Path::new(oldfile).extension().map(|s| s.to_string_lossy().into_owned());
    let new_compressed = Some(String::from("zst")) == Path::new(newfile).extension().map(|s| s.to_string_lossy().into_owned());

    let oldfile = std::fs::File::open(oldfile).expect("failed to open file");
    let mut oldfile : Box<dyn Read> = if old_compressed {
        Box::new(zstd::stream::read::Decoder::new(oldfile)?)
    } else {
        Box::new(oldfile)
    };

    let newfile = std::fs::File::open(newfile).expect("failed to open file");
    let new_file_sz = newfile.metadata()?.len();
    let mut newfile : Box<dyn Read> = if new_compressed {
        Box::new(zstd::stream::read::Decoder::new(newfile)?)
    } else {
        Box::new(newfile)
    };

    let mut outfile = std::fs::File::create(output.expect("no out file")).expect("failed to open file");

    let mut dict_data = Vec::new();
    std::io::copy(&mut BufReader::new(&mut oldfile), &mut dict_data)?;

    let dict = zstd::dict::EncoderDictionary::new(&dict_data[..], 12);

    let mut compressor = zstd::stream::write::Encoder::with_prepared_dictionary(&mut outfile, &dict)?;
    compressor.long_distance_matching(true)?;
    //compressor.set_pledged_src_size(Some(new_file_sz))?;
    //compressor.window_log(31)?;
    compressor.set_parameter(zstd::zstd_safe::CParameter::WindowLog(31))?;
    //compressor.set_parameter(zstd::zstd_safe::CParameter::ForceMaxWindow(true))?;
    compressor.set_parameter(zstd::zstd_safe::CParameter::TargetLength(4096))?;
    //compressor.set_parameter(zstd::zstd_safe::CParameter::ChainLog(30))?;
    compressor.multithread(get_threads())?;
    let mut compressor = compressor.auto_finish();

    let pb = ProgressBar::new(new_file_sz);
        //.with_style(ProgressStyle::with_template("{elapsed} {eta} {wide_bar:.blue/white} {bytes}/{total_bytes}").unwrap());
    if new_compressed {
        pb.set_style(ProgressStyle::with_template("{spinner} {elapsed} -- {bytes} -- {bytes_per_sec}").unwrap())
    } else {
        pb.set_style(ProgressStyle::with_template("{elapsed} {eta} {wide_bar:.blue/white} {bytes}/{total_bytes}").unwrap())
    }
    pb.enable_steady_tick(std::time::Duration::from_millis(250));

    let mut buf = [0u8; 4096];
    loop {
        let n = newfile.read(&mut buf[..])?;
        if n == 0 {
            break;
        }
        compressor.write_all(&buf[0..n])?;
        pb.inc(n as u64);
    }

    pb.finish_and_clear();

    Ok(())
}

fn apply_patch(oldfile: &str, patchfile: &str, output: Option<&String>) -> Result<()> {

    let old_compressed = Some(String::from("zst")) == Path::new(oldfile).extension().map(|s| s.to_string_lossy().into_owned());
    let out_compressed = Some(String::from("zst")) == output
        .and_then(|v| Path::new(v).extension())
        .map(|s| s.to_string_lossy().into_owned());

    let oldfile = std::fs::File::open(oldfile).expect("failed to open file");
    let oldfile : Box<dyn Read> = if old_compressed {
        Box::new(zstd::stream::read::Decoder::new(oldfile)?)
    } else {
        Box::new(oldfile)
    };

    let patchfile = std::fs::File::open(patchfile).expect("failed to open file");
    let mut patchfile = BufReader::new(patchfile);

    let outfile = std::fs::File::create(output.expect("no out file")).expect("failed to open file");
    let mut outfile : Box<dyn Write> = if out_compressed {
        let mut enc = zstd::stream::write::Encoder::new(outfile, 3)?;
        enc.multithread(get_threads())?;
        let enc = enc.auto_finish();
        Box::new(enc)
    } else {
        Box::new(outfile)
    };

    let mut dict_data = Vec::new();
    let _n = std::io::copy(&mut BufReader::new(oldfile), &mut dict_data)?;

    let dict = zstd::dict::DecoderDictionary::new(&dict_data[..]);

    let mut decompressor = zstd::stream::read::Decoder::with_prepared_dictionary(&mut patchfile, &dict)?;
    decompressor.set_parameter(zstd::zstd_safe::DParameter::WindowLogMax(31))?;

    let pb = ProgressBar::new_spinner()
        .with_style(ProgressStyle::with_template("{bytes}  {spinner}").unwrap());
    pb.enable_steady_tick(std::time::Duration::from_millis(250));

    let mut buf = [0u8; 1204 * 1024];
    loop {
        let n = decompressor.read(&mut buf[..])?;
        if n == 0 {
            break;
        }
        outfile.write_all(&buf[0..n])?;
        pb.inc(n as u64);
    }

    outfile.flush()?;
    pb.finish();

    Ok(())
}
