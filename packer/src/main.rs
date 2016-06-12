use std::io::Write;
use std::io::Read;

fn get_md5(path: &str) -> String {

    let mut file = std::fs::File::open(path).expect("failed to open file for reading");
    let mut context = md5::Context::new();
    let mut space = [0u8; 1024];
    loop {
        match file.read(&mut space) {
            Ok(n) => {
                if n > 0 {
                    context.consume(&space[0..n]);
                } else {
                    break;
                }
            }
            _ => {
                todo!("failed read");
            }
        }
    }
    let digest = context.compute();
    //dbg!(&digest);
    let mut hash = String::new();
    for byte in digest {
        hash.push_str(&format!("{:02x}", byte));
    }
    hash
}

fn main() {

    let args = std::env::args().skip(1);

    let file = std::fs::File::create("data.tar").expect("failed to open file for writing");
    let tar = tar::Archive::new(file);

    let mut meta = Vec::new();

    for file in args {
        let hash = get_md5(&file);
        println!("packing file: {} {}", &file, hash);

        tar.append_path(&file).expect("failed to add file to tar");
        meta.push((file, hash));
    }
    //dbg!(&meta);

    tar.finish().expect("failed to write tar");
    drop(tar);

    // write meta date file
    let mut metafile = std::fs::File::create("meta.txt").expect("failed to open meta file");
    writeln!(&mut metafile, "-- files --").unwrap();
    for (path, hash) in meta {
        writeln!(&mut metafile, "{},{}", path, hash).unwrap();
    }
    drop(metafile);

    let package_tar = std::fs::File::create("package.tar").expect("failed to open file for writing");
    let tar = tar::Archive::new(package_tar);
    tar.append_path("meta.txt").expect("failed to add file to tar");
    tar.append_path("data.tar").expect("failed to add file to tar");
    tar.finish().expect("failed to create tar");
    drop(tar);
    std::fs::remove_file("meta.txt").unwrap();
    std::fs::remove_file("data.tar").unwrap();

    let mut infile = std::fs::File::open("package.tar").expect("failed to open file for reading");
    let file = std::fs::File::create("package.tar.gz").expect("failed to open file for writing");
    let mut gz = flate2::write::GzEncoder::new(file, flate2::Compression::default());
    std::io::copy(&mut infile, &mut gz).unwrap();
    std::fs::remove_file("package.tar").unwrap();

}
