use bpm_version_compare::*;

fn main() {

    let mut versions : Vec<_> = std::env::args().skip(1).map(|s| VersionOwned(s)).collect();

    #[cfg(feature="semver")]
    versions.retain(|v| semver::Version::parse(v.as_str()).is_ok());

    versions.sort();
    versions.dedup();

    for v in versions {
        println!("{}", v.as_str());
    }
}

