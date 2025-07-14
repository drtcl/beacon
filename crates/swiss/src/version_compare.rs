use clap::Command;
use clap::arg;
use anyhow::Result;
use anyhow::Context;

pub fn args() -> Command {
    Command::new("version-compare")
        .about("Compare and sort versions")
        .long_about("Take a list of versions, sort and print them in descending order")
        .visible_alias("vc")
        .arg(arg!(versions: <version>... "Versions to compare and sort"))
        .arg(arg!(--"skip-invalid" "Skip invalid versions"))
        .arg(arg!(--semver "Require semver versions"))
        .arg(arg!(--ascending "output in ascending order")
            .overrides_with("descending")
        )
        .arg(arg!(--descending "output in descending order (default)")
            .overrides_with("ascending")
        )

}

pub fn main(matches: &clap::ArgMatches) -> Result<()> {

    let versions = matches.get_many::<String>("versions").context("version expected")?;
    let semver = matches.get_flag("semver");
    let skip_invalid = matches.get_flag("skip-invalid");

    let descending = !matches.get_flag("ascending");

    let mut versions = versions.map(|v| version::Version::new(v)).collect::<Vec<_>>();

    if skip_invalid {
        versions.retain(|v| package::is_valid_version(v.as_str()));

        if semver {
            versions.retain(|v| v.is_semver());
        }
    } else {
        let mut err = 0;
        for v in &versions {
            if !package::is_valid_version(v.as_str()) {
                eprintln!("error: invalid version string: {}", v);
                err += 1;
            }
            if semver && !v.is_semver() {
                eprintln!("error: version is not semver: {}", v);
                err += 1;
            }
        }
        if err > 0 {
            std::process::exit(1);
        }
    }

    versions.sort();
    versions.dedup();

    // // debug: assert that all versions compare correctly to all other versions
    // for i in 0..versions.len() {
    //     for j in 0..versions.len() {
    //         if j < i {
    //             assert!(versions[i] > versions[j]);
    //         }
    //         if j > i {
    //             assert!(versions[i] < versions[j]);
    //         }
    //     }
    // }

    if descending {
        versions.reverse();
    }

    for v in versions {
        println!("{}", v);
    }

    Ok(())
}
