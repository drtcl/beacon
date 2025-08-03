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
        .arg(arg!(--explain "Explain each version"))
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
    let explain = matches.get_flag("explain");
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
        if explain {
            print_explain(v, true);
        } else {
            println!("{}", v);
        }
    }


    Ok(())
}

fn print_explain(v: version::Version, json: bool) {

    let mut nums = vec![];
    let mut prerel = None;
    let mut build = None;
    let mut mess  = None;
    let mut pr_part_str = None;
    let mut bl_part_str = None;
    let mut ms_part_str = None;

    use bpm_version_compare::ScanToken;
    for part in bpm_version_compare::scan_it(&v) {
        match part {
            ScanToken::Num{n, ..}  => {
                nums.push(n);
            }
            ScanToken::Prerel(s) => {
                prerel = Some(s);
                pr_part_str = Some(bpm_version_compare::explain_parts(s, false));
            }
            ScanToken::Build(s) => {
                build = Some(s);
                bl_part_str = Some(bpm_version_compare::explain_parts(s, false));
            }
            ScanToken::Mess(s) => {
                mess = Some(s);
                ms_part_str = Some(bpm_version_compare::explain_parts(s, true));
            }
        }
    }

    if !json {
        println!("{}", v);
        println!("  nums:         {:?}", nums.as_slice());
        println!("  build:        {}", build.unwrap_or(""));
        println!("  prerel:       {}", prerel.unwrap_or(""));
        println!("  mess:         {}", mess.unwrap_or(""));
        println!("  valid_semver: {}", v.is_semver());
        println!("  prerel_parts: {}", pr_part_str.as_deref().unwrap_or(""));
        println!("  build_parts:  {}", bl_part_str.as_deref().unwrap_or(""));
        println!("  mess_parts:   {}", ms_part_str.as_deref().unwrap_or(""));
    } else {
        let mut json = serde_json::json!{{
            "nums": nums,
            "build": build,
            "prerel": prerel,
            "mess": mess,
            "prerel_parts": pr_part_str,
            "build_parts": bl_part_str,
            "mess_parts": ms_part_str,
        }};

        json.as_object_mut().unwrap().retain(|_, v| !v.is_null());

        let json = json.to_string();
        //let json = serde_json::to_string_pretty(&json).unwrap();
        let json = format!("{{\"version\":\"{}\",{}", v.as_str(), &json[1..]);
        println!("{}", json);
    }

}
