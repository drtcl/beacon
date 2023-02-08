use clap::{Command, arg};

pub fn get_cli() -> Command {

    Command::new("bpm")
        .version("0.1.0")
        .about("bpm : Bryan's Package Manager")
        .author("Bryan Splitgerber")
        .arg(arg!(-c --config [file] "use a specific config file"))
        .subcommand_required(true)
        .subcommand(
            Command::new("search")
                .about("search for packages")
                .arg(arg!(--exact "match package name exactly"))
                .arg(arg!(<pkg> "(partial) package name to search for"))

        )
        .subcommand(
            Command::new("list")
                .subcommand(Command::new("installed").about("list currently installed packages"))
                .subcommand(Command::new("available").about("list available packages")
                    .arg(arg!([pkg] "package name substring"))
                    .arg(arg!(--exact  "match package name exactly"))
                )
                .subcommand(Command::new("channels")
                    .arg(arg!([pkg] "package name substring"))
                    .arg(arg!(--exact  "match package name exactly"))
                )
        )
//        .subcommand(
//            Command::new("list-available").about("list available packages")
//                .alias("list_available")
//                .arg(arg!([pkg] "package name substring"))
//                .arg(arg!(--exact  "match package name exactly"))
//        )
        .subcommand(
            Command::new("scan")
                .about("scan providers, update provider package list caches")
                //.arg(arg!([provider_name]... "scan specific providers"))
        )
        .subcommand(
            Command::new("install")
                .about("install new packages")
                .arg(arg!(<pkg> "package name or path to local package file"))
        )
        .subcommand(
            Command::new("uninstall")
                .alias("remove")
                .about("remove installed packages")
                .arg(arg!(<pkg> "package name or path to local package file"))
        )
        .subcommand(
            Command::new("verify")
                .about("perform consistency check on package state")
                .arg(arg!([pkg]... "Package name(s) to verify. If no package is specified, verify all."))
        )
        .subcommand(
            Command::new("info")
                .about("show detailed info about a package")
                .arg(arg!(<pkg> "package name or path to local package file"))
                .arg(arg!(--channels "list channels the given package"))
        )

}
