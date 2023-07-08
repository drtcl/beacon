use clap::{Command, arg, ArgAction};
use crate::provider;

pub fn pull_many_opt<'a>(matches: &'a clap::ArgMatches, key: &str) -> Option<Vec<&'a String>> {
    matches.get_many::<String>(key).map(|v| v.collect::<Vec<&String>>())
}

pub fn pull_many<'a>(matches: &'a clap::ArgMatches, key: &str) -> Vec<&'a String> {
    pull_many_opt(matches, key).unwrap_or_default()
}

pub fn parse_providers(matches: &clap::ArgMatches) -> provider::ProviderFilter {
    //let providers = matches.get_many::<String>("providers").map(|v| v.collect::<Vec<&String>>());
    let providers = pull_many_opt(matches, "providers");
    providers.map_or(provider::ProviderFilter::empty(), |v| provider::ProviderFilter::from_names(v))
}

fn providers_arg() -> clap::Arg {
    arg!(--providers <names> "Only operate on the given list of providers (comma delimited)")
        .alias("provider")
        .value_delimiter(',')
        .action(ArgAction::Append)
}

pub fn get_cli() -> Command {

    Command::new("bpm")
        .version("0.1.0")
        .about("bpm : A simple, generic, general-purpose package manager")
        .author("Bryan Splitgerber")
        .arg(arg!(-c --config <file> "use a specific config file"))
        .subcommand_required(true)
        .subcommand(
            Command::new("search")
                .about("search for packages")
                .arg(arg!(<pkg> "(partial) package name to search for"))
                .arg(arg!(--exact "match package name exactly"))
        )
        .subcommand(
            Command::new("list")
                .subcommand(Command::new("installed").about("list currently installed packages"))
                .subcommand(Command::new("available").about("list available packages")
                    .alias("avail")
                    .arg(arg!([pkg] "package name substring"))
                    .arg(arg!(--exact  "match package name exactly"))
                    .arg(providers_arg())
                    .arg(arg!(--json "output in json lines format"))
                    .arg(arg!(--oneline "output in one packger per line format")
                        .conflicts_with("json")
                    )
                    .arg(arg!(--limit <N> "Limit to the N latest versions per package")
                        .value_parser(clap::value_parser!(u32))
                        .default_value("0")
                    )
                    .arg(arg!(--channels <channels> "Filter on only the given channels")
                        .alias("channel")
                        .value_delimiter(',')
                        .action(ArgAction::Append)
                    )
                )
                .subcommand(Command::new("channels")
                    .arg(arg!([pkg] "package name substring"))
                    .arg(arg!(--exact  "match package name exactly"))
                    .arg(providers_arg())
                )
        )
        .subcommand(
            Command::new("scan")
                .about("scan providers, update provider package list caches")
                .arg(arg!(--debounce <time> "Do not scan agian if last scan was within the given time"))
                .arg(providers_arg())
        )
        .subcommand(
            Command::new("install")
                .about("install new packages")
                .arg(arg!(<pkg> "package name or path to local package file"))
                .arg(arg!(--"no-pin" "Do not pin to a specific version. Package may immediately be a candidate for updating."))
        )
        .subcommand(
            Command::new("uninstall")
                .alias("remove")
                .about("remove installed packages")
                .arg(arg!(<pkg> "package name or path to local package file"))
        )
        .subcommand(
            Command::new("update")
                .about("update packages")
                .arg(arg!([pkg]... "package name or path to local package file"))
        )
        .subcommand(
            Command::new("verify")
                .about("perform consistency check on package state")
                .arg(arg!([pkg]... "Package name(s) to verify. If no package is specified, verify all."))
                .arg(arg!(--restore "Restore files that have been modified to original installation state"))
                .arg(arg!(-v --verbose "Ouput extra information"))
        )
        .subcommand(
            Command::new("query")
                .subcommand_required(true)
                .about("Query information about installed packages")
                .subcommand(Command::new("owner").about("Query which package owns a local file")
                    .arg(arg!(<file> "The file to find the owner of"))
                )
                .subcommand(Command::new("list-files").about("Query the list of files from a package")
                    .arg(arg!(<pkg> "The package to list the files of"))
                    .arg(arg!(--depth <n> "Maximum depth. Toplevel is depth 1.")
                        .value_parser(clap::value_parser!(u32))
                    )
                    .arg(arg!(--absolute    "Show aboslute paths instead of package paths"))
                    .arg(arg!(--"show-type" "Show the type of each file"))

                    // --file-type
                   // .arg(arg!(--"file-type" <type> "Which types of files to include")
                   //      .value_parser(["file", "dir", "symlink"])
                   //      //.value_delimiter(',')
                   //      //.action(ArgAction::Append)
                   // )
                )
                // other ideas:
                // which provider it came from
                // mount
                // file sizes
                // risked (files in owned dirs, that are not owned files)
        )
        //.subcommand(
        //    Command::new("pin")
        //)
        //.subcommand(
        //    Command::new("unpin")
        //)
        //.subcommand(
        //    Command::new("inspect")
        //)
        .subcommand(
            Command::new("cache").about("cache management")
                .subcommand(
                    Command::new("clear")
                        .about("clear the cache") //TODO explain
                )
                .subcommand(
                    Command::new("touch")
                        .about("extend the cache lifetime of a package file")
                        .arg(arg!(<pkg> "Package name or filepath to touch"))
                        .arg(arg!(--duration <time> "How much time to add to retention"))
                )
                .subcommand(
                    Command::new("fetch")
                        .about("Fetch a package and store it in the cache")
                        .arg(arg!(<pkg>... "Package name(s) to fetch"))
                )
        )
        .subcommand(
            Command::new("info")
                .about("show detailed info about a package")
                .arg(arg!(<pkg> "package name or path to local package file"))
                .arg(arg!(--channels "list channels the given package"))
        )
        //.subcommand(
        //    Command::new("pack")
        //        .about("packaged bpm-pack utils")
        //)
}
