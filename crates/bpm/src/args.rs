use clap::{Command, Arg, arg, ArgAction, builder::styling};
use crate::provider;

pub fn pull_many_opt<'a>(matches: &'a clap::ArgMatches, key: &str) -> Option<Vec<&'a String>> {
    matches.get_many::<String>(key).map(|v| v.collect::<Vec<&String>>())
}

pub fn pull_many<'a>(matches: &'a clap::ArgMatches, key: &str) -> Vec<&'a String> {
    pull_many_opt(matches, key).unwrap_or_default()
}

pub fn parse_providers(matches: &clap::ArgMatches) -> provider::ProviderFilter {
    let providers = pull_many_opt(matches, "providers");
    providers.map_or(provider::ProviderFilter::empty(), |v| provider::ProviderFilter::from_names(v.iter()))
}

fn providers_arg() -> Arg {
    arg!(--providers <names> "Only operate on the given list of providers (comma delimited)")
        .alias("provider")
        .value_delimiter(',')
        .action(ArgAction::Append)
}

fn arch_arg() -> Arg {
    arg!(--arch <archs> "Filter on specific arch strings")
        .alias("archs")
        .value_delimiter(',')
        .action(ArgAction::Append)
}

pub fn get_cli() -> Command {

    const STYLES: styling::Styles = styling::Styles::styled()
        .header(styling::AnsiColor::Yellow.on_default().bold())
        .usage(styling::AnsiColor::Green.on_default().bold())
        .literal(styling::AnsiColor::Blue.on_default().bold())
        .placeholder(styling::AnsiColor::Cyan.on_default());

    let cli = Command::new("bpm")
        .version(clap::crate_version!())
        .about("The Beacon Package Manager : bpm : A simple, generic, general-purpose package manager")
        .author("Bryan Splitgerber")
        .color(clap::ColorChoice::Auto)
        .styles(STYLES)
        .arg(arg!(-c --config <file> "use a specific config file"))
        .subcommand_required(true)
        .subcommand(
            Command::new("scan")
                .about("Scan providers, updating provider package list caches")
                .arg(arg!(--debounce <time> "Do not scan agian if last scan was within the given time"))
                .arg(providers_arg())
                .arg(arch_arg())
        )
        .subcommand(
            Command::new("search")
                .about("Search for packages")
                .arg(arg!(<pkg> "(partial) package name to search for"))
                .arg(arg!(--exact "match package name exactly"))
                .arg(arch_arg())
        )
        .subcommand(
            Command::new("list")
                .about("List installed or available packages")
                .subcommand(Command::new("installed").about("List currently installed packages")
                    .arg(arg!(--"show-arch" "Show package architecture"))
                    .after_help("Print a table of package name, version, channel, and optionally package architecture.\n\nThe version is prefixed with either = or ^.\n  = indicates that the package is pinned to this version\n  ^ means the package is eligible for updates.\nIf a channel is listed, the package is pinned to that channel.")
                )
                .subcommand(Command::new("available").about("List available packages")
                    .alias("avail")
                    .arg(arg!([pkg] "package name substring"))
                    .arg(arg!(--exact  "match package name exactly"))
                    .arg(arg!(--limit <N> "Limit to the N latest versions per package")
                        .value_parser(clap::value_parser!(u32))
                        .default_value("0")
                    )
                    .arg(arg!(--json "output in json lines format"))
                    .arg(arg!(--oneline "output in one package per line format")
                        .conflicts_with("json")
                    )
                    .arg(arg!(--"show-arch" "Show package architecture"))
                    .arg(arch_arg())
                    .arg(arg!(--channels <channels> "Filter on only the given channels")
                        .alias("channel")
                        .value_delimiter(',')
                        .action(ArgAction::Append)
                    )
                    .arg(providers_arg())
                )
                .subcommand(Command::new("channels")
                    .about("List the channels of packages")
                    .arg(arg!([pkg] "package name substring"))
                    .arg(arg!(--exact  "match package name exactly"))
                    .arg(arg!(--json "output in json lines format"))
                    .arg(arch_arg())
                    .arg(providers_arg())
                )
        )
        .subcommand(
            Command::new("install")
                .about("Install new packages")
                .arg(arg!(<pkg> "Package name or path to local package file."))
                .arg(arg!(--"no-pin" "Do not pin to a specific version. Package may immediately be a candidate for updating."))
                .arg(arg!(-u --update "Install a different version of an already installed package. No effect if pkg is not already installed."))
                .arg(arg!(--reinstall "Allow installing the same version again."))
                .arg(arg!(-t --target <location> "Install into user specified <location>. Can be prefixed with \"MOUNT:\" to name a mount in the config file."))
                .arg(providers_arg())
                .arg(arch_arg())
        )
        .subcommand(
            Command::new("uninstall")
                .visible_alias("remove")
                .about("Remove installed packages")
                .arg(arg!(<pkg> "package name or path to local package file"))
                .arg(arg!(-v --verbose))
                .arg(arg!(--"remove-unowned" "Remove any unowned files"))
        )
        .subcommand(
            Command::new("update")
                .about("Update packages")
                .arg(arg!([pkg]... "package name or path to local package file"))
                .arg(providers_arg())
        )
        .subcommand(
            Command::new("verify")
                .about("Perform consistency check on package state")
                .arg(arg!([pkg]... "Package name(s) to verify. If no package is specified, verify all."))
                .arg(arg!(--restore "Restore files that have been modified to original installation state. Does not restore volatile files."))
                .arg(arg!(--"restore-volatile" "Also restore volatile files. No effect if --restore is not given"))
                .arg(arg!(--mtime "Enable mtime verification"))
                .arg(arg!(-v --verbose "Ouput extra information").action(ArgAction::Count))
                .arg(arg!(--"fail-fast" "Stop after finding first modified file. No effect if using --restore"))
        )
        //IDEA
        //.subcommand(
            //Command::new("restore")
                //.about("Restore modified files within a package to original installation state")
                //.arg(arg!(--volatile "also restore volatile files"))
        //)
        .subcommand(
            Command::new("query")
                .subcommand_required(true)
                .about("Query information about installed packages")
                .subcommand(Command::new("owner").about("Query which package owns a local file")
                    .arg(arg!(<file> "The file to find the owner of"))
                )
                .subcommand(Command::new("list-files").about("Query the list of files from a package")
                    .alias("files")
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
                .subcommand(Command::new("kv").about("Query a package's Key-Value store")
                    .arg(Arg::new("pkg")
                         .required_unless_present("all")
                         .conflicts_with("all")
                         .num_args(1..)
                    )
                    .arg(Arg::new("keys")
                         .long("keys")
                         .alias("key")
                         .short('k')
                         .help("Limit output to a set of keys")
                         .value_name("a,b,c")
                         .value_delimiter(',')
                         .action(ArgAction::Append)
                    )
                    .arg(arg!(-a --all "Select (Query) all packages"))
                    .arg(arg!(--"from-providers" [provider] "Get the KV from the provider, not a specific package version")
                         .alias("from-provider")
                         .require_equals(true)
                         .default_missing_value("*")
                    )
                    .arg(arch_arg())
                    .after_help("With no additional args, the entire KV will be dumped in json format")
                )
                // other ideas:
                // which provider it came from
                // mount
                // file sizes
                // hash
                // uuid
                // risked (files in owned dirs, that are not owned files)
        )
        .subcommand(
            Command::new("pin")
                .about("Pin a package to the version that is currently installed or to a channel")
                .arg(arg!(<pkg> "Package being pinned"))
                .arg(arg!(-c --channel <channel> "A channel to pin the package to"))
        )
        .subcommand(
            Command::new("unpin")
                .about("Unpin a package from a version or channel")
                .arg(arg!(<pkg> "Package being pinned"))
        )
        //.subcommand(
        //    Command::new("inspect")
        //)
        //.subcommand(
        //    Command::new("info")
        //        .about("show detailed info about a package")
            //        .arg(arg!(<pkg> "package name or path to local package file"))
        //        .arg(arg!(--channels "list channels the given package"))
        //)
        .subcommand(
            Command::new("cache").about("Cache management")
                .subcommand(
                    Command::new("list")
                        .about("List cached package files and when they expire")
                )
                .subcommand(
                    Command::new("fetch")
                        .about("Fetch a package and store it in the cache")
                        .arg(providers_arg())
                        .arg(arch_arg())
                        .arg(arg!(<pkg>... "Package(s) to fetch"))
                )
                .subcommand(
                    Command::new("touch")
                        .about("Extend the cache lifetime of a package file")
                        .arg(arg!(<pkg> "The package to evict"))
                        .arg(arg!(-v --version <version> "Which version, otherwise assume all versions."))
                        .arg(arg!(-d --duration <time> "When to expire, time fom now."))
                )
                .subcommand(
                    Command::new("clean")
                        .about("Clean expired cache entries")
                )
                .subcommand(
                    Command::new("clear")
                        .about("Clear the cache")
                        .arg(arg!(--"in-use" "Allow evicting package files that are currently in use"))
                )
                .subcommand(
                    Command::new("evict")
                        .about("Remove a package from the cache")
                        .arg(arg!(<pkg> "The package to evict"))
                        .arg(arg!(-v --version <version> "Which version, otherwise assume all versions."))
                        .arg(arg!(--"in-use" "Allow evicting package files that are currently in use"))
                )
        );

    #[cfg(feature = "pack")]
    let cli = cli.subcommand(
        bpmpack::args::build_cli(Command::new("pack")
            .about("Bundled bpm-pack utils. Create packages.")
        )
    );

    #[cfg(feature = "swiss")]
    let cli = cli.subcommand(
        swiss::build_cli(Command::new("util")
            .about("Additional tools/utils")
        )
    );

    cli
}
