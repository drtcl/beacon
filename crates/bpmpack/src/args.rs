use clap::{Command, arg, Arg, ArgAction};

pub fn get_cli() -> clap::Command {

    build_cli(
        clap::Command::new("bpm-pack")
            .about("Bryan's Package Manager : bpm-pack : package creation utility")
            .author("Bryan Splitgerber")
    )
}

pub fn build_cli(cmd: clap::Command) -> clap::Command {

    cmd.version("0.1.0")
        .disable_version_flag(true)
        .subcommand_negates_reqs(true)
        .args_conflicts_with_subcommands(true)
        .after_help("With no <COMMAND>, build a package.")
        .subcommand(
            Command::new("set-version")
                .about("Take an unversioned package and build a new package that is versioned")
                .arg(arg!(<pkgfile> "package file to version"))
                .arg(arg!(--version <version> "The package's version")
                    .required(true)
                )
                .arg(arg!(--semver "Require the version to be a valid semver [see http://semver.org]"))
        )
        .subcommand(
            Command::new("list-files")
                .alias("list")
                .about("list the files contained in a package file")
                .arg(arg!(<pkgfile> "package file to inspect"))
        )
        .subcommand(
            Command::new("test-ignore")
                .about("test ignore patterns, output which files would be added or ignored")
                .arg(arg!(<file>... "files to include in the package"))
                .arg(arg!(-v --verbose "verbose output, show status for every file"))
                .arg(arg!(--"wrap-with-dir" <dirname> "Wrap all files into one root dir"))
                .arg(arg!(--"ignore-file" <path> "Use an ignore file to exclude or include files")
                    .action(ArgAction::Append)
                    .required(false)
                    .value_hint(clap::ValueHint::FilePath)
                )
                .arg(arg!(--"ignore-pattern" <pattern> "Use an ignore pattern to exclude or include files")
                    .visible_alias("pattern")
                    .action(ArgAction::Append)
                    .required(false)
                )
                .arg(arg!(--"file-modes" <path> "Read file modes from a file")
                    .action(ArgAction::Append)
                    .required(false)
                    .value_hint(clap::ValueHint::FilePath)
                )
        )
        .arg(arg!(<file>... "files to include in the package"))
        .arg(arg!(-n --name <name> "The name of the package")
            .required(true)
        )
        .arg(arg!(--version <version> "The package's version")
            .required(true)
        )
        .arg(arg!(--unversioned "Build a package without a version. The package will be invalid until versioned later.")
            .conflicts_with("version")
        )
        .arg(arg!(--mount <mount> "The packages mount point, where to install into")
            .required(true)
        )
        .arg(arg!(--"ignore-file" <path> "Use an ignore file to exclude or include files")
            .action(ArgAction::Append)
            .required(false)
            .value_hint(clap::ValueHint::FilePath)
        )
        .arg(arg!(--"ignore-pattern" <pattern> "Use an ignore pattern to exclude or include files")
            .action(ArgAction::Append)
            .required(false)
        )
        .arg(arg!(--"file-modes" <path> "Read file modes from a file")
            .action(ArgAction::Append)
            .required(false)
            .value_hint(clap::ValueHint::FilePath)
        )
        .arg(arg!(-o --"output-dir" <dir> "directory to put the built package file"))
        .arg(arg!(--"wrap-with-dir" <dirname> "Wrap all files into one root directory"))
        //.arg(arg!(--"strip-parent-dir" "Strip off parent directory, only include the contents of the given directory"))
        .arg(arg!(--semver "Require the version to be a valid semver [see http://semver.org]"))

        // note: symlinks that point to absolute paths are always rejected
        .arg(arg!(--"allow-symlink-dne" "Allow symlinks to files that do not exist"))
        .arg(arg!(--"allow-symlink-outside" "Allow symlinks to files outside the package"))

        .arg(arg!(-v --verbose "verbose output, show status for every file"))
        .arg(arg!(--"no-cleanup" "do not remove intermediate files"))
        .arg(arg!(complevel: -c <level> "compression level")
             .value_parser(clap::value_parser!(u32))
             .default_value("0")
        )
        .arg(arg!(threads: -T <threads> "number of threads to use during compression. 0=# of CPUs")
             .value_parser(clap::value_parser!(u8))
             .default_value("0")
             // negative values are not actually accepted, but this gives a better error message
             .allow_negative_numbers(true)
        )
        .arg(arg!(--description <description> "Provide a brief description of the package"))
        .arg(arg!(--kv <keyvalue> "Key-Value")
             .value_name("key=value")
             .action(clap::ArgAction::Append)
             .value_delimiter('=')
             //.num_args(2)
        )
        .arg(
            Arg::new("depend")
                .long("depend")
                .action(clap::ArgAction::Append)
                .value_name("pkg[@version]")
                .help("Add a dependency")
        )
}
