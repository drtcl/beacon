# The Beacon Package Manager - bpm

A general purpose, flexible, configurable, package manager for no specific language, OS, or platform.

# Usage

## Scan for packages

Query your providers for what packages and versions are available.

    bpm scan

## Search for packages

    bpm search foo
    bpm search foo --exact

or list all available packages

    bpm list available

## Install a package

install the latest `foo` package from a **provider**:

    bpm install foo

install a specific version:

    bpm install foo@1.2.3

install from a local package file:

    bpm install path/to/foo_1.2.3.bpm

## Uninstall a package

    bpm uninstall foo

## Updating

Update all packages:

    bpm update

Update a specific package:

    bpm update foo

Update a specific package to a specific version:

    bpm install --update foo@1.2.3

# Providers

**bpm** can pull packages from multiple sources.

- [x] filesystem
- [x] http
- [x] https
- [ ] bpmd

Accepted directory structures:

    1) flat -- packages listed at root

        pkg/
            foo_1.0.0.bpm
            foo_2.0.0.bpm
            bar_0.1.0.bpm

    2) named -- packages in named directories

        pkg/
            foo/
                foo_1.0.0.bpm
                foo_2.0.0.bpm
                channels.json        (optional)
                channel_stable/      (optional)
                    foo_1.1.1.bpm
                    foo_2.1.1.bpm
                channel_beta/        (optional)
                    foo_1.1.0-beta.bpm
                    foo_2.1.0-beta.bpm
            bar/
                bar_0.1.0.bpm
                channels.json        (optional)

# Configuration

**bpm** uses one main configuration file.
This is the order of places that **bpm** searches to find the config file:

1. bpm_config.toml next to the executable
2.     config.toml next to the executable
3. bpm_config.toml in [user's config dir](https://docs.rs/directories/latest/directories/struct.BaseDirs.html#method.config_dir)
4. bpm_config.toml in any parent dir from executable


Example `bpm_config.toml`:

    database = "db.json"
    use_default_target = true

    [cache]
    dir = "${BPM}/cache"
    retention = "10 days"
    auto_clean = true

    [mount]
    TARGET = "/path/to/install/dir"

    [providers]
    test1 = "http://localhost:8000/pkg/${OS}/"
    test2 = "file:///path/to/packages/"

## String Replacements

The config file supports some basic string replacements using `${KEY}` syntax.

For example:

bpm_config.toml:

    [cache]
    dir = "${BPM}/cache"

    [mount]
    MAIN = "${BPM}/install/bin/${OS}/${ARCHX8664}"

direct replacements:

| key | value |
|-----|-------|
| `BPM` | directory of the bpm executable |
| `THIS` | directory of the config file |
| `OS`  | `linux`, `unix`, `windows`, `darwin`, `wasm`, `unknown` |
| `ARCH3264` or `POINTER_WIDTH` | `32` or `64` |
| `ARCHX8664` | `x86`, `x86_64`, `arm`, or `aarch64` |

basic true/false replacements that optionally take true and false strings:

  ${VAR:true_value:false_value}

For example `${windows:foo:bar}` will result in "foo" on windows and "bar" on anything non-windows.

| key | value |
|-----|-------|
| `${linux}` | "linux" |
| `${windows}` | "windows" |
| `${macos}` | "macos" |
| `${unix}` | "unix" |
| `${bsd}` | "bsd" |
| `${freebsd}` | "freebsd" |
| `${openbsd}` | "openbsd" |
| `${netbsd}` | "netbsd" |
| `${wasm}` | "wasm" |
| `${32}` | "32" |
| `${64}` | "64" |
| `${x86}` | "x86" |
| `${x86_64}` | "x86_64" |
| `${x86-64}` | "x86-64" |
| `${amd64}` | "amd64" |
| `${x64}` | "x64" |
| `${aarch64}` | "aarch64" |
| `${arm}` | "arm" |
| `${gnu}` | "gnu" |
| `${msvc}` | "msvc" |
| `${musl}` | "musl" |

environment variable replacements:

`${ENV(VAR)}` the value of the env var is the replacement string, <br>
OR <br>
`${ENV(VAR):true_value:false_value}` <br>

An env var is only considered false if it is not defined OR has the value "0".

Note: `BPM` and `THIS` are only valid as prefixes to a path. Example: `"${BPM}/path/parts/`, `${THIS}/../path/parts`, or for a provider `file://{$BPM}/pkg/path"`.

## Mount Points
All packages are install into a *mount point*.
The default mount point is **TARGET** and is used when a package does not specify a mount point to use.
Mount points must be listed in config file.
It is an error to attempt to install a package to a missing mount point.
This does require some coordination between package creators and package consumers.

### Example Scenario
 **bpm** being used to provide packages for a game.

- bpm is configured with two mount moints, one called `ASSETS` and another called `MODS`.
- A package `space_assets` could provide versioned game assets and installs to the `ASSETS` mount point.
- Another package `space_mod` could provide modified game logic and install into the `MODS` mount point.
- `space_mod` can depend on `space_assets` so that when `space_mod` gets installed, `space_assets` also gets installed. (TODO -- dependencies feature is still a work in progress)

# Package Files

## Naming

Package file naming follows this format: `<pkg_name>_<version>.bpm`

## Versioning

Every package must be versioned.

**bpm** prefers using [semantic versioning](https://semver.org) but other versioning schemes will work as well.
For a given package, version format should stay consistent from one version to the next.
Version formats other than semver are compared using a best-effort approach using lexicographic ordering. [[rules]](https://docs.rs/version-compare/latest/version_compare/)

### Channels
If channels are defined for a package, the channel name can be used when installing a package.

    bpm install foo@current

The `channels.json` file can be used to define the channels and looks something like this:

    {
        "current": [
            "1.0.0",
            "0.9.0"
        ],
        "beta": [
            "2.0.0",
        ],
    }

Packages can also just be put into channel directories. The directory name starts with `channel_`, for example: `channel_stable` in:

    pkg/
        foo/
            channel_stable/
                foo-1.1.1.bpm

Multiple versions *can* be specified for a channel, but only the *greatest* version is ever used by bpm. Older versions can be left in the file for your own history or for information for other tools using bpm.

## Creating a Package

    # create foo-1.2.3.bpm from files at files/foo
    bpm pack --name foo -version 1.2.3 --mount TARGET files/foo

    # Packages can be created without knowing the version at creation time
    bpm pack --name foo --unversioned --mount TARGET files/foo
    # and then repackaged with a version later.
    bpm pack set-version --version 3.1.4 foo_unversioned.bpm

