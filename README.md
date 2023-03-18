# bpm -- Bryan's Package Manager

---

[TOC]

---

# About
A general purpose, flexible, configurable, package manager for no specific language, OS, or platform.

# Usage
## Installing a package

install the latest `foo` package from a **provider**:

    bpm install foo

install a specific version:

    bpm install foo@1.2.3

install from a local package file:

    bpm install path/to/foo-1.2.3.bpm

## Uninstalling a package

    bpm uninstall foo

## Searching for packages

    bpm search foo
    bpm search foo --exact

# Providers

TODO

**bpm** can pull packages from multiple sources.

- [x] fs
- [x] http
- [x] https
- [ ] bpmd

>Accepted directory structures:
>
>    1) flat -- packages listed at root
>
>        pkg/
>            foo-1.0.0.bpm
>            foo-2.0.0.bpm
>            bar-0.1.0.bpm
>
>    2) named -- packages in named directories
>
>        pkg/
>            foo/
>                foo-1.0.0.bpm
>                foo-2.0.0.bpm
>                channels.json        (optional)
>                channel_stable/      (optional)
>                    foo-1.1.1.bpm
>                    foo-2.1.1.bpm
>                channel_beta/        (optional)
>                    foo-1.1.0-beta.bpm
>                    foo-2.1.0-beta.bpm
>            bar/
>                bar-0.1.0.bpm
>                channels.json        (optional)
>

# Configuration

TODO

`config.toml`

- database
- cache
  - dir
  - retention
  - auto_clear
- use_default_target
- providers
- mount points

## String Replacements

The config.toml file supports some basic string replacements using `${KEY}` syntax.

For example:

config.toml:

```
[cache]
dir = "${BPM}/cache"

[mount]
MAIN = "${BPM}/install/bin/${OS}/${ARCHX8664}"
```

| key | value |
|-----|-------|
| `BPM` | directory of the bpm executable |
| `OS`  | `linux,` `unix,` `windows,` `wasm,` `unknown` |
| `ARCH3264` | `32` or `64` |
| `ARCHX8664` | `x86` or `x86_64` |


## Mount Points
All packages are install into a *mount point*.
The default mount point is **TARGET** and is used when a package does not specify a mount point to use.
Mount points must be listed in the bpm config.toml file.
It is an error to attempt to install a package to a missing mount point.
This does require some coordination between package creators and package consumers.

### Example Scenario
 **bpm** being used to provide packages for a game.

- bpm is configured with two mount moints, one called `ASSETS` and another called `MODS`.
- A package `space_assets` could provide versioned game assets and installs to the `ASSETS` mount point.
- Another package `space_mod` could provide modified game logic and install into the `MODS` mount point.
- `space_mod` can depend on `space_assets` so that when `space_mod` gets installed, `space_assets` also gets installed.


# Package Files

## Naming
Since most languages do not allow hypens ('-') in identifiers, package names also cannot contain hypens.
This restriction allows for easier identification of a package's name from the package file.
Split on the first hypen, everything before the hypen is the package name.


Package file naming format: `<pkg_name>-<version>.bpm`

## Versioning

Every package must be versioned.

**bpm** prefers using [semantic versioning](https://semver.org) but other versioning schemes will work as well.
For a given package, version format must stay consistent from one version to the next.
Version formats other than semver are compared using a best-effort approach using lexicographic ordering.

### Channels
If channels are defined for a package, the channel name can be used when installing a package.

    bpm install foo@current

The `channels.json` file can be used to define the channels and looks something like this:

```
{
    "current": [
        "1.0.0",
        "0.9.0"
    ],
    "beta": [
        "2.0.0",
    ],
}
```

Packages can also just be put into channel directories. The directory name starts with `channel_`, for example: `channel_stable` in:

>    pkg/
>        foo/
>            channel_stable/
>                foo-1.1.1.bpm

Multiple versions *can* be specified for a channel, but only the *greatest* version is ever used by bpm. Older versions can be left in the file for your own history or for information for other tools using bpm.

### Special Versions
- [ ] TODO

Special versions can be used when installing a package

    bpm install foo@stable

- latest or latestcurrent -- latest non-beta version
- latestbeta -- latest beta version
- latestcurrentorbeta -- latest current or beta version

## Creating a Package
    # create foo-1.2.3.bpm from files at files/foo
    bpmpack --name foo -version 1.2.3 --mount TARGET files/foo

