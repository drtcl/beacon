# bpm

This is a rust rewrite of my python-implemented bpm package manager.

Curretly only has a packaging util that should work with the python version.



### Planning

like apt update, scan update servers and store what is available

    bpm scan

install directly from a package file

    bpm install <path/to/package/file>

or install from an update server

    bpm install <package_name>

uninstall packages

    bpm uninstall <name>

like apt upgrade

    bpm update <name>

A set of cache commands

    bpm cache fetch <pkg>
    bpm cache clear

info query commands

    bpm info <pkg>
    bpm listfiles <pkg>
