# dqlite-snapshot-unpack

A small utility to unpack dqlite snapshots.

## Installation

To install it you can just use typical Go tooling like:

```
go install github.com/marco6/dqlite-snapshot-unpack@latest
```

## Usage

```
dqlite-snapshot-unpack <snapshot>
```

Please not that this tool will unpack all databases in the current folder, so a better usage would be on the lines of:

```
mkdir unpack-folder
cd unpack-folder
dqlite-snapshot-unpack <path-to-snapshot>
```

So that the original folder remains clean.
