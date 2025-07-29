# dqlite-snapshot-unpack

A small utility to unpack dqlite snapshots.

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
