# Reboot-stable migration bindings

Use `storectl maintain migration record|inspect|rebind -proof FILE` for the
source/store volume-binding contract. The implementation never opens Pebble or
rewrites SST/WAL data. Linux requires `findmnt` and GNU `stat` for XFS UUID and
nanosecond birth-time qualification.

Record once while all original bindings are valid:

```sh
sudo storectl maintain migration record -proof /etc/go-carbon-storage-guard/volume-binding.json \
  -source /data/whisper -store /data/shared-exact -archive /data/originals
sudo storectl maintain migration inspect -proof /etc/go-carbon-storage-guard/volume-binding.json
```

Omit `-archive` when no archive policy exists. Roots must be canonical, disjoint
and on the same XFS UUID. Proof creation is exclusive; do not replace a proof to
adopt a changed directory. Keep proof, binary and parent directories root-owned
outside application-writable storage.

Install this service drop-in using your configuration manager:

```ini
[Service]
ExecStartPre=+/usr/bin/storectl maintain migration rebind -proof /etc/go-carbon-storage-guard/volume-binding.json
```

Replace the old Python ExecStartPre invocation and its helper installation in the
same configuration change after the package supplies `/usr/bin/storectl`.
Keep the existing proof path and bytes. Do not remove an installed helper until
every unit consuming it has switched. Installing a drop-in requires daemon-reload;
it does not require restarting a serving store.

V6/V1 device-number changes require recorded UUID, canonical path and inode
matches, an exclusive **fcntl record lock** on the existing Pebble LOCK inode,
fsynced before-images and atomic mode/owner-preserving marker replacements.
The proof is updated last, so partial repairs resume. Same-device starts only
validate. V6/V7/V9/V11/V12 stable V2 bindings validate birth time or source token
without changing proof or markers, including after device renumbering. Foreign
UUIDs/inodes, unknown formats, symlinks and unrecorded archive policies fail.

Migration progress is available through `/admin/info?scopes=cache,storage` and
`INCREMENTAL-STORE-MIGRATION.json`. The optional daemon worker lease remains a
version-1 `migration-control.json` with generation, workers and expiry. The old
standalone adaptive controller and hook installer are retired; their private
campaign configuration is not an installation requirement.
