# Exact sealed floats

`store-cold-pack-float-codec = true` enables the exact float encoder during
cold packing. It defaults to false and reloads through SIGHUP. Readers always
support the new format. Integer and rounded-decimal sealing keep their existing
modes. Nothing changes on the ingest path.

The writer constructs today's complete frame first. For float groups of at
least 129 points it selects a codec on the first 64 points from today’s
encoding, exact decimal scaling with raw exceptions (ALP), a raw IEEE dictionary
with packed IDs, and Chimp128 with optional Zstd. It then builds the selected
complete group frame. The old frame wins ties and any larger candidate. When the legacy frame exceeds five bytes per point, Chimp must save at least
12.5% to replace it: smaller savings on dense frames did not justify the
additional dependent reads in capture benchmarks.
ALP and dictionary remain eligible. Every candidate includes timestamp framing,
checksum, dispatch and compression. State resets at each group.

ALP accepts an integer mantissa only if division by the scale reconstructs the
original IEEE word. Other values carry an ordinal and their original 64 bits.
NaN payloads, signed zero, infinities and subnormals remain exact. Chimp uses an
explicit point count instead of a NaN terminator. Dictionary equality compares
IEEE words, not floating-point equality.

## Durable format and compatibility

Sealed group version 4 keeps the 16-byte v3 header: version, flags, uint16 count,
uint32 first timestamp, uint32 step, CRC32 IEEE. Only implicit-time (4) and Zstd
(8) flags are allowed. The CRC covers the first twelve header bytes with the
Zstd bit cleared, followed by the uncompressed body. The body contains the v3
timestamp deltas when needed, then one dispatch byte: 1 = exact ALP, 2 =
dictionary, 3 = Chimp128. Bounds and decompression limits remain unchanged.

ALP stores an exception count followed by gap-varint/uint64 pairs, then the
existing decimal integer encoding with exponent -18 through 0. Dictionary stores
a varint cardinality, little-endian uint64 words in first-occurrence order and
little-endian packed IDs of width `ceil(log2(cardinality))`. Chimp uses the
four Chimp128 reference/window forms and lead buckets 0/8/12/16/18/20/22/24.
Packed streams require zero unused bits and no trailing bytes.

Enabling the flag does not write the v11 marker. Before publishing the first
version-4 group, maintenance syncs and atomically replaces CHUNKSTORE with
`go-carbon-chunks-v11` (v10 belongs to the earlier ALP experiment). It then commits the batch. A crash between those steps
can leave a conservative fence. Earlier binaries reject the unknown marker.
A reader binary with the flag off can still open v11 stores and retain their
fence. Transfers advertise accepted capability separately from stored format,
and sync the destination's fence before ingesting new groups.

## Existing history and rollback

Set `store-cold-pack = "float-repack"` and enable the flag to rewrite existing
v3 float groups. Native keys and group boundaries stay fixed. The ordinary
cold-pack byte/CPU/disk/debt gates, revision checks and checkpoints apply. Change
back to `exact` or `decimal` afterward. Mode, write-window and float-flag changes
reset the cursor; budget-only changes preserve it. All options reload on SIGHUP.

For rollback to a release without the reader, stop the daemon and use the new
offline tool, retaining the original `-source` binding when applicable:

```sh
cold-pack -store STORE -source SOURCE -mode float-downgrade -downgrade
```

This rewrites version-4 groups to v3 without a size fallback, even when larger.
After a clean complete pass it audits all visible keys, including orphan and
staged generations, at the configured byte rate, flushes, then lowers the fence
to v9. Errors, conflicts, skipped ranges, interruption and remaining v4 groups
retain the v11 fence. Run again after resolving them. Reader-only rollback to
this binary requires only switching the flag off. Full `unpack -downgrade`
still lowers a completely unpacked store to v7.
