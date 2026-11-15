# Read-only journal byte probe

This offline tool opens input-log segments read-only, validates frame headers,
replays their logical contents and passes the same points through the PDC2
journal codec. It checks the decoded canonical name, clock, timestamp and every
IEEE value bit against the source before reporting a result.

```
go run ./tool/v2-bytes -input /path/to/immutable/input-log -segments 7
```

The newest segment is excluded. Other descriptors are opened before decoding,
and the probe rejects a header change during each segment replay. A descriptor
does not pin a recycled inode; a live source can therefore make the probe fail.
`-manifest` reads headers only; `-wait` holds selected descriptors before decoding.
The source must use the current input-log format. Run only with an explicitly
approved resource window on a shared host.

Byte counts are logical bytes, including source framing and 32 bytes per PDC2
record. FirstArrival nanoseconds are preserved by default, including zero.
`-arrival-seconds` explicitly selects a lossy whole-second diagnostic; that mode
must never be described as full-fidelity source-record compression.
PDC2 dictionaries reset per source segment; each source frame is split
at a conservative 4MiB expanded estimate. Metadata definitions are empty: the
reported journal cost excludes placement/policy metadata and checkpoints. The
probe writes no journal or database and does not measure physical writes,
compaction, seals, fsync latency, SSTs, read latency or throughput capacity.
It cannot establish an at-rest compression ratio.

`-archives /path/to/exports` instead reads complete native transfer-v2 responses,
decodes complete native snapshot archives (including computed rollup tails),
counts exported occupied entries and configured capacity,
and re-encodes the same slots with the exact packed-block encoder, including
its raw IEEE fallback for ring images outside the ordinary codec domain. Every
decoded timestamp and value bit is checked, including fallback blocks. The
benchmark-local codec copy can be removed once the shared package is on the
base branch. Output includes retention strata and packed-descriptor counts.
Source KV bytes include each exported key and value; neither source KV bytes
nor sealed payload bytes represent physical SST size. Policy/name/key overhead,
SST compression, obsolete versions and compaction scratch require separate
measurements. Archive sampling and authenticated export are the caller's job.

`-partition 0` selects one of the input log's 16 independent stream dictionaries.
The read-only virtual reader preserves every selected frame byte and skips other
partitions without decoding their payloads. Source and PDC2 numerators then use
only the selected points; each selected segment's 64-byte header is charged in
full. Report that sampling stratum and the segment creation-time coverage with
any result. It is not a measurement of every host input point.
