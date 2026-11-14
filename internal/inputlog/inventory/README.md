# Offline input journal recovery inventory

`storectl inventory inputlog` enumerates candidate metric names from an owner-copied,
exact-length input journal prefix. It reuses the normal frame/stream decoder,
verifies every payload CRC and the complete captured SHA256, and includes frames
before the current replay watermark. It opens the copied file read-only and never
opens a running log, acquires its lock, changes replay state or writes metric data.

For a concrete bounded example, first capture an immutable exact-length journal
prefix and record its SHA256, then use the manifest beside this README:

```sh
storectl inventory inputlog -manifest example.json -output received-candidates.jsonl.gz
```

The gzip JSONL starts with the verified snapshot identity, counts and limitations,
then emits sorted names, per-window record counts and first/last selected metric
timestamps. Duplicate records are counted; this is not a count of unique cells.
NaN values do not select a name; signed infinities do. The command refuses corrupt
frames, incomplete dictionaries, preallocated tails, changed size/hash and its
byte/point/frame/name/dictionary limits. It removes its new output on any failure.
It has a five-minute deadline and requires an external memory limit when run on a
store. Defaults are2M names and256MiB dictionary bytes; explicit manifest limits
may raise these to at most16M names and2GiB. A population larger than2M candidates requires an explicit reviewed name and
dictionary limit. Run an8M/1GiB manifest under an8GiB memory limit after confirming
32GiB available.

This inventory is a candidate list, not fill authority or a completed recovery
proof. Raw point timestamps do not establish receive times. Records outside the
copied prefix or selected timestamp windows remain uncovered. Before filling,
bind each candidate to its positively acknowledged diversion epoch and read the
source store's committed archives. Use `storectl sync` to fill gaps without replacing target values;
never replay journal values directly or replace occupied finite/Inf cells. Keep
the captured prefix until the inventory is verified, then delete the owned copy
 after exporting the required audit evidence.
