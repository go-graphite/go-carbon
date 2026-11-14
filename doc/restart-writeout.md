# Writeout cadence across restarts

Automatic writeout pacing (`[whisper] max-updates-per-second = -1`) uses
wall-clock phases derived from each metric's hash. Starting a process or
finishing metadata warmup does not move these phases. A full batch remains
eligible during startup. Queue planning overlaps consumption of an existing
plan, including when every queued metric is already warm.

A checkpoint records each cached metric's first arrival in an optional companion
named `.arrival-v1.<dump filename>`. Its `GCAGE001` header identifies version 1.
Each record contains a varint name length, the name, a signed varint Unix
nanosecond arrival, and CRC32C over those fields. The complete companion is
validated before its ages are used. Ordinary dump point encodings are unchanged;
older readers ignore the companion. Checkpoint retirement removes it too.

Continuous input-log frames append arrival metadata as a zstd skippable frame,
with an `AGE1` version marker. The existing frame CRC covers it. There is one
signed varint Unix nanosecond arrival per metric group, in stream order. Older
readers skip it and decode identical point bytes. New readers also accept older
frames without it. Arrival timestamps do not replace or change point timestamps
or floating-point bits.

When upgrading from a dump or journal without ages, automatic pacing makes
restored entries eligible immediately under the existing writer admission limit.
It cannot reconstruct first arrival from a point timestamp, since backfill and
out-of-order input are supported. Subsequent dumps and journal replay preserve
the recorded age. Fixed-rate pacing keeps its existing startup behavior.
