# Raw qualification logs

Each `.txt` file in this directory is an unmodified copy of the named completed
benchmark log from `/tmp/go-carbon-chunk-prototype-results`. `summary.csv`
transcribes the timed-loop fields from those logs. No binaries or profiles are
included.

The `cwhisper-ooo` rows retain their requested cache setting for traceability,
but that file backend does not use Pebble's cache option. `allocated_bytes` in
the summary is the final `allocated-bytes` disk value; timed-loop Go heap
allocation is separately reported as `allocated_b_per_point`.

For the matched 100,000-metric, 80-round late-write pair on the local VM, the
logged Pebble-chunks/control ratios are 0.313 for CPU microseconds per point
and 0.044 for final allocated disk bytes. This is a synthetic local result;
memory, wall time, and setup time remain separate fields in `summary.csv`.

`fully-merged-file-probe.txt` is a separate allocation-only check on the XFS
development host: the same 80-round late trace with 100 identical file metrics,
all merged. Its 819,200 allocated bytes exclude directories. It is not included
in the timing CSV because other validation work was running on the host. The
SSH nonce wrapper was removed from this probe log; its test output is unchanged.
