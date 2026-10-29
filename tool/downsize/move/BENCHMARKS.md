# Local resize benchmarks

Build the application, resize command and loopback laboratory with the same Go
toolchain used by CI. All targets and temporary directories must be explicit.

```sh
GOTOOLCHAIN=go1.27.0 GOFLAGS=-mod=vendor go build -o /tmp/go-carbon ./
GOTOOLCHAIN=go1.27.0 GOFLAGS=-mod=vendor go build -o /tmp/resize ./tool/downsize
GOTOOLCHAIN=go1.27.0 GOFLAGS=-mod=vendor go build -o /tmp/resize-lab ./tool/downsize/move/lab
/tmp/resize-lab -help
```

Use a new temporary directory for each run. Record fixture dimensions, source
and target encoding, transferred raw/stored bytes, wall time, CPU time and peak
RSS. Compare alternating runs with identical fixture dimensions and cache state.
The lab validates transferred point values and exposes profiling options. Keep
cold/warm cache results separate. Loopback throughput does not establish device,
network or foreground workload capacity on another machine.

Source restarts invalidate an existing whole-store source session. Start a new
baseline after that event. Worker/controller resumption does not imply source
session persistence. Shared CPU and I/O governors are ceilings; validate their
interaction with the actual foreground workload before selecting limits.
