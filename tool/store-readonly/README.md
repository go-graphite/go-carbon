# Offline metric inspection

`store-readonly` reads an existing, stopped chunkstore using `Options.ReadOnly`.
It retains database locking, disables maintenance, and refuses filesystem mutations.
It has no listener, import operation, catalogue walk, or write mode.
An existing incremental-migration marker retains its recorded source binding;
the reader verifies that source's identity without changing it.

Build with `CGO_ENABLED=0 go build -mod=vendor ./tool/store-readonly`.
Feed one JSON request per line:

```sh
printf '%s\n' '{"name":"example.metric","ranges":[{"from":1790000000,"until":1790003600}]}' |
  ./store-readonly -directory /absolute/path/to/stopped/store
```

Each response includes the last physical point across all archives, durable last-update
activity, and each archive's zero/nonzero counts and last point. Optional half-open
ranges select physical points to print. With no ranges, points are counted but only
the last point is printed. Values use hexadecimal IEEE-754 bits, preserving signed
zero and non-finite values. A missing metric returns `error: "not found"`; other
read failures terminate the process and must not be treated as missing data.

A last point's timestamp and a metric's last-update activity are distinct. Expiration
policy must be checked separately; this tool never declares data safe to delete.
Archive counts describe physical snapshots, including slots whose retention may
have elapsed. They are not render counts or disjoint timestamp totals.

The test creates and closes temporary exact and continuation stores, reads bounded
ranges through this command's entry point, verifies values and last-point metadata,
and compares every file's SHA-256, size and nanosecond mtime before and after.
