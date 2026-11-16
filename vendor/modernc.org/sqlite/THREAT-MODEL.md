# Threat model

The working sheet behind the Scope section of [SECURITY.md](SECURITY.md): the
places where input from outside the application reaches the hand-written Go of
this driver, what that input can be, and what goes wrong if the code it reaches
is wrong. Each row records its status and the commit that settled it. Last
checked against v1.60.0.

It covers four surfaces: the connection string, the connection lifecycle, the
page cache binding and the VFS bridge. The transpiled C in `lib/` and `vec/` is
a different exercise, described at the end.

## Positions

The rows name who supplies the input, because the answer to "is this in scope"
depends on it:

- **A1** writes the *connection string*: an application that builds a DSN from
  configuration, an environment variable or a URL parameter.
- **A2** writes *SQL text*, the usual injection position.
- **A3** writes *data in a table*, but not the schema or the statements.
- **A4** supplies a *database file*, or can write to one while it is open.
- **A5** is *another user of the same process*: a later borrower of a pooled
  connection, a second goroutine, or code the application plugs in.

A1 is out of scope as an attacker position (SECURITY.md, since a24dc91): a DSN
carries the authority of the application that opens it. Its rows stay here
because `StrictPragmas` and the rule that a rejected DSN applies nothing are in
scope, and because they say what a DSN taken from an untrusted source can do.

What sets this driver apart from a cgo one is that A3 and A4 reach Go that was
transpiled from C, whose memory faults surface through the Go runtime as a
panic, a fatal error or silent corruption, and that the page cache binding and
the VFS bridge are hand-written Go on the same path.

Status is one of: **Fixed** or **Documented** in a commit, **Holds** with where
the code keeps it, **Struck** with the reason, **Out of scope** by SECURITY.md,
and **Open** with what is missing.

## 1. The connection string

Parsed in `applyQueryParams` (sqlite.go:285), with `getVFSName` (sqlite.go:118)
reading the same query string first. Validation is split from application:
every driver key is checked before the apply phase runs its first statement,
because `_journal_mode` and `_auto_vacuum` change the file permanently and a
later typo must not leave a half-converted database. The file itself is opened,
and created if it does not exist, before either phase.

| Pos. | Supplies | Reaches | If it is wrong | Status |
| --- | --- | --- | --- | --- |
| A1 | `_pragma=...` | run as SQL text with `PRAGMA` prepended, in the apply phase, after `_busy_timeout` and `_auto_vacuum` and before the other keys | Anything after a `;` runs too: `_pragma=foreign_keys(1);ATTACH 'x.db' AS x` attaches, and creates, `x.db`, and `_defensive=1` does not stop it. A `_pragma` that fails leaves the ones before it applied. | **Documented** in 3f65c53 (driver.go:143), which also adds `StrictPragmas(true)` (strictpragma.go:43): a value holding more than one statement is rejected in the validation phase, held to SQLite's own parser by `TestSingleStatementAgainstSQLite` and `FuzzSingleStatement`. |
| A1 | `vfs=name` | `getVFSName`, then the `zVfs` argument of `sqlite3_open_v2` | Every read and write of the connection goes to a VFS the application did not choose. | **Out of scope** (a24dc91): selecting the VFS is what the key is for. |
| A1 | `_txlock`, `_journal_mode`, `_auto_vacuum`, `_busy_timeout`, `_foreign_keys`, `_synchronous`, `_query_only`, `_dqs`, `_defensive` | validated before anything is applied | A bad value later in the DSN must leave the file as it was. | **Holds**: "validation precedes application" in dsn_test.go:210 (0895392) opens a DSN with a valid `_journal_mode` or `_auto_vacuum` and an invalid later key, then reopens the file and checks both settings; `TestDefensiveDSNValidation` (defensive_test.go:34) checks that an invalid `_defensive` combination creates no file. |
| A1 | `_time_format`, `_timezone`, `_inttotime`, `_texttotime` | conversion of values on read and write | Wrong timestamps, not memory unsafety. | **Struck**: harmless, and `time.LoadLocation` refuses a name containing `..`. |
| A1, A4 | a `file:` name with `immutable=1` | SQLite's own URI handling | SQLite assumes the file cannot change; if it does, results are undefined. | **Out of scope** (a24dc91). `Driver.Open` lists the parameter and links SQLite's URI documentation, which states the condition (driver.go:269); the driver's own text does not repeat it. |

## 2. The connection lifecycle

`Driver.Open` (driver.go:287) opens the connection, installs the registered
user-defined functions and collations, runs the connection hooks and registers
the virtual table modules, and closes the connection if any of them fails. `database/sql` then hands the
same `driver.Conn` to whichever caller borrows it next.

| Pos. | Supplies | Reaches | If it is wrong | Status |
| --- | --- | --- | --- | --- |
| A5 | a borrowed connection returned to the pool | per-connection state: PRAGMAs set with Exec, ATTACHed databases, temporary tables, anything registered through `sql.Conn.Raw` | State one borrower sets is visible to the next. | **Documented** in 73f6671 (doc.go:160): the driver does not reset it; use DSN keys or a connection hook for state every connection should have. |
| A2, A3 | a statement that calls a user-defined function | the pooled `*FunctionContext` handed to the callback | A callback that keeps the context past its return holds one that is zeroed or belongs to another call. | **Struck**: the type has no methods and no exported fields, so a retained one is inert. The note on the struct (sqlite.go:708, 73f6671) requires any method added later to be safe on one. |
| A5 | a second goroutine on one connection reached through `sql.Conn.Raw` | the connection's `libc.TLS` and driver state, before SQLite's `SQLITE_OPEN_FULLMUTEX` mutex | Two goroutines on one connection can corrupt memory. | **Documented** in 73f6671 (doc.go:170): such a connection is not safe for concurrent use. |
| A5 | a context cancelled while its query runs | `conn.interrupt` (conn.go:729), called from the goroutine watching the context | `sqlite3_interrupt` ran on the `libc.TLS` of the query it interrupts. Safe only because the transpiled function is a single atomic store today. | **Fixed** in 9ab49d8: the interrupt runs on a TLS of its own. Found next to the row above, not in it. |
| A1 | a DSN that fails part way through the apply phase | `newConn` (conn.go:56) closes and discards the connection | Assignments to the connection must not outlive the failure, which matters as the list of applied keys grows. | **Holds**: `newConn` closes and discards the connection when `applyQueryParams` fails (conn.go:151), and the comment at the top of `applyQueryParams` states the rule for whoever adds a key. |

## 3. The page cache binding

When an application calls `RegisterPageCache`, SQLite calls the binding in
`pagecache.go` and `pagecache_trampolines.go` for every page it reads;
`pcache/` is the optional reference implementation. SQLite stores an opaque
handle, the trampolines recover the binding under a read lock and map each
`(binding, key)` pair to a stub allocated with `libc.Xcalloc`. Without
`RegisterPageCache` none of this runs.

| Pos. | Supplies | Reaches | If it is wrong | Status |
| --- | --- | --- | --- | --- |
| A3, A4 | any workload that reads pages | `Fetch`, `Unpin`, `Rekey`, `Truncate` and `Destroy` of the registered cache | A stub freed while SQLite still holds it, or returned for the wrong key, is silent corruption of data SQLite believes it read correctly. | **Fixed** in 0378426 for the case the binding can see: a `Fetch` that returns nil or a different `Page` for a key SQLite holds pinned now panics instead of freeing the stub (the `pinned` field at pagecache_trampolines.go:66, `pcacheContractViolation` at :76), tested by `pagecache_pin_test.go`. A `Cache` that frees the buffers of a `Page` it keeps returning, or returns another key's page on a first fetch, is not detected; that is the contract in the next row. SECURITY.md names the binding in scope (8cdb8e2, 72dd037). |
| A5 | a `PageCache` implementation in the application | the same callbacks, now calling application code | An implementation that frees or replaces a page SQLite holds pinned breaks the stub lifetime. | **Struck** as a position: it is a contract, not an attacker, and the binding now enforces it (0378426). The documentation says that `PageCache.Create` may run concurrently and that under `cache=shared` a `Cache` is called from several goroutines (c203e39). |
| A4 | a file that makes SQLite truncate or rekey often | the `Truncate` sweep and the `Rekey` path | Stub lifetime again, on the least exercised paths. | **Open**: since f2c4107, `make test_pcache` runs the whole suite through the binding (the commit records a run under `-race` as well), and `pcache/` has unit tests for both paths; the SQL workload of `pcache/e2e_test.go` does not reliably reach `Rekey`; `BenchmarkPoolEvictionChurn` does, about 13 per cycle by its own count, but a benchmark is not part of the suite. No fuzz target drives sequences of `Truncate` and `Rekey`. |
| A5 | an import of `modernc.org/sqlite/vec` | `vec`'s `init`, which initializes SQLite before any `RegisterPageCache` | Registering a page cache fails in such a program. | **Documented** in c203e39 on `RegisterPageCache` and in the `vec` and `pcache` package documentation. Found next to the rows above. |

## 4. The VFS bridge

`vfs/` exposes a Go `fs.FS` to SQLite through C ABI entry points in
`vfs/patches.go` (`vfsOpen`, `vfsRead`, `vfsAccess`, `vfsFileSize`,
`vfsClose`), with `New` as the constructor. Open file systems and files are
`uintptr` handles into one map.

| Pos. | Supplies | Reaches | If it is wrong | Status |
| --- | --- | --- | --- | --- |
| A2, A4 | SQL that writes to a database opened through a VFS made by `New`, or closing a WAL database shipped with its `-wal` file | `vfsOpen`'s out-flags (vfs/patches.go:138), then the `xWrite` that `vfsio` did not have | Reported as writable, the file took writes this VFS cannot do: with `journal_mode=OFF` an `UPDATE` reached the missing `xWrite` and the process panicked, and so did the checkpoint on closing the WAL database; with the journal on, the `UPDATE` failed with a misleading `SQLITE_NOMEM`. | **Fixed** by merge request !145: `vfsOpen` reports `SQLITE_OPEN_READONLY`, so every write fails with `SQLITE_READONLY` (`TestVFSWriteRefused`) and the checkpoint is skipped (`TestVFSWALReadOnlyClose`). Since then `xWrite` and `xTruncate` answer `SQLITE_READONLY` and `xSync` `SQLITE_OK` instead of being nil: no method SQLite can call is missing. Found after this sheet was drafted. |
| A2, A4 | `PRAGMA journal_mode` leaving `PERSIST` or `TRUNCATE`, or a 0-byte database next to a `-journal` file in the served file system | `xDelete`, which SQLite calls without consulting the read-only flag, from `sqlite3PagerSetJournalMode` and from `hasHotJournal` | The `xDelete` transpiled from vfs.c is a stub that aborted the process, and the code after the abort would have unlinked the path on the host file system, relative to the working directory. | **Fixed** after !145: `New` installs `vfsDeleteReadOnly`, which deletes nothing and fails with `SQLITE_IOERR_DELETE`, which both callers tolerate (`TestVFSDeleteRefused`). Found while reviewing !145. |
| A4 | a database opened through an application `fs.FS` | `vfsRead` (vfs/patches.go:144), with an offset and length chosen by SQLite | A read that ends past the end of the file must return `SQLITE_IOERR_SHORT_READ` and zero the unread tail, as SQLite's VFS contract requires. | **Struck**: `vfsRead` does both. The three read tests passed with either half broken, so `TestVFSReadEOF` now calls `vfsRead` directly and asserts both. |
| A5 | 2^32 opens in one process on a 32-bit target | the handle map through `addObject` (vfs/patches.go:79) | One counter numbers file systems and files alike; on the 32-bit targets it wrapped after 2^32 opens and overwrote a live handle. | **Fixed** in 3c3e178: `addObject` skips handles in use, and 0, tested across a wrap by `TestAddObjectSkipsLiveHandles` (vfs/close_test.go:68). |
| A5 | `FS.Close` while a database is open through it | `FS.Close` (vfs/patches.go:270), then the next `vfsAccess` | The VFS was freed while connections still called through it: a panic, or with the memory reused, another file system. | **Fixed** in 3c3e178: `Close` refuses with an error wrapping `ErrInUse` while files are open (`TestFSCloseInUse`), and `getObject` and `removeObject` release the lock before they panic. `Close` concurrent with an open stays the caller's to avoid, as documented at vfs/patches.go:267: the count cannot cover the moment between SQLite finding the VFS and opening the file. |

## Out of scope

As in SECURITY.md, and for the same reasons:

- SQL injection by the application. Placeholders exist.
- Queries that are simply expensive. SQLite's own limits are the tool.
- A hostile database file that is cleanly rejected with an error. That is the
  library working.
- Anything reachable only after editing generated code by hand.

## Not covered: the transpiled C

The largest surface is the C in `lib/` and `vec/`, transpiled to Go, where the
bug class is generated Go that does not faithfully implement the C. Reasoning
about entry points does not reach it; differential testing against the C
amalgamation does.
