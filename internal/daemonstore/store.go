// Package daemonstore defines the storage boundary used by the daemon. Engine
// implementations own journalling, replay, immutable snapshots and policy state.
package daemonstore

import (
	"context"
	"errors"
	"time"

	"github.com/go-graphite/go-carbon/points"
)

var (
	ErrUnavailable = errors.New("immutable-parts daemon backend is not available in this build")
	ErrNotFound    = errors.New("metric not found")
)

type Archive struct{ Step, Points int }
type Metric struct {
	Archives          []Archive
	AggregationMethod string
	XFilesFactor      float32
}
type Match struct {
	Name string
	Leaf bool
}

// Series owns its values; no snapshot, iterator or mutable buffer may escape
// Fetch. The implementation must charge response allocations to its read budget.
type Series struct {
	Metric            Metric
	From, Until, Step int
	Values            []float64
	// Release returns the backend response reservation. Protocols normally use
	// ResponseScope; direct callers must call Release after consuming Values.
	Release func()
}
type Stats struct {
	IngestedPoints                     uint64
	Parts                              int64
	CompactionDebtBytes                uint64
	WriteStalls                        uint64
	SnapshotPins                       int64
	SealLatencySeconds, ReadP99Seconds float64
}
type Options struct {
	// WriteEngine selects the restart-only standalone or shared-core path.
	WriteEngine string
	// JournalCodec selects the restart-only receipt writer: empty/pdx1 or pdc3.
	JournalCodec string
	// Legacy storage is a read-only base. Open must never start its maintenance
	// or writer paths, even when a coarse archive straddles the cutover.
	LegacyDataDir, LegacyStoreDir, LegacyBackend string
	DataDir                                      string
	Shards, Owners                               int
	PartInterval                                 time.Duration
	Policy                                       func(string) (Metric, error)
}

// Reader is shared by HTTP, carbonlink and gRPC. Find includes branch matches
// and uses Graphite glob semantics. All returned memory belongs to the caller.
// Calls must honor cancellation; shutdown cancels and joins all admitted calls
// before closing the backend, including protocols without a request context.
type Reader interface {
	Fetch(context.Context, string, int, int) (*Series, error)
	Info(context.Context, string) (Metric, error)
	Find(context.Context, string) ([]Match, error)
	CacheQuery(context.Context, string) ([]points.Point, error)
}

type Failure struct {
	Operation string
	Err       error
}

// Backend is opened only after replay finishes. Append must journal the original
// batch, order and application clock before visibility, with periodic sync. It
// cannot retain Points or Data: receivers reuse buffers after Append returns.
// Flush is a durability barrier for every accepted append and sealed frontier.
// Close joins background jobs and is called only after input and reads stop.
// Stats must be safe concurrently with queries, appends and Close, and after Close.
// Errors reports asynchronous journal, seal and compaction failures; a nil
// channel is permitted only when the backend has no background work.
type Backend interface {
	Errors() <-chan Failure
	Reader
	Append(*points.Points) error
	Flush() error
	Close() error
	Stats() Stats
}

type OpenFunc func(Options) (Backend, error)
