package chunkstore

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
)

func TestReclaimCancellationBetweenFilesKeepsJournal(t *testing.T) {
	t.Parallel()
	fs := vfs.NewMem()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var armed atomic.Bool
	var compactions atomic.Int64
	opts := (&pebble.Options{FS: fs, DisableAutomaticCompactions: true, MemTableSize: 4 << 20,
		EventListener: &pebble.EventListener{CompactionEnd: func(pebble.CompactionInfo) {
			if armed.Load() {
				compactions.Add(1)
				cancel()
			}
		}}}).EnsureDefaults()
	opts.Levels = make([]pebble.LevelOptions, 7)
	for i := range opts.Levels {
		opts.Levels[i].TargetFileSize = 8 << 10
		opts.Levels[i].Compression = pebble.NoCompression
	}
	db, err := pebble.Open("/store", opts)
	if err != nil {
		t.Fatal(err)
	}
	key := func(kind byte, i int) []byte { return []byte(fmt.Sprintf("%c%08d", kind, i)) }
	const rows = 512
	value := bytes.Repeat([]byte("retained"), 256)
	for _, kind := range []byte{'t', 'u'} {
		for i := range rows {
			if err := db.Set(key(kind, i), value, pebble.NoSync); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := db.Compact([]byte{0}, []byte{255}, true); err != nil {
		t.Fatal(err)
	}
	tables, err := db.SSTables()
	if err != nil || len(tables[6]) < 8 {
		t.Fatalf("fixture needs multiple bottom files: %v %v", tables, err)
	}
	b := db.NewBatch()
	spans := make(map[byte]*relayoutSpan)
	for _, kind := range []byte{'t', 'u'} {
		spans[kind] = &relayoutSpan{start: key(kind, 0), end: key(kind, rows-1)}
		for i := 0; i < rows; i += 2 {
			if err := b.Delete(key(kind, i), nil); err != nil {
				t.Fatal(err)
			}
		}
	}
	journal := binary.BigEndian.AppendUint64(bytes.Clone(relayoutReclaimPrefix), 1)
	if err := b.Set(journal, encodeRelayoutReclaim(1<<20, spans), nil); err != nil {
		t.Fatal(err)
	}
	if err := b.Commit(pebble.Sync); err != nil {
		t.Fatal(err)
	}
	_ = b.Close()
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}
	s := &Store{db: db, fs: fs, dir: "/store", writeOptions: pebble.Sync, pebbleOptions: opts}
	if err := s.SetRelayoutLimits(1, ^uint64(0)); err != nil {
		t.Fatal(err)
	}
	armed.Store(true)
	started := time.Now()
	if err := s.reclaimRelayout(ctx, true, 0); !errors.Is(err, context.Canceled) {
		t.Fatalf("reclaim did not stop between files: %v", err)
	}
	armed.Store(false)
	t.Logf("cancellation returned in %s after %d compactions", time.Since(started), compactions.Load())
	if n := compactions.Load(); n == 0 || n >= int64(len(tables[6])) {
		t.Fatalf("cancel drained the whole journal: %d compactions, %d bottom files", n, len(tables[6]))
	}
	keys, _, n, err := s.readRelayoutReclaims(context.Background())
	if err != nil || len(keys) != 1 || n != 1<<20 {
		t.Fatalf("partial reclaim retired its journal: %d %d %v", len(keys), n, err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = pebble.Open("/store", opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	s.db = db
	if err := s.recoverRelayoutReclaim(context.Background()); err != nil {
		t.Fatal(err)
	}
	keys, _, n, err = s.readRelayoutReclaims(context.Background())
	if err != nil || len(keys) != 0 || n != 0 {
		t.Fatalf("restart did not finish both namespaces: %d %d %v", len(keys), n, err)
	}
	for _, kind := range []byte{'t', 'u'} {
		for i := range rows {
			got, closer, err := db.Get(key(kind, i))
			if i%2 == 0 {
				if !errors.Is(err, pebble.ErrNotFound) {
					t.Fatalf("deleted key returned: %c/%d %v", kind, i, err)
				}
			} else {
				if err != nil || !bytes.Equal(got, value) {
					t.Fatalf("retained tail changed: %c/%d %v", kind, i, err)
				}
				_ = closer.Close()
			}
		}
	}
}

func TestManualCompactionCancelledBeforeAdmission(t *testing.T) {
	t.Parallel()
	s, _ := openTestStore(t, t.TempDir(), Options{fs: vfs.NewMem()})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := s.compactSpans(ctx, []byte{0}, []byte{255}, nil); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	closeTestStore(t, s)
	if err := s.Compact(); !errors.Is(err, context.Canceled) {
		t.Fatal("compact admitted after close", err)
	}
}

// Include upper-level overlap in every estimate; a broad upper table must not
// disappear from the byte budget just because the deepest files are small.
func TestManualCompactionPlanAllLevels(t *testing.T) {
	table := func(lo, hi string, size uint64) pebble.SSTableInfo {
		return pebble.SSTableInfo{TableInfo: pebble.TableInfo{Smallest: pebble.InternalKey{UserKey: []byte(lo)}, Largest: pebble.InternalKey{UserKey: []byte(hi)}, Size: size}}
	}
	tables := make([][]pebble.SSTableInfo, 7)
	for i := range 2048 {
		tables[6] = append(tables[6], table(fmt.Sprintf("u%05d", i), fmt.Sprintf("u%05d~", i), 128<<20))
	}
	spans := planManualCompaction(tables, []byte("u"), []byte("v"), manualCompactionMaxBytes)
	if len(spans) != 86 {
		t.Fatalf("256 GiB should use tens of spans: %d", len(spans))
	}
	for i, s := range spans {
		if s.bytes > manualCompactionMaxBytes {
			t.Fatal("oversized group", s.bytes)
		}
		if i > 0 && !bytes.Equal(spans[i-1].end, s.start) {
			t.Fatal("gap in plan")
		}
	}
	// Deliberately unordered L0 endpoints, with overlap across both bottom files.
	tables = make([][]pebble.SSTableInfo, 7)
	tables[6] = []pebble.SSTableInfo{table("a", "b", 40), table("c", "d", 40), table("e", "f", 40)}
	tables[0] = []pebble.SSTableInfo{table("c", "f", 15), table("a", "d", 30)}
	spans = planManualCompaction(tables, []byte("a"), []byte("g"), 100)
	if len(spans) != 3 || spans[0].bytes != 70 || spans[1].bytes != 85 || spans[2].bytes != 55 {
		t.Fatalf("upper overlap omitted: %+v", spans)
	}
	// A single oversized deepest file is indivisible. Do not cut on L0 and
	// repeatedly rewrite it, and retain the trailing uncovered key range.
	tables[6] = []pebble.SSTableInfo{table("a", "z", 200)}
	spans = planManualCompaction(tables, []byte("a"), []byte("zz"), 100)
	if len(spans) != 1 || spans[0].bytes != 245 || string(spans[0].end) != "zz" {
		t.Fatalf("indivisible: %+v", spans)
	}
}

func TestManualCompactionAdmissionJoinsOnlyInflight(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(fmt.Sprint(fail), func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			entered, release := make(chan struct{}, 2), make(chan struct{})
			var once sync.Once
			unblock := func() { once.Do(func() { close(release) }) }
			defer unblock()
			var admissions, policies atomic.Int64
			failure := errors.New("compact failure")
			done := make(chan error, 1)
			go func() {
				done <- runManualCompaction(ctx, make([]manualCompactionSpan, 40), 2, func() error { policies.Add(1); return nil }, func(manualCompactionSpan) error {
					admissions.Add(1)
					entered <- struct{}{}
					<-release
					if fail {
						return failure
					}
					return nil
				})
			}()
			for range 2 {
				select {
				case <-entered:
				case <-time.After(time.Minute):
					t.Fatal("no parallel admission")
				}
			}
			if !fail {
				cancel()
			}
			select {
			case err := <-done:
				t.Fatal("did not join entered calls", err)
			default:
			}
			unblock()
			err := <-done
			if fail && !errors.Is(err, failure) || !fail && !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
			if admissions.Load() != 2 || policies.Load() != 2 {
				t.Fatal("admitted after cancel/error", admissions.Load(), policies.Load())
			}
		})
	}
}

func TestManualCompactionPolicyBeforeEverySpan(t *testing.T) {
	var policies, calls int
	err := runManualCompaction(context.Background(), make([]manualCompactionSpan, 40), 2, func() error { policies++; return nil }, func(manualCompactionSpan) error { return nil })
	if err != nil || policies != 40 {
		t.Fatal(err, policies)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	err = runManualCompaction(ctx, make([]manualCompactionSpan, 40), 2, func() error { cancel(); return nil }, func(manualCompactionSpan) error { calls++; return nil })
	if !errors.Is(err, context.Canceled) || calls != 0 {
		t.Fatal("policy cancellation admitted work", err, calls)
	}
}

func TestManualCompactionWorkerPanicJoins(t *testing.T) {
	var joined atomic.Int64
	func() {
		defer func() {
			if recover() != "injected" {
				t.Error("worker panic was not returned to maintenance")
			}
		}()
		_ = runManualCompaction(context.Background(), make([]manualCompactionSpan, 4), 2, nil, func(manualCompactionSpan) error { defer joined.Add(1); panic("injected") })
	}()
	if joined.Load() == 0 || joined.Load() > 2 {
		t.Fatal("invalid joined workers", joined.Load())
	}
}

func TestManualCompactionSlotsSharedAcrossCallers(t *testing.T) {
	s := &Store{}
	s.compactionWorkers.Store(4)
	entered, release := make(chan struct{}, 2), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	var calls atomic.Int64
	failure := errors.New("injected compact error")
	held := func() error { calls.Add(1); entered <- struct{}{}; <-release; return failure }
	done := make(chan error, 2)
	for range 2 {
		go func() { done <- s.withManualCompactionSlot(context.Background(), held) }()
	}
	for range 2 {
		select {
		case <-entered:
		case <-time.After(time.Minute):
			t.Fatal("no shared admission")
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := s.withManualCompactionSlot(ctx, held); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if calls.Load() != 2 {
		t.Fatal("more than two Compact calls in flight")
	}
	unblock()
	for range 2 {
		if err := <-done; !errors.Is(err, failure) {
			t.Fatal(err)
		}
	}
	if err := s.withManualCompactionSlot(context.Background(), func() error { return nil }); err != nil {
		t.Fatal("slot leaked", err)
	}
}
