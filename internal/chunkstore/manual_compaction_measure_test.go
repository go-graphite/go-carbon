package chunkstore

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
)

// A fixed 1 MiB/s output cap stands in for a slow device, rather than the
// operational compaction budget (which shutdown disables). Same physical LSM
// fixture and retained values for both calls. No production IO is involved.
func TestManualCompactionStopMeasurement(t *testing.T) {
	mode := os.Getenv("ZERO_COMPACT_MEASURE")
	if mode == "" {
		t.Skip("set ZERO_COMPACT_MEASURE=wide|split")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var armed atomic.Bool
	var cancelAt atomic.Int64
	fs := newCompactionPacingFS(vfs.Default, 0)
	opts := (&pebble.Options{FS: fs, DisableAutomaticCompactions: true, MemTableSize: 4 << 20, MaxConcurrentCompactions: func() int { return 4 }, EventListener: &pebble.EventListener{TableCreated: fs.tableCreated, CompactionBegin: func(pebble.CompactionInfo) {
		if armed.CompareAndSwap(true, false) {
			cancelAt.Store(time.Now().UnixNano())
			cancel()
		}
	}}}).EnsureDefaults()
	opts.Levels = make([]pebble.LevelOptions, 7)
	for i := range opts.Levels {
		opts.Levels[i].TargetFileSize = 128 << 10
		opts.Levels[i].Compression = pebble.NoCompression
	}
	dir := t.TempDir()
	db, err := pebble.Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	const rows = 8192
	key := func(i int) []byte { return []byte(fmt.Sprintf("u%08d", i)) }
	value := make([]byte, 4096)
	for i := range value {
		value[i] = byte(i % 251)
	}
	for i := range rows {
		if err := db.Set(key(i), value, pebble.NoSync); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := db.Compact([]byte("u"), []byte("v"), true); err != nil {
		t.Fatal(err)
	}
	// Put deletion markers into separate flush files matching the old files.
	// ZERO_COMPACT_BROAD=1 deliberately tests a single broad L0 table too.
	for i := 0; i < rows; i += 2 {
		if err := db.Delete(key(i), pebble.NoSync); err != nil {
			t.Fatal(err)
		}
		if os.Getenv("ZERO_COMPACT_BROAD") != "1" && i%64 == 62 {
			if err := db.Flush(); err != nil {
				t.Fatal(err)
			}
		}
	}
	journal := binary.BigEndian.AppendUint64(append([]byte(nil), relayoutReclaimPrefix...), 1)
	spans := map[byte]*relayoutSpan{'u': {start: key(0), end: key(rows - 1)}}
	if err := db.Set(journal, encodeRelayoutReclaim(rows*4096, spans), pebble.Sync); err != nil {
		t.Fatal(err)
	}
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}
	s := &Store{db: db, fs: fs, dir: dir, writeOptions: pebble.Sync, pebbleOptions: opts}
	s.compactionWorkers.Store(4)
	_ = s.SetRelayoutLimits(1, ^uint64(0))
	fs.pacer.setRate(1 << 20)
	armed.Store(true)
	before := fs.pacer.bytes.Load()
	at := time.Now()
	if mode == "wide" {
		err = db.Compact([]byte("u"), []byte("v"), true)
	} else {
		err = s.reclaimRelayout(ctx, true, 0)
	}
	elapsed := time.Since(at)
	fs.pacer.setRate(0)
	if mode != "wide" && !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if mode == "wide" && err != nil {
		t.Fatal(err)
	}
	if cancelAt.Load() == 0 {
		t.Fatal("did not cancel in-flight compaction")
	}
	stop := time.Since(time.Unix(0, cancelAt.Load()))
	t.Logf("COMPACT_STOP mode=%s broad=%s wall_s=%.6f cancel_to_return_s=%.6f output_bytes=%d relay_pause_drop_model=%d", mode, os.Getenv("ZERO_COMPACT_BROAD"), elapsed.Seconds(), stop.Seconds(), fs.pacer.bytes.Load()-before, max(0, int64(stop.Seconds()*160000)-800000))
	if mode != "wide" && stop >= 10*time.Second {
		t.Errorf("bounded spans did not meet stop target: %s", stop)
	}
	// A cancelled reclaim must leave the durable obligation, even when its
	// first stock Compact happened to finish the physical cleanup.
	if mode != "wide" {
		keys, _, _, err := s.readRelayoutReclaims(context.Background())
		if err != nil || len(keys) != 1 {
			t.Fatal("journal retired on cancellation", err)
		}
	}
}
