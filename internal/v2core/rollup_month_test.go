package v2core

import (
	"context"
	"encoding/binary"
	"math"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
	partsformat "github.com/go-graphite/go-carbon/internal/v2part"
	whisper "github.com/go-graphite/go-whisper"
)

// A 7d coarse window (2026-11-26..12-03) spans the Nov/Dec month partitions:
// its sources, closure watermark and closed record live in different parts.
// Open projection, idle closure, a late correction and restarts stay
// classic-exact through both the full scan and the bounded read.
func TestClosedWindowAcrossMonthPartitions(t *testing.T) {
	for _, method := range []sealrollup.Method{sealrollup.Average, sealrollup.Last} {
		policy, encoded := rollupTestPolicy(t, "test.metric", "1h:90d,7d:2y", method, 0)
		ws := int64(1795651200) // Thursday 2026-11-26 00:00 UTC
		dec := int64(1796083200)
		var clock atomic.Int64
		clock.Store(ws)
		now := func() time.Time { return time.Unix(clock.Load(), 0) }
		path := filepath.Join(t.TempDir(), "classic.wsp")
		classic, err := whisper.CreateWithOptions(path, whisper.MustParseRetentionDefs("1h:90d,7d:2y"), whisper.AggregationMethod(method), 0, &whisper.Options{Sparse: true, Now: now})
		if err != nil {
			t.Fatal(err)
		}
		cfg := coreTestConfig()
		cfg.Shards = 1
		cfg.Producers = 1
		cfg.Rollups = true
		cfg.MaxRows = 1024
		root := t.TempDir()
		core, err := Open(root, cfg, Hooks{Now: now})
		if err != nil {
			t.Fatal(err)
		}
		reopen := func() {
			if err := core.Close(); err != nil {
				t.Fatal(err)
			}
			if core, err = Open(root, cfg, Hooks{Now: now}); err != nil {
				t.Fatal(err)
			}
		}
		order := uint64(0)
		write := func(at int64, rows []Row) {
			clock.Store(at)
			order++
			ptrs := make([]*whisper.TimeSeriesPoint, len(rows))
			for i := range rows {
				rows[i].ID, rows[i].Seq = 7, order*1000+uint64(i)
				ptrs[i] = &whisper.TimeSeriesPoint{Time: int(rows[i].Time), Value: math.Float64frombits(rows[i].Bits)}
			}
			if len(rows) > 0 {
				if err := classic.UpdateMany(ptrs); err != nil {
					t.Fatal(err)
				}
				if err := core.AppendBatches(0, []Batch{{ID: 7, Order: order, Clock: at, Policy: encoded, Points: rows}}, order, false); err != nil {
					t.Fatal(err)
				}
			} else {
				order--
			}
			if err := core.AppendBatches(0, nil, order, true); err != nil {
				t.Fatal(err)
			}
		}
		hourly := func(from, until int64, value func(int64) float64) []Row {
			var rows []Row
			for ts := from; ts < until; ts += 3600 {
				rows = append(rows, Row{Time: ts, Bits: math.Float64bits(value(ts))})
			}
			return rows
		}
		coarse := policy.Config.Archives[1]
		windowSlot := (ws / coarse.Step) % coarse.Count
		check := func(stage string, wantOpen bool, minPartitions int) {
			t.Helper()
			snap := core.DBs()[0].NewSnapshot()
			defer snap.Close()
			partitions := map[int64]bool{}
			it, err := newArchiveIter(context.Background(), snap, nil)
			if err != nil {
				t.Fatal(err)
			}
			for valid := it.First(); valid; valid = it.Next() {
				if k := it.Key(); len(k) >= 4 && string(k[:4]) == "GCP\x03" {
					key, err := partsformat.Decode(k)
					if err != nil {
						t.Fatal(err)
					}
					partitions[key.Partition] = true
				}
			}
			if err := it.Close(); err != nil {
				t.Fatal(err)
			}
			if len(partitions) < minPartitions {
				t.Fatalf("%s: %d month partitions, want >= %d", stage, len(partitions), minPartitions)
			}
			found := false
			if err := ScanRollups(snap, func(p RollupPolicy, m RollupMetadata, records []sealrollup.Record) error {
				found = true
				checkCoreClassicSlots(t, path, p.Config, records, clock.Load())
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			if !found {
				t.Fatalf("%s: missing metric", stage)
			}
			// Bounded read of the spanning slot: stored closed record or projection
			// over source slots read from both month partitions.
			index, err := NewRollupSnapshot(context.Background(), snap, nil, nil, nil, 0)
			if err != nil {
				t.Fatal(err)
			}
			var got []sealrollup.Record
			if err := index.ScanArchive(context.Background(), "test.metric", 1, windowSlot, windowSlot+1, func(r sealrollup.Record) error {
				got = append(got, r)
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			ts, bits := classicSlot(t, path, coarse, 1, windowSlot)
			if len(got) != 1 || got[0].Sample.Time != ts || got[0].Sample.Bits != bits {
				t.Fatalf("%s: bounded read %+v classic (%d,%x)", stage, got, ts, bits)
			}
		}
		// Open window, November sources only.
		write(dec-1800, hourly(ws, dec, func(ts int64) float64 { return float64(ts%7200) / 360 }))
		check("nov-open", true, 1)
		reopen()
		// Still open: projection merges November and December partitions.
		write(dec+86400+1800, hourly(dec, dec+86400, func(ts int64) float64 { return -float64(ts%10800) / 3600 }))
		check("dec-open", true, 2)
		// Idle closure past the window end, then restart.
		clock.Store(ws + coarse.Step + 3600)
		write(ws+coarse.Step+3600, nil)
		check("idle-closed", false, 2)
		reopen()
		check("closed-reopen", false, 2)
		// Next window data, then a late November point recomputes the closed window.
		write(ws+coarse.Step+7200, hourly(ws+coarse.Step+3600, ws+coarse.Step+7200, func(int64) float64 { return 5 }))
		write(ws+coarse.Step+7200, []Row{{Time: dec - 12*3600, Bits: math.Float64bits(1e6)}})
		check("late", false, 2)
		reopen()
		check("late-reopen", false, 2)
		if err := core.Close(); err != nil {
			t.Fatal(err)
		}
		_ = classic.Close()
	}
}

func classicSlot(t *testing.T, path string, a sealrollup.Archive, archive int, slot int64) (int64, uint64) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	off := int(binary.BigEndian.Uint32(data[16+archive*12:]))
	raw := data[off : off+int(a.Count)*12]
	for i := 0; i < len(raw); i += 12 {
		if ts := int64(binary.BigEndian.Uint32(raw[i:])); ts != 0 && (ts/a.Step)%a.Count == slot {
			return ts, binary.BigEndian.Uint64(raw[i+4:])
		}
	}
	return 0, 0
}
