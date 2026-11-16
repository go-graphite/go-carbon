//go:build sealrollup_integration

package main

import (
	"math"
	"os"
	"path/filepath"
	"testing"

	"github.com/go-graphite/go-carbon/internal/partlog"
)

func TestNumericPDC4RawAndRenderedRecovery(t *testing.T) {
	root := t.TempDir()
	t.Setenv("PARTLOG_BENCH_METADATA", "1")
	const total, metrics, producers, owners = 4200, 420, 3, 2
	if err := logBacklogBenchmark(root, "write", total, metrics, producers, owners); err != nil {
		t.Fatal(err)
	}
	logDir := filepath.Join(root, "log")
	cs, err := openReplayCatalogs(logDir, producers, true)
	if err != nil {
		t.Fatal(err)
	}
	j, err := partlog.Open(logDir, partlog.Options{Streams: producers})
	if err != nil {
		t.Fatal(err)
	}
	for p := range producers {
		var rows []row
		for i := range total / producers {
			seq := uint64(total + i*producers + p)
			rows = append(rows, row{ID: seq % metrics, Time: 1800000000 + int64(seq/20000000), Bits: math.Float64bits(float64(seq % 100003)), Seq: seq})
		}
		payload, e := cs[p].encode(rows)
		if e != nil {
			t.Fatal(e)
		}
		if _, e = j.Append(p, 0, payload); e != nil {
			t.Fatal(e)
		}
	}
	if err = j.Close(); err != nil {
		t.Fatal(err)
	}
	if err = closeReplayCatalogs(cs); err != nil {
		t.Fatal(err)
	}
	cfg := coreConfig{AppendParts: true, Shards: owners, Producers: producers, MaxRows: 1 << 20, PartitionSeconds: 3600, RawCodec: "run1"}
	for range 2 { // Second recovery seeks beyond both record formats.
		l, e := openLoggedCore(logDir, filepath.Join(root, "parts"), cfg, partlog.Options{Streams: producers})
		if e != nil {
			t.Fatal(e)
		}
		if e = l.replay(); e != nil {
			t.Fatal(e)
		}
		if e = l.close(); e != nil {
			t.Fatal(e)
		}
	}
	if err = os.WriteFile(filepath.Join(root, "live-counts.json"), []byte("[1400,1400,1400]"), 0600); err != nil {
		t.Fatal(err)
	}
	if err = verifyLogBenchmark(root, total, metrics, producers, owners, func(stage string, points uint64, extra map[string]any) {
		t.Logf("%s points%d %v", stage, points, extra)
	}); err != nil {
		t.Fatal(err)
	}
	verifyCorpusRender(t, root, owners)
}
