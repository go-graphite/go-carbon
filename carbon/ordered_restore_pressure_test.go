//go:build unix

package carbon

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/cache"
	"github.com/go-graphite/go-carbon/internal/inputlog"
	"github.com/go-graphite/go-carbon/internal/recovery"
	"github.com/go-graphite/go-carbon/points"
)

// Same compact dump and real journal on sb4 and sb4b; its long metric records
// reproduce the admission concentration seen on the affected host. Opt-in
// because the one-second-timer oracle deliberately takes tens of seconds.
func TestMeasureOrderedRestorePressure(t *testing.T) {
	if os.Getenv("REPLAY_FIX_RESTORE") != "1" {
		t.Skip("set REPLAY_FIX_RESTORE=1")
	}
	count := 2000000
	if n, e := strconv.Atoi(os.Getenv("REPLAY_FIX_RESTORE_POINTS")); e == nil && n > 0 {
		count = n
	}
	root := t.TempDir()
	path := filepath.Join(root, "cache-compact.1.2.bin")
	const metric = "restore.concentrated.production.metric"
	data := make([]points.Point, count)
	want := sha256.New()
	var scratch [16]byte
	for i := range data {
		bits := uint64(i) * 0x9e3779b97f4a7c15
		if i%97 == 0 {
			bits = 0x7ff8000000000042
		}
		data[i] = points.Point{Value: math.Float64frombits(bits), Timestamp: int64(1700000000 + i/3 - i%5)}
		binary.LittleEndian.PutUint64(scratch[:8], uint64(data[i].Timestamp))
		binary.LittleEndian.PutUint64(scratch[8:], bits)
		want.Write(scratch[:])
	}
	raw := recovery.AppendCompactPoints(nil, data)
	raw = recovery.AppendCompactRecord(nil, metric, raw, len(data))
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	c := cache.New()
	c.SetMaxSize(0)
	c.EnableInputLog()
	l, err := inputlog.Open(filepath.Join(root, "journal"), inputlog.Options{SyncInterval: time.Second, SegmentSize: 16 << 20})
	if err != nil {
		t.Fatal(err)
	}
	l.Start(c, cache.ShardCount)
	defer c.DisableInputLog()
	defer l.Kill()
	app := New("")
	started := time.Now()
	if err := app.RestoreFromFile(path, c.AddRestoredBeforeInput); err != nil {
		t.Fatal(err)
	}
	if err := l.Sync(); err != nil {
		t.Fatal(err)
	}
	elapsed := time.Since(started)
	got := sha256.New()
	batch, ok := c.PopNotConfirmed(metric)
	if !ok || len(batch.Data) != count {
		t.Fatal("missing/duplicate restore", c.Size())
	}
	for _, p := range batch.Data {
		binary.LittleEndian.PutUint64(scratch[:8], uint64(p.Timestamp))
		binary.LittleEndian.PutUint64(scratch[8:], math.Float64bits(p.Value))
		got.Write(scratch[:])
	}
	expected := fmt.Sprintf("%x", want.Sum(nil))
	actual := fmt.Sprintf("%x", got.Sum(nil))
	if actual != expected {
		t.Fatal("restore bits/order changed", actual, expected)
	}
	if l.Metrics().Unlogged != 0 {
		t.Fatal("unlogged points", l.Metrics())
	}
	t.Logf("bytes=%d points=%d duration=%v restore_MB_per_s=%.2f points_per_s=%.0f cycles=%d sha256=%s", len(raw), count, elapsed, float64(len(raw))/elapsed.Seconds()/1e6, float64(count)/elapsed.Seconds(), l.Metrics().Cycles, actual)
}
