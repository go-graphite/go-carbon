package whisperio

import (
	"context"
	"fmt"
	"math/rand/v2"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

func floatSweep(t *testing.T, db *store.Store, mode string, enabled bool) {
	t.Helper()
	if err := db.ConfigureColdPack(store.ColdPackOptions{Mode: mode, FloatCodec: enabled, WriteWindow: time.Second, CPUFraction: 1, BytesPerSecond: 1 << 40}); err != nil {
		t.Fatal(err)
	}
	finishFloatSweep(t, db)
}

// A completed sweep may defer a publication at the 2ms validation guard.
// Resume its checkpoint first, then retry a dirty sweep without changing policy.
func finishFloatSweep(t *testing.T, db *store.Store) {
	t.Helper()
	for attempt := 0; attempt < 3; attempt++ {
		before := db.ColdPackStats()
		if err := db.RunColdPack(context.Background()); err != nil {
			t.Fatal(err)
		}
		after := db.ColdPackStats()
		if after.Errors == before.Errors && after.Conflicts == before.Conflicts && after.Skipped == before.Skipped {
			return
		}
	}
	t.Fatalf("no clean float sweep: %+v", db.ColdPackStats())
}
func floatGroups(t *testing.T, db *store.Store, name string) (old, new int) {
	t.Helper()
	records, err := db.ExportTransfer(context.Background(), []string{name})
	if err != nil {
		t.Fatal(err)
	}
	for _, kv := range records[0].Keys {
		if len(kv.Key) == 25 && kv.Key[14] == 'c' && len(kv.Value) >= 16 {
			switch kv.Value[0] {
			case 3:
				old++
			case 4:
				new++
			}
		}
	}
	return
}

func TestColdPackFloatLifecycle(t *testing.T) {
	ctx := context.Background()
	options := coldOptions(&ContinuationCodec{})
	dir := t.TempDir()
	db, err := store.Open(dir, options)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if db != nil {
			db.Close()
		}
	}()
	db.StopBackground()
	cfg := store.MetricConfig{Name: "float.metric", Retentions: []store.Retention{{Step: 60, Count: 10080}}, AggregationMethod: store.Average}
	if _, err := db.Create(ctx, cfg); err != nil {
		t.Fatal(err)
	}
	r := rand.New(rand.NewPCG(42, 17))
	points := make([]store.Point, 1800)
	for i := range points {
		points[i] = store.Point{Timestamp: coldBase + int64(i*60), Value: float64(r.IntN(16)) / 3}
	}
	if err := db.UpdateMany(ctx, cfg.Name, points); err != nil {
		t.Fatal(err)
	}
	want, err := db.Snapshot(ctx, cfg.Name)
	if err != nil {
		t.Fatal(err)
	}
	floatSweep(t, db, "exact", false)
	old, new := floatGroups(t, db, cfg.Name)
	if old == 0 || new != 0 || db.TransferFormat() != 9 {
		t.Fatalf("legacy old/new=%d/%d format=%d", old, new, db.TransferFormat())
	}
	if err := db.ConfigureColdPack(store.ColdPackOptions{Mode: "exact", FloatCodec: true}); err != nil {
		t.Fatal(err)
	}
	if db.TransferFormat() != 9 {
		t.Fatal("flag fenced an unchanged store")
	}
	if err := db.DowngradeFloatCodec(ctx); err == nil {
		t.Fatal("incomplete downgrade accepted")
	}
	floatSweep(t, db, "float-repack", true)
	_, new = floatGroups(t, db, cfg.Name)
	if new == 0 || db.TransferFormat() != 11 {
		t.Fatalf("new=%d format=%d", new, db.TransferFormat())
	}
	got, err := db.Snapshot(ctx, cfg.Name)
	if err != nil {
		t.Fatal(err)
	}
	coldEqual(t, want, got, false)
	// A reader-only restart retains the fence and decodes the new groups.
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db = nil
	for _, tool := range filepath.SplitList(os.Getenv("FLOAT_CODEC_OLD_TOOLS")) {
		ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		output, err := exec.CommandContext(ctx, tool, "-store", dir, "-mode", "unpack").CombinedOutput()
		cancel()
		if err == nil || !strings.Contains(string(output), store.ErrFormat.Error()) {
			t.Fatalf("old reader %s did not reject the marker: %v %s", tool, err, output)
		}
	}
	db, err = store.Open(dir, options)
	if err != nil {
		t.Fatal(err)
	}
	db.StopBackground()
	if db.TransferFormat() != 11 {
		t.Fatal("lost fence")
	}
	got, err = db.Snapshot(ctx, cfg.Name)
	if err != nil {
		t.Fatal(err)
	}
	coldEqual(t, want, got, false)
	// Transfer capability is separate from the durable destination marker.
	records, err := db.ExportTransfer(ctx, []string{cfg.Name})
	if err != nil {
		t.Fatal(err)
	}
	target, err := store.Open(filepath.Join(t.TempDir(), "target"), options)
	if err != nil {
		t.Fatal(err)
	}
	defer target.Close()
	target.StopBackground()
	floatSweep(t, target, "exact", false)
	if _, err := target.ImportTransfer(ctx, records); err == nil {
		t.Fatal("v9 accepted v4 groups")
	}
	if err := target.ConfigureColdPack(store.ColdPackOptions{Mode: "exact", FloatCodec: true}); err != nil {
		t.Fatal(err)
	}
	if target.TransferFormat() != 9 || target.AcceptTransferFormat() != 12 {
		t.Fatal("capability upgraded marker")
	}
	if _, err := target.ImportTransfer(ctx, records); err != nil {
		t.Fatal(err)
	}
	if target.TransferFormat() != 11 {
		t.Fatal("transfer did not fence")
	}
	got, err = target.Snapshot(ctx, cfg.Name)
	if err != nil {
		t.Fatal(err)
	}
	coldEqual(t, want, got, false)
	// Late corrections and append both pass through the real thaw path.
	updates := []store.Point{{Timestamp: points[777].Timestamp, Value: 7.123456789}, {Timestamp: points[len(points)-1].Timestamp + 60, Value: 9.25}}
	if err := db.UpdateMany(ctx, cfg.Name, updates); err != nil {
		t.Fatal(err)
	}
	if err := target.UpdateMany(ctx, cfg.Name, updates); err != nil {
		t.Fatal(err)
	}
	want, err = db.Snapshot(ctx, cfg.Name)
	if err != nil {
		t.Fatal(err)
	}
	floatSweep(t, db, "exact", true)
	got, err = db.Snapshot(ctx, cfg.Name)
	if err != nil {
		t.Fatal(err)
	}
	coldEqual(t, want, got, false)
	floatSweep(t, db, "float-downgrade", false)
	if db.TransferFormat() != 11 {
		t.Fatal("sweep lowered marker without admin finalization")
	}
	_, new = floatGroups(t, db, cfg.Name)
	if new != 0 {
		t.Fatal("new groups remain")
	}
	if err := db.DowngradeFloatCodec(ctx); err != nil {
		t.Fatal(err)
	}
	if db.TransferFormat() != 9 {
		t.Fatal("did not lower v11 fence")
	}
	got, err = db.Snapshot(ctx, cfg.Name)
	if err != nil {
		t.Fatal(err)
	}
	coldEqual(t, want, got, false)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db = nil
	db, err = store.Open(dir, options)
	if err != nil {
		t.Fatal(err)
	}
	db.StopBackground()
	if db.TransferFormat() != 9 {
		t.Fatal("downgrade marker not durable")
	}
}

func TestColdPackFloatWriterGate(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		db, err := store.Open(t.TempDir(), coldOptions(&ContinuationCodec{}))
		if err != nil {
			t.Fatal(err)
		}
		db.StopBackground()
		cfg := store.MetricConfig{Name: "writer.metric", Retentions: []store.Retention{{Step: 60, Count: 10080}}, AggregationMethod: store.Average}
		if _, err := db.Create(context.Background(), cfg); err != nil {
			t.Fatal(err)
		}
		r := rand.New(rand.NewPCG(1, 2))
		p := make([]store.Point, 1800)
		for i := range p {
			p[i] = store.Point{Timestamp: coldBase + int64(i*60), Value: float64(r.IntN(16)) / 3}
		}
		if err := db.UpdateMany(context.Background(), cfg.Name, p); err != nil {
			t.Fatal(err)
		}
		floatSweep(t, db, "exact", enabled)
		old, new := floatGroups(t, db, cfg.Name)
		if enabled && new == 0 || !enabled && (new != 0 || old == 0) {
			t.Fatalf("enabled=%t old/new=%d/%d", enabled, old, new)
		}
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestColdPackFloatIntegerDoesNotFence(t *testing.T) {
	db, err := store.Open(t.TempDir(), coldOptions(&ContinuationCodec{}))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.StopBackground()
	ctx := context.Background()
	cfg := store.MetricConfig{Name: "integer.metric", Retentions: []store.Retention{{Step: 60, Count: 10080}}, AggregationMethod: store.Average}
	if _, err := db.Create(ctx, cfg); err != nil {
		t.Fatal(err)
	}
	p := make([]store.Point, 1800)
	for i := range p {
		p[i] = store.Point{Timestamp: coldBase + int64(i*60), Value: float64(i * 17)}
	}
	if err := db.UpdateMany(ctx, cfg.Name, p); err != nil {
		t.Fatal(err)
	}
	floatSweep(t, db, "exact", true)
	old, new := floatGroups(t, db, cfg.Name)
	if old == 0 || new != 0 || db.TransferFormat() != 9 {
		t.Fatalf("integer fallback old/new=%d/%d format=%d", old, new, db.TransferFormat())
	}
}

func TestColdPackFloatCheckpointResume(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	options := coldOptions(&ContinuationCodec{})
	db, err := store.Open(dir, options)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if db != nil {
			db.Close()
		}
	}()
	db.StopBackground()
	for metric := 0; metric < 4; metric++ {
		name := fmt.Sprintf("resume.%d", metric)
		cfg := store.MetricConfig{Name: name, Retentions: []store.Retention{{Step: 60, Count: 10080}}, AggregationMethod: store.Average}
		if _, err := db.Create(ctx, cfg); err != nil {
			t.Fatal(err)
		}
		r := rand.New(rand.NewPCG(uint64(metric), 17))
		p := make([]store.Point, 1800)
		for i := range p {
			p[i] = store.Point{Timestamp: coldBase + int64(i*60), Value: float64(r.IntN(16)) / 3}
		}
		if err := db.UpdateMany(ctx, name, p); err != nil {
			t.Fatal(err)
		}
	}
	floatSweep(t, db, "exact", false)
	for _, mode := range []string{"float-repack", "float-downgrade"} {
		o := store.ColdPackOptions{Mode: mode, FloatCodec: mode == "float-repack", WriteWindow: time.Second, CPUFraction: 1, BytesPerSecond: 1 << 40}
		if err := db.ConfigureColdPack(o); err != nil {
			t.Fatal(err)
		}
		if done, err := db.ColdPackStep(ctx); err != nil || done {
			t.Fatalf("first step done=%t err=%v", done, err)
		}
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		db = nil
		options.ColdPack = o
		db, err = store.Open(dir, options)
		if err != nil {
			t.Fatal(err)
		}
		db.StopBackground()
		if db.ColdPackStats().Resumed == 0 {
			t.Fatal("cursor not resumed")
		}
		finishFloatSweep(t, db)
		for metric := 0; metric < 4; metric++ {
			_, new := floatGroups(t, db, fmt.Sprintf("resume.%d", metric))
			if mode == "float-repack" && new == 0 || mode == "float-downgrade" && new != 0 {
				t.Fatalf("%s metric=%d new=%d", mode, metric, new)
			}
		}
	}
	if err := db.DowngradeFloatCodec(ctx); err != nil {
		t.Fatal(err)
	}
}
