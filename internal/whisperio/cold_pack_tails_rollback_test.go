package whisperio

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

func TestColdPackTailsRollback(t *testing.T) {
	bin := os.Getenv("COLD_TAILS_RELEASE_BINARY")
	if bin == "" {
		t.Skip("set exact release test binary")
	}
	ctx := context.Background()
	now := time.Unix(1793700000, 0)
	cfg := store.MetricConfig{Name: "tails.rollback", Retentions: []store.Retention{{Step: 60, Count: 43200}, {Step: 1800, Count: 35040}}, AggregationMethod: store.Average, XFilesFactor: .5}
	codec := &tailSeedCodec{ContinuationCodec: ContinuationCodec{DirectLateWrites: true, ReuseBlockKeys: true}, seeds: map[string]mapContinuationIO{}}
	codec.seeds[cfg.Name] = tailSeed(t, codec, cfg, now, 48*time.Hour, 5000, false, 0)
	opts := coldOptions(codec)
	opts.TimeBuckets = false
	opts.Now = func() time.Time { return now }
	opts.RelayoutMinFreeBytes = 1
	path := t.TempDir()
	db := tailOpen(t, path, opts)
	if _, err := db.Create(ctx, cfg); err != nil {
		t.Fatal(err)
	}
	db = tailRelayout(t, db, path, &opts)
	before, err := db.Snapshot(ctx, cfg.Name)
	if err != nil {
		t.Fatal(err)
	}
	tailSweep(t, db, true, "exact")
	control := tailOpen(t, t.TempDir(), opts)
	defer control.Close()
	if _, err := control.Create(ctx, cfg); err != nil {
		t.Fatal(err)
	}
	input := []store.Point{{Timestamp: now.Add(-49 * time.Hour).Unix(), Value: 777}, {Timestamp: now.Unix() - 120, Value: 111}, {Timestamp: now.Unix() - 60, Value: 222}}
	if err := control.UpdateMany(ctx, cfg.Name, input); err != nil {
		t.Fatal(err)
	}
	after, err := control.Snapshot(ctx, cfg.Name)
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(tailReleasePlan{Now: now.Unix(), Before: before, After: after, Input: input})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(path, "rollback.json"), data, 0600); err != nil {
		t.Fatal(err)
	}
	cmdCtx, cancel := context.WithTimeout(ctx, 90*time.Second)
	defer cancel()
	cmd := exec.CommandContext(cmdCtx, bin, "-test.run=^TestColdPackTailsReleaseHelper$", "-test.v", "-test.timeout=80s")
	cmd.Env = append(os.Environ(), "COLD_TAILS_RELEASE_STORE="+path)
	out, err := cmd.CombinedOutput()
	t.Log(string(out))
	if err != nil {
		t.Fatal(err)
	}
	db = tailOpen(t, path, opts)
	defer db.Close()
	got, err := db.Snapshot(ctx, cfg.Name)
	if err != nil {
		t.Fatal(err)
	}
	coldEqual(t, after, got, false)
	got, err = db.Snapshot(ctx, "release.new.metric")
	if err != nil || len(got.Archives[0].Points) != 1 || got.Archives[0].Points[0].Value != 42 {
		t.Fatal("new reader lost release write", err)
	}
}
