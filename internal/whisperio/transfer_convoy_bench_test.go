package whisperio

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/vfs"
	"github.com/go-graphite/go-carbon/helper"
	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/stagetime"
)

// Opt-in real storage benchmark. Each importer gets a disjoint set of names;
// foreground writers share their stripes but never their revisions. Histories
// span many time partitions beyond the hot window, with sparse churn and late
// corrections. Run each process alone and retain the JSON line as a receipt.
func TestTransferConvoyRealShape(t *testing.T) {
	if os.Getenv("GO_CARBON_CONVOY") != "1" {
		t.Skip("opt-in sustained storage benchmark")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 9*time.Minute)
	defer cancel()
	now := time.Unix(1794000000, 0)
	clock := func() time.Time { return now }
	opts := coldOptions(&ContinuationCodec{DirectLateWrites: true, ReuseBlockKeys: true})
	opts.Now = clock
	opts.CacheSize = 64 << 20
	opts.MemTableSize = 16 << 20
	opts.MetadataCacheSize = 16 << 20
	opts.SyncInterval = time.Hour
	source, err := store.Open("/source", opts.WithFS(vfs.NewMem()))
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	source.StopBackground()
	cfg := store.MetricConfig{Name: "convoy.seed", Retentions: []store.Retention{{Step: 60, Count: 20160}, {Step: 3600, Count: 8760}}, AggregationMethod: store.Last}
	if _, err = source.Create(ctx, cfg); err != nil {
		t.Fatal(err)
	}
	history := make([]store.Point, 6000)
	for i := range history {
		history[i] = store.Point{Timestamp: now.Unix() - int64(18000-i*3)*60, Value: float64(i%73) + float64(i)*.0000001}
	}
	if err = source.UpdateMany(ctx, cfg.Name, history); err != nil {
		t.Fatal(err)
	}
	exported, err := source.ExportTransfer(ctx, []string{cfg.Name})
	if err != nil {
		t.Fatal(err)
	}
	template := exported[0]
	var wire int64
	for _, kv := range template.Keys {
		wire += int64(len(kv.Key) + len(kv.Value) + 8)
	}
	// The driver defaults to 8192 names and a 64MiB estimated byte cap.
	batch := min(8192, int((64<<20)/(wire+256)))
	if v := os.Getenv("GO_CARBON_CONVOY_BATCH"); v != "" {
		if _, err = fmt.Sscan(v, &batch); err != nil {
			t.Fatal(err)
		}
	}
	if batch < 16 {
		t.Fatal("fixture exceeds batch budget")
	}
	target, err := store.Open(t.TempDir(), opts)
	if err != nil {
		t.Fatal(err)
	}
	defer target.Close()
	target.StopBackground()
	const workers = 96
	const imports = 16
	groups := make([][]store.TransferMetric, imports)
	stripeCounts := map[int]int{}
	for w := range imports {
		groups[w] = make([]store.TransferMetric, batch)
		for i := range batch {
			r := template
			r.Config.Name = fmt.Sprintf("convoy.copy.partition.%02d.churn.%05d", w, i)
			r.Version.Name = r.Config.Name
			groups[w][i] = r
		}
		for start := 0; start < batch; start += 16 {
			set := map[uint64]bool{}
			for _, r := range groups[w][start:min(start+16, batch)] {
				set[(helper.HashString(r.Config.Name)>>32)%4096] = true
			}
			stripeCounts[len(set)]++
		}
	}
	t.Logf("fixture batch=%d keys/name=%d bytes/name=%d", batch, len(template.Keys), wire)
	// Prepopulate with the same real history so measured imports take merge paths.
	for _, group := range groups {
		if _, err = target.ImportTransfer(ctx, group); err != nil {
			t.Fatal(err)
		}
	}
	var names [workers]string
	for w := range workers {
		desired := (helper.HashString(groups[w%imports][((w/imports)*16)%batch].Config.Name) >> 32) % 4096
		for i := 0; ; i++ {
			n := fmt.Sprintf("convoy.foreground.%03d.%d", w, i)
			if (helper.HashString(n)>>32)%4096 == desired {
				names[w] = n
				break
			}
		}
		cfg.Name = names[w]
		if _, err = target.Create(ctx, cfg); err != nil {
			t.Fatal(err)
		}
		if err = target.UpdateMany(ctx, cfg.Name, history[len(history)-256:]); err != nil {
			t.Fatal(err)
		}
	}
	var stop atomic.Bool
	var count atomic.Uint64
	var wg sync.WaitGroup
	errs := make(chan error, workers+imports)
	startWriters := func() {
		for w := range workers {
			wg.Go(func() {
				var turn int64
				for !stop.Load() {
					c, timer := stagetime.Start(ctx)
					data := []store.Point{{Timestamp: now.Unix() - 60 - (turn%32)*60, Value: float64(turn)}, {Timestamp: now.Unix() - 12000*60 - (turn%128)*60, Value: float64(-turn)}}
					e := target.UpdateManyMaintained(c, names[w], data, store.ContinuationMaintenance{})
					timer.Finish()
					if e != nil {
						errs <- e
						return
					}
					count.Add(1)
					turn++
				}
			})
		}
	}
	startWriters()
	time.Sleep(2 * time.Second)
	before := stagetime.Snapshot()
	baseCount := count.Load()
	baseAt := time.Now()
	time.Sleep(3 * time.Second)
	baselineSeconds := time.Since(baseAt).Seconds()
	baselineUpdates := count.Load() - baseCount
	baselineWait := stagetime.Snapshot()[stagetime.LockWait].Seconds - before[stagetime.LockWait].Seconds
	before = stagetime.Snapshot()
	baseCount = count.Load()
	statsBefore := target.TransferApplyStats()
	at := time.Now()
	latencies := make([]float64, imports)
	var copies sync.WaitGroup
	for w := range imports {
		copies.Go(func() {
			began := time.Now()
			_, e := target.ImportTransfer(ctx, groups[w])
			latencies[w] = time.Since(began).Seconds()
			if e != nil {
				errs <- e
			}
		})
	}
	copies.Wait()
	seconds := time.Since(at).Seconds()
	updates := count.Load() - baseCount
	wait := stagetime.Snapshot()[stagetime.LockWait].Seconds - before[stagetime.LockWait].Seconds
	statsAfter := target.TransferApplyStats()
	stop.Store(true)
	wg.Wait()
	close(errs)
	for e := range errs {
		t.Error(e)
	}
	sort.Float64s(latencies)
	output := map[string]any{"fixture": "sparse 6000-point histories, 18000 minutes, two archives, time buckets, late corrections", "history_keys_per_name": len(template.Keys), "wire_bytes_per_name": wire, "batch_names": batch, "importers": imports, "persister_workers": workers, "distinct_stripes_per_hold_histogram": stripeCounts, "seconds": seconds, "persister_updates_per_second": float64(updates) / seconds, "persister_wait_seconds_per_second": wait / seconds, "import_MB_per_second": float64(wire) * float64(batch*imports) / 1e6 / seconds, "import_latency_p50_seconds": latencies[imports/2], "import_latency_max_seconds": latencies[imports-1], "baseline_updates_per_second": float64(baselineUpdates) / baselineSeconds, "baseline_wait_seconds_per_second": baselineWait / baselineSeconds, "apply_before": statsBefore, "apply_after": statsAfter}
	data, _ := json.Marshal(output)
	t.Logf("CONVOY_JSON %s", data)
}
