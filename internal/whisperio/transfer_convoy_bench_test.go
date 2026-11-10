package whisperio

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"runtime"
	"runtime/pprof"
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
	limit := 3 * time.Minute
	if v := os.Getenv("GO_CARBON_CONVOY_LIMIT"); v != "" {
		var e error
		limit, e = time.ParseDuration(v)
		if e != nil {
			t.Fatal(e)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), limit)
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
	hotspot := os.Getenv("GO_CARBON_CONVOY_HOTSPOT") == "1"
	var collisionNames [4096][]string
	if hotspot {
		need := batch * imports
		got := 0
		for i := 0; got < need; i++ {
			n := fmt.Sprintf("convoy.copy.hot.%09d", i)
			stripe := (helper.HashString(n) >> 32) % 4096
			if stripe < 95 || stripe == 4095 {
				collisionNames[stripe] = append(collisionNames[stripe], n)
				got++
			}
		}
		// Top up each stripe to avoid distribution noise in name generation.
		required := (batch*imports+94)/95 + 16
		for stripe := range collisionNames {
			if stripe >= 95 && stripe != 4095 {
				continue
			}
			limit := required
			if stripe == 4095 {
				limit = (batch*imports + 15) / 16
			}
			for i := 0; len(collisionNames[stripe]) < limit; i++ {
				n := fmt.Sprintf("convoy.copy.extra.%d.%d", stripe, i)
				if (helper.HashString(n)>>32)%4096 == uint64(stripe) {
					collisionNames[stripe] = append(collisionNames[stripe], n)
				}
			}
		}
	}
	var collisionCursor [4096]int
	groups := make([][]store.TransferMetric, imports)
	stripeCounts := map[int]int{}
	for w := range imports {
		groups[w] = make([]store.TransferMetric, batch)
		for i := range batch {
			r := template
			r.Config.Name = fmt.Sprintf("convoy.copy.partition.%02d.churn.%05d", w, i)
			if hotspot {
				j := w*batch + i
				stripe := ((j/16)*15 + j%16) % 95
				if j%16 == 15 {
					stripe = 4095
				}
				r.Config.Name = collisionNames[stripe][collisionCursor[stripe]]
				collisionCursor[stripe]++
			}
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
	t.Log("prepopulated all import targets")
	var names [workers]string
	for w := range workers {
		desired := (helper.HashString(groups[w%imports][((w/imports)*16)%batch].Config.Name) >> 32) % 4096
		if hotspot {
			desired = uint64(w)
			if w == workers-1 {
				desired = 4095
			}
		}
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
					if hotspot && w == workers-1 {
						data = history
					}
					e := target.UpdateManyMaintained(c, names[w], data, store.ContinuationMaintenance{})
					timer.Finish()
					if e != nil {
						errs <- e
						return
					}
					count.Add(1)
					turn++
					if hotspot && w == workers-1 {
						time.Sleep(2 * time.Millisecond)
					}
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
	beforeID, e := target.CatalogHighWatermark(ctx)
	if e != nil {
		t.Fatal(e)
	}
	at := time.Now()
	var cpu *os.File
	if prefix := os.Getenv("GO_CARBON_CONVOY_PROFILE"); prefix != "" {
		cpu, err = os.Create(prefix + ".cpu")
		if err != nil {
			t.Fatal(err)
		}
		runtime.SetMutexProfileFraction(10000)
		if err = pprof.StartCPUProfile(cpu); err != nil {
			t.Fatal(err)
		}
	}
	latencies := make([]float64, imports)
	var copies sync.WaitGroup
	var completed atomic.Uint64
	var completedBytes atomic.Uint64
	for w := range imports {
		copies.Go(func() {
			began := time.Now()
			_, e := target.ImportTransfer(ctx, groups[w])
			latencies[w] = time.Since(began).Seconds()
			if e == nil {
				completed.Add(1)
				completedBytes.Add(uint64(store.TransferWireSize2(groups[w])))
			}
			if e != nil {
				errs <- e
			}
		})
	}
	progressDone := make(chan struct{})
	var progress sync.WaitGroup
	progress.Go(func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-progressDone:
				return
			case <-ticker.C:
				t.Logf("CONVOY_PROGRESS seconds=%.1f updates=%d apply=%+v", time.Since(at).Seconds(), count.Load()-baseCount, target.TransferApplyStats())
			}
		}
	})
	copies.Wait()
	close(progressDone)
	progress.Wait()
	if cpu != nil {
		pprof.StopCPUProfile()
		cpu.Close()
		f, e := os.Create(os.Getenv("GO_CARBON_CONVOY_PROFILE") + ".mutex")
		if e != nil {
			t.Fatal(e)
		}
		pprof.Lookup("mutex").WriteTo(f, 0)
		f.Close()
	}
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
	var publishedNames, publishedBytes int64
	for _, group := range groups {
		for _, r := range group {
			m, e := target.Metadata(context.Background(), r.Config.Name)
			if e != nil {
				t.Fatal(e)
			}
			if m.ID > beforeID {
				publishedNames++
				publishedBytes += store.TransferWireSize2([]store.TransferMetric{r}) - 44
			}
		}
	}
	output := map[string]any{"hotspot": hotspot, "fixture": "sparse 6000-point histories, 18000 minutes, two archives, time buckets, late corrections", "history_keys_per_name": len(template.Keys), "wire_bytes_per_name": wire, "batch_names": batch, "importers": imports, "persister_workers": workers, "distinct_stripes_per_hold_histogram": stripeCounts, "seconds": seconds, "persister_updates_per_second": float64(updates) / seconds, "persister_wait_seconds_per_second": wait / seconds, "import_MB_per_second": float64(wire) * float64(batch*imports) / 1e6 / seconds, "import_latency_p50_seconds": latencies[imports/2], "import_latency_max_seconds": latencies[imports-1], "baseline_updates_per_second": float64(baselineUpdates) / baselineSeconds, "baseline_wait_seconds_per_second": baselineWait / baselineSeconds, "apply_before": statsBefore, "apply_after": statsAfter}
	var exactWire int64
	for _, group := range groups {
		exactWire += store.TransferWireSize2(group)
	}
	output["published_names"] = publishedNames
	output["published_wire_record_bytes"] = publishedBytes
	output["published_MB_per_second"] = float64(publishedBytes) / 1e6 / seconds
	output["completed_imports"] = completed.Load()
	output["completed_wire_bytes"] = completedBytes.Load()
	output["completed_import_MB_per_second"] = float64(completedBytes.Load()) / 1e6 / seconds
	output["encoded_wire_bytes"] = exactWire
	output["encoded_import_MB_per_second"] = float64(exactWire) / 1e6 / seconds
	data, _ := json.Marshal(output)
	t.Logf("CONVOY_JSON %s", data)
}
