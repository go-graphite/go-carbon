package v2core

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2part"
)

type envelopeImage struct {
	Rows     int      // physical metadata rows, each checked against its canonical definition
	Scanned  []string // restart scan (scanRollups)
	Snapshot []string // reader snapshot metadata, lookups and records
	Names    []string // ScanCatalogNames
	Slim     int      // physical rows without a stored definition
}

type envelopeRun struct {
	Stages []string
	Images []envelopeImage
}

// Engines that store the definition and engines that do not, fed the same
// input, decode identical metadata (definition bytes included), records,
// lookups and name listings after seals, merges, restarts and expiry. Every
// physical envelope of both decodes to the canonical definition of its name.
// Physical placement of idle-closure envelopes depends on publication timing,
// so the logical surfaces are compared rather than the physical row sets.
func TestSlimEnvelopeEngineDifferential(t *testing.T) {
	if os.Getenv("GO_CARBON_SLIM_IMAGE") != "" {
		writeSlimEnvelopeImages(t, os.Getenv("GO_CARBON_SLIM_ENCODING") == "full")
		return
	}
	t.Parallel()
	// The writer switch is process-global. Isolate encodings, keeping every
	// fixture and image check while allowing the two independent engines to run
	// together and alongside the rest of the package.
	var runs [2]envelopeRun
	t.Run("encodings", func(t *testing.T) {
		for i, encoding := range []string{"full", "slim"} {
			t.Run(encoding, func(t *testing.T) {
				t.Parallel()
				output := filepath.Join(t.TempDir(), "images.json")
				cmd := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestSlimEnvelopeEngineDifferential$", "-test.count=1", "-test.timeout=10m")
				cmd.Env = append(os.Environ(), "GO_CARBON_SLIM_IMAGE="+output, "GO_CARBON_SLIM_ENCODING="+encoding)
				if log, err := cmd.CombinedOutput(); err != nil {
					t.Fatalf("%s encoding: %v\n%s", encoding, err, log)
				}
				data, err := os.ReadFile(output)
				if err != nil {
					t.Fatal(err)
				}
				if err := json.Unmarshal(data, &runs[i]); err != nil {
					t.Fatal(err)
				}
			})
		}
	})
	stages, fullImages, slimImages := runs[0].Stages, runs[0].Images, runs[1].Images
	if !reflect.DeepEqual(stages, runs[1].Stages) || len(fullImages) != len(stages) || len(slimImages) != len(stages) {
		t.Fatal("encoding stages differ")
	}
	for i, stage := range stages {
		full, slim := fullImages[i], slimImages[i]
		if full.Slim != 0 || slim.Slim == 0 {
			t.Fatalf("%s: slim rows full=%d slim=%d of %d", stage, full.Slim, slim.Slim, slim.Rows)
		}
		full.Slim, slim.Slim, full.Rows, slim.Rows = 0, 0, 0, 0
		if !reflect.DeepEqual(full, slim) {
			for j := range min(len(full.Scanned), len(slim.Scanned)) {
				if full.Scanned[j] != slim.Scanned[j] {
					t.Logf("scanned %d\nfull=%.700s\nslim=%.700s", j, full.Scanned[j], slim.Scanned[j])
					break
				}
			}
			t.Fatalf("%s: scanned equal=%v snapshot equal=%v names equal=%v", stage, reflect.DeepEqual(full.Scanned, slim.Scanned), reflect.DeepEqual(full.Snapshot, slim.Snapshot), reflect.DeepEqual(full.Names, slim.Names))
		}
		if len(full.Scanned) == 0 || len(full.Names) == 0 {
			t.Fatalf("%s: empty image", stage)
		}
	}
	expired := 0
	for _, row := range slimImages[len(slimImages)-1].Scanned {
		if bytes.Contains([]byte(row), []byte("Expired:true")) {
			expired++
		}
	}
	if expired == 0 {
		t.Fatal("expiry did not run")
	}
	last := slimImages[len(slimImages)-1]
	t.Logf("%d stages identical; final: %d metrics scanned, %d expired, %d names", len(stages), len(last.Scanned), expired, len(last.Names))
}

func writeSlimEnvelopeImages(t *testing.T, full bool) {
	t.Setenv("CORE_MERGE_WORKERS", "0")
	t.Cleanup(func() { rollupWriteDefinitions.Store(false) })
	cfg := coreTestConfig()
	cfg.Shards, cfg.Producers, cfg.Rollups = 1, 1, true
	cfg.MaxRows = 1024
	rng := rand.New(rand.NewPCG(7, 11))
	ladders := []string{"10s:3d", "10s:2d,1m:14d,30m:2y", "1m:30d,1h:1y,1d:10y", "1s:1d,1m:30d,1h:1y,1d:10y", "1s:1h,1m:1d,1h:1y"}
	xffs := []float32{0, .1, .5, 1}
	names := []string{"quoted.\"metric\\name", "html.<metric>&x", "unicode.метрика", "ctl.\x01\x7f", "line sep"}
	for len(names) < 48 {
		b := make([]byte, 1+rng.IntN(100))
		for j := range b {
			b[j] = byte(0x21 + rng.IntN(0x5e))
		}
		names = append(names, fmt.Sprintf("%s.%d", b, len(names)))
	}
	type metric struct {
		id     uint64
		name   string
		policy []byte
		step   int64
	}
	var metrics []metric
	canonical := map[string][]byte{}
	for i, name := range names {
		method := sealrollup.Average + sealrollup.Method(i%int(sealrollup.First))
		p, raw := rollupTestPolicy(t, name, ladders[i%len(ladders)], method, xffs[i%len(xffs)])
		metrics = append(metrics, metric{uint64(i + 1), name, raw, p.Config.Archives[0].Step})
		canonical[name] = raw
	}
	var now atomic.Int64 // batch clock
	var engine *Engine
	var root string
	open := func(expire func(RollupPolicy, RollupMetadata, time.Time) bool) {
		if engine != nil {
			if err := engine.Close(); err != nil {
				t.Fatal(err)
			}
		}
		// A fixed hook clock keeps background idle closure from running at
		// timing-dependent points, as in TestRollupHistoryDifferential.
		h := Hooks{Now: func() time.Time { return time.Unix(1, 0) }}
		if expire != nil {
			// Every name is asked at the batch clock, after its last write.
			at := time.Unix(now.Load(), 0)
			h.Expire, h.ExpiryTTL, h.Now = expire, expiryWindow(time.Nanosecond), func() time.Time { return at }
		}
		e, err := Open(root, cfg, h)
		if err != nil {
			t.Fatal(err)
		}
		engine = e
	}
	image := func() envelopeImage {
		orderProofObserve(t, t.Name(), engine)
		var out envelopeImage
		ctx := context.Background()
		pin := engine.DBs()[0].NewSnapshot()
		defer pin.Close()
		it, err := newArchiveIter(ctx, pin, nil)
		if err != nil {
			t.Fatal(err)
		}
		for valid := it.First(); valid; valid = it.Next() {
			key := it.Key()
			if len(key) < 4 || string(key[:4]) != "GCP\x03" {
				continue
			}
			k, err := v2part.Decode(key)
			if err != nil {
				t.Fatal(err)
			}
			if k.Archive != v2part.MetadataArchive {
				continue
			}
			if it.Value()[3] == rollupMetadataSlim {
				out.Slim++
			}
			m, err := DecodeRollupMetadata(k.Metric, k.Group, it.Value())
			if err != nil {
				t.Fatal(err)
			}
			if want := canonical[string(k.Metric)]; !bytes.Equal(m.Policy, want) {
				t.Fatalf("envelope of %q decodes to definition %q, want %q", k.Metric, m.Policy, want)
			}
			out.Rows++
		}
		if err = it.Close(); err != nil {
			t.Fatal(err)
		}
		if err = scanRollups(pin, nil, func(p RollupPolicy, m RollupMetadata, rs []sealrollup.Record) error {
			out.Scanned = append(out.Scanned, fmt.Sprintf("%+v %+v %q %+v", p, m, m.Policy, rs))
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		slices.Sort(out.Scanned)
		index, err := NewRollupSnapshot(ctx, pin, nil, nil, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		index.Metadata(func(name string, p RollupPolicy, m RollupMetadata) {
			out.Snapshot = append(out.Snapshot, fmt.Sprintf("meta %q %+v %+v %q", name, p, m, m.Policy))
		})
		// Readers share one snapshot concurrently, as fetches do. Each name's
		// read is independent; the image is sorted below.
		lookups, errs := make([]string, len(metrics)), make([]error, len(metrics))
		var wg sync.WaitGroup
		for i, m := range metrics {
			wg.Go(func() {
				p, meta, ok := index.LookupMetadata(m.name)
				rs, err := index.Records(ctx, m.name)
				if err != nil {
					errs[i] = err
					return
				}
				slices.SortFunc(rs, func(a, b sealrollup.Record) int {
					if a.Archive != b.Archive {
						return a.Archive - b.Archive
					}
					return int(a.Slot - b.Slot)
				})
				lookups[i] = fmt.Sprintf("lookup %q %v %+v %+v %q %+v", m.name, ok, p, meta, meta.Policy, rs)
			})
		}
		wg.Wait()
		if err := errors.Join(errs...); err != nil {
			t.Fatal(err)
		}
		out.Snapshot = append(out.Snapshot, lookups...)
		slices.Sort(out.Snapshot)
		if err = ScanCatalogNames(ctx, pin, func(name string) error {
			out.Names = append(out.Names, name)
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		slices.Sort(out.Names)
		out.Names = slices.Compact(out.Names)
		return out
	}
	// Each encoding runs alone with its own engine, so no background work of
	// one run can observe the other's writer setting.
	run := func(full bool) (stages []string, images []envelopeImage) {
		rollupWriteDefinitions.Store(full)
		defer rollupWriteDefinitions.Store(false)
		root, engine = t.TempDir(), nil
		now.Store(1794700800)
		rng := rand.New(rand.NewPCG(3, 5))
		open(nil)
		defer func() { _ = engine.Close(); engine = nil }()
		snap := func(stage string) {
			stages, images = append(stages, stage), append(images, image())
		}
		order, merges := uint64(0), 0
		for epoch := 0; epoch < 32; epoch++ {
			clock := now.Add(600)
			var batches []Batch
			// Every name, every epoch: parts stay in one merge size tier.
			for _, m := range metrics {
				order++
				rows := make([]Row, 2+rng.IntN(5))
				for j := range rows {
					rows[j] = Row{ID: m.id, Time: clock - int64(rng.IntN(60))*m.step, Bits: rng.Uint64(), Seq: uint64(j + 1)}
				}
				batches = append(batches, Batch{ID: m.id, Policy: m.policy, Order: order, Clock: clock, Points: rows})
			}
			if err := engine.AppendBatches(0, batches, order, true); err != nil {
				t.Fatal(err)
			}
			snap(fmt.Sprintf("seal %d", epoch))
			if epoch%16 == 15 {
				merges += mergeDrain(t, engine.owners[0])
				snap(fmt.Sprintf("merge %d", epoch))
				open(nil)
				snap(fmt.Sprintf("restart %d", epoch))
			}
		}
		if merges == 0 {
			t.Fatal("no parts merged")
		}
		now.Add(600)
		open(func(_ RollupPolicy, m RollupMetadata, _ time.Time) bool { return m.ID%5 == 0 })
		if err := engine.AppendBatches(0, nil, order, true); err != nil {
			t.Fatal(err)
		}
		open(nil)
		snap("expiry")
		mergeDrain(t, engine.owners[0])
		snap("expiry merge")
		open(nil)
		snap("expiry restart")
		t.Logf("full=%v: %d merges", full, merges)
		return stages, images
	}
	stages, images := run(full)
	data, err := json.Marshal(envelopeRun{Stages: stages, Images: images})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(os.Getenv("GO_CARBON_SLIM_IMAGE"), data, 0600); err != nil {
		t.Fatal(err)
	}
}
