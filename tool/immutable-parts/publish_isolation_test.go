package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"runtime"
	rmetrics "runtime/metrics"
	"runtime/pprof"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pf "github.com/go-graphite/go-carbon/internal/partsformat"
	pp "github.com/go-graphite/go-carbon/internal/partspublish"
	pr "github.com/go-graphite/go-carbon/internal/partsread"
)

type isolationMetric struct {
	ID   uint64
	Name string
}
type isolationOwner struct {
	pub                                                     *pp.Publisher
	handoff                                                 string
	catalog                                                 []byte
	names                                                   map[uint64]string
	refs                                                    []pp.Ref
	planNS, commitNS, seals, points                         atomic.Uint64
	sortNS, prepareNS, maxPlanNS, maxPrepareNS, maxCommitNS atomic.Uint64
}

func isolationMaximum(dst *atomic.Uint64, value uint64) {
	for old := dst.Load(); value > old; old = dst.Load() {
		if dst.CompareAndSwap(old, value) {
			return
		}
	}
}

// An external profiler may attach after the reader child exists and release
// these barriers once its counters are ready/stopped. The throughput clock
// excludes waiting. Ordinary invocations need no external coordinator.
func isolationCounterBarrier(root, name string) error {
	if os.Getenv("PARTS_PUBLISH_EXTERNAL_COUNTERS") != "1" {
		return nil
	}
	for deadline := time.Now().Add(30 * time.Second); time.Now().Before(deadline); {
		if _, err := os.Stat(filepath.Join(root, name)); err == nil {
			return nil
		} else if !os.IsNotExist(err) {
			return err
		}
		time.Sleep(time.Millisecond)
	}
	return fmt.Errorf("external counter barrier %s timed out", name)
}

// This deliberately composes the actual core transaction with the durable
// publisher, as a continuation of TestCoreDurablePartPublisher. It qualifies
// that composition only, not the separate production daemon's policy adapter.
func newIsolationOwners(root string, shards int) ([]*isolationOwner, []isolationMetric, error) {
	owners := make([]*isolationOwner, shards)
	var all []isolationMetric
	for owner := range owners {
		o := &isolationOwner{names: map[uint64]string{}}
		owners[owner] = o
		if os.Getenv("PARTS_PUBLISH_PROCESS") == "1" {
			o.handoff = filepath.Join(root, fmt.Sprintf("snapshot-%02d.json", owner))
		}
		var entries []isolationMetric
		// Real portable names in one routing scope per owner. Numeric core routing
		// remains the explicit adapter boundary; no name hash is used as identity.
		for candidate, id := 0, uint64(owner); len(entries) < 64; candidate++ {
			name := fmt.Sprintf("isolation.owner%d.metric%d", owner, candidate)
			if pf.Route(pf.ID(name)) != uint16(owner) {
				continue
			}
			for coreShard(id, shards) != owner {
				id++
			}
			entries = append(entries, isolationMetric{id, name})
			o.names[id] = name
			id++
		}
		all = append(all, entries...)
		o.catalog, _ = json.Marshal(entries)
		decode := func(raw json.RawMessage) (pr.Catalog, func(pp.Ref, []byte) (uint64, error), int64, error) {
			var entries []isolationMetric
			if err := json.Unmarshal(raw, &entries); err != nil {
				return nil, nil, 0, err
			}
			ids := make(map[string]uint64, len(entries))
			for _, m := range entries {
				ids[m.Name] = m.ID
			}
			a := pr.Archive{Step: 1, Points: 86400}
			cat := pr.CatalogFuncs{LookupFunc: func(name string) (pr.Metric, bool) {
				id, ok := ids[name]
				return pr.Metric{ID: id, Archives: []pr.Archive{a}}, ok
			}}
			resolve := func(ref pp.Ref, key []byte) (uint64, error) {
				k, err := pf.Decode(key)
				if err != nil {
					return 0, err
				}
				id, ok := ids[string(k.Metric)]
				if !ok || k.Group != "isolation" || !bytes.HasPrefix(key, ref.Scope) {
					return 0, errors.New("unknown portable identity/scope")
				}
				return id, nil
			}
			return cat, resolve, int64(len(raw) * 4), nil
		}
		var err error
		o.pub, err = pp.New(pp.Options{Root: filepath.Join(root, fmt.Sprintf("parts-%02d", owner)), DecodeCatalog: decode})
		if err != nil {
			for _, p := range owners {
				if p != nil && p.pub != nil {
					p.pub.Close()
				}
			}
			return nil, nil, err
		}
	}
	return owners, all, nil
}

func (o *isolationOwner) plan(owner int, in coreSealInput) (coreSealPlan, error) {
	start := time.Now()
	// The hook runs before the core sorts. Keep apply batches untouched and
	// group a detached row view for the immutable block encoder.
	in.Rows = slices.Clone(in.Rows)
	sortCoreRows(in.Rows, 86400)
	o.sortNS.Add(uint64(time.Since(start)))
	generation := in.Next.NextPart
	var part pf.PartID
	if _, err := rand.Read(part[:]); err != nil {
		return coreSealPlan{}, err
	}
	scope, err := pf.PartitionPrefixForGroup("isolation", uint16(owner), 0)
	if err != nil {
		return coreSealPlan{}, err
	}
	name := fmt.Sprintf("part-%020d", generation)
	a := pr.Archive{Step: 1, Points: 86400}
	var blocks []pr.BlockData
	for pos := 0; pos < len(in.Rows); {
		end := pos + 1
		for end < len(in.Rows) && in.Rows[end].ID == in.Rows[pos].ID {
			end++
		}
		metric := o.names[in.Rows[pos].ID]
		if metric == "" {
			return coreSealPlan{}, errors.New("unknown core ID")
		}
		for from, ordinal := pos, uint32(0); from < end; ordinal++ {
			until := min(end, from+pr.ArchiveBlockBytes(a)/24)
			key, err := pf.Encode(pf.Key{Group: "isolation", VShard: uint16(owner), Metric: pf.ID(metric), Part: part, Ordinal: ordinal})
			if err != nil {
				return coreSealPlan{}, err
			}
			samples := make([]pr.Sample, until-from)
			for i, r := range in.Rows[from:until] {
				samples[i] = pr.Sample{Time: r.Time, Bits: r.Bits, Sequence: r.Seq}
			}
			blocks = append(blocks, pr.BlockData{SlotBlock: pr.SlotBlock{ID: in.Rows[pos].ID, Archive: a, Slots: pr.SlotRange{From: 0, Until: a.Points}, Key: key}, Samples: samples})
			from = until
		}
		pos = end
	}
	ref := pp.Ref{Name: name, Generation: generation, Version: 1, Codec: pp.CodecV1, Scope: scope}
	refs := append(append([]pp.Ref(nil), o.refs...), ref)
	prepareStart := time.Now()
	plan, err := o.pub.PlanSeal(pp.Request{Envelope: pp.Envelope{Version: 1, Generation: generation, Catalog: o.catalog, Frontier: in.Next.Cursors, Parts: refs}, Added: map[string][]pr.BlockData{name: blocks}})
	if err != nil {
		return coreSealPlan{}, err
	}
	prepareNS := uint64(time.Since(prepareStart))
	planNS := uint64(time.Since(start))
	o.prepareNS.Add(prepareNS)
	isolationMaximum(&o.maxPrepareNS, prepareNS)
	o.planNS.Add(planNS)
	isolationMaximum(&o.maxPlanNS, planNS)
	o.seals.Add(1)
	o.points.Add(uint64(len(in.Rows)))
	return coreSealPlan{Files: [][]coreKV{{{Key: []byte{0x70, 'v'}, Value: plan.Value}}}, Commit: func() {
		start := time.Now()
		plan.Commit()
		o.refs = refs
		if o.handoff != "" {
			if err := os.WriteFile(o.handoff+".tmp", plan.Value, 0600); err != nil {
				panic(err)
			}
			if err := os.Rename(o.handoff+".tmp", o.handoff); err != nil {
				panic(err)
			}
		}
		commitNS := uint64(time.Since(start))
		o.commitNS.Add(commitNS)
		isolationMaximum(&o.maxCommitNS, commitNS)
	}}, nil
}

func TestPublisherWriterIsolation(t *testing.T) {
	root := os.Getenv("PARTS_PUBLISH_BENCH_DIR")
	if root == "" {
		t.Skip("opt-in engine publication benchmark")
	}
	readers, err := strconv.Atoi(os.Getenv("PARTS_PUBLISH_READERS"))
	if err != nil || readers < 0 || readers > 64 {
		t.Fatal("readers 0..64 required")
	}
	duration := 20 * time.Second
	if raw := os.Getenv("PARTS_PUBLISH_SECONDS"); raw != "" {
		n, e := strconv.Atoi(raw)
		if e != nil || n < 1 {
			t.Fatal("invalid duration")
		}
		duration = time.Duration(n) * time.Second
	}
	settle := 3 * time.Second
	if raw := os.Getenv("PARTS_PUBLISH_SETTLE_MS"); raw != "" {
		n, parseErr := strconv.Atoi(raw)
		if parseErr != nil || n < 0 {
			t.Fatal("invalid settle milliseconds")
		}
		settle = time.Duration(n) * time.Millisecond
	}
	var batchesPerProducer int
	if raw := os.Getenv("PARTS_PUBLISH_BATCHES_PER_PRODUCER"); raw != "" {
		var parseErr error
		batchesPerProducer, parseErr = strconv.Atoi(raw)
		if parseErr != nil || batchesPerProducer < 1 {
			t.Fatal("invalid fixed batch count")
		}
	}
	if _, err = os.Stat(root); !os.IsNotExist(err) {
		t.Fatal("benchmark directory must not exist")
	}
	if err = os.MkdirAll(root, 0700); err != nil {
		t.Fatal(err)
	}
	pprof.SetGoroutineLabels(pprof.WithLabels(context.Background(), pprof.Labels("role", "writer")))
	defer pprof.SetGoroutineLabels(context.Background())
	const shards = 16
	owners, metrics, err := newIsolationOwners(root, shards)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		for _, o := range owners {
			o.pub.Close()
		}
	}()
	cfg := coreConfig{Shards: shards, Producers: 4, MaxRows: 262144, AppendParts: true, PartitionSeconds: 86400}
	e, err := openCoreWithHooks(filepath.Join(root, "db"), cfg, coreHooks{PlanSeal: func(owner int, in coreSealInput) (coreSealPlan, error) { return owners[owner].plan(owner, in) }})
	if err != nil {
		t.Fatal(err)
	}
	closed := false
	defer func() {
		if !closed {
			_ = e.close()
		}
	}()
	// Seed through exactly the same transaction before readers start.
	for p := 0; p < cfg.Producers; p++ {
		var batches []coreBatch
		for i, m := range metrics {
			if i%cfg.Producers == p {
				batches = append(batches, coreBatch{ID: m.ID, Order: 1, Clock: 200, Policy: []byte("1s:1d"), Points: []row{{m.ID, 100, math.Float64bits(100), 1}}})
			}
		}
		if err = e.submitBatches(p, batches, 1, true); err != nil {
			t.Fatal(err)
		}
	}
	var process *isolationProcess
	localReaders := readers
	if os.Getenv("PARTS_PUBLISH_PROCESS") == "1" {
		process, err = startIsolationProcess(root, readers)
		if err != nil {
			t.Fatal(err)
		}
		defer process.stop()
		localReaders = 0
	}
	// Background parents keep cancelCtx.Err reader-local. A shared cancelCtx
	// introduces reader-only mutex contention unrelated to publication.
	contexts := make([]context.Context, localReaders)
	cancels := make([]context.CancelFunc, localReaders)
	for n := range localReaders {
		contexts[n], cancels[n] = context.WithCancel(context.Background())
	}
	cancelAll := func() {
		for _, cancel := range cancels {
			cancel()
		}
	}
	defer cancelAll()
	var readerWG sync.WaitGroup
	var queries, readErrors atomic.Uint64
	ready := make(chan error, localReaders)
	budget := pr.NewBudget(64 << 30)
	for n := 0; n < localReaders; n++ {
		readerWG.Add(1)
		go func(n int) {
			defer readerWG.Done()
			ctx := contexts[n]
			pprof.SetGoroutineLabels(pprof.WithLabels(ctx, pprof.Labels("role", "reader")))
			unpin, err := pinSnapshotReader()
			ready <- err
			if err != nil {
				return
			}
			defer unpin()
			for i := n; ctx.Err() == nil; i++ {
				owner := i % shards
				m := metrics[owner*64+(i/shards)%64]
				h, err := owners[owner].pub.Pin(budget, 8<<20)
				if err == nil {
					var series *pr.Series
					series, err = h.Render(ctx, m.Name, 99, 161, 200)
					if err == nil {
						if series == nil || len(series.Values) == 0 || math.Float64bits(series.Values[0]) != math.Float64bits(100) {
							err = errors.New("published seed missing or changed")
						} else {
							for i, value := range series.Values {
								if !math.IsNaN(value) && math.Float64bits(value) != math.Float64bits(float64(series.From+int64(i)*series.Step)) {
									err = errors.New("published value bits differ from input")
									break
								}
							}
						}
					}
					h.Close()
				}
				if err != nil && ctx.Err() == nil {
					readErrors.Add(1)
				}
				queries.Add(1)
			}
		}(n)
	}
	for range localReaders {
		if err = <-ready; err != nil {
			cancelAll()
			readerWG.Wait()
			t.Fatal(err)
		}
	}
	if err = os.WriteFile(filepath.Join(root, "writer-ready"), []byte(strconv.Itoa(os.Getpid())), 0600); err != nil {
		t.Fatal(err)
	}
	if err = isolationCounterBarrier(root, "writer-go"); err != nil {
		t.Fatal(err)
	}
	gcMetrics := []rmetrics.Sample{
		{Name: "/cpu/classes/gc/mark/assist:cpu-seconds"},
		{Name: "/cpu/classes/gc/total:cpu-seconds"},
		{Name: "/cpu/classes/total:cpu-seconds"},
		{Name: "/gc/cycles/total:gc-cycles"},
		{Name: "/gc/heap/allocs:bytes"},
		{Name: "/cpu/classes/gc/pause:cpu-seconds"},
		{Name: "/cpu/classes/gc/mark/dedicated:cpu-seconds"},
	}
	rmetrics.Read(gcMetrics)
	assistStart := gcMetrics[0].Value.Float64()
	gcCPUStart := gcMetrics[1].Value.Float64()
	cpuStart := gcMetrics[2].Value.Float64()
	cyclesStart := gcMetrics[3].Value.Uint64()
	allocStart := gcMetrics[4].Value.Uint64()
	pauseStart := gcMetrics[5].Value.Float64()
	markStart := gcMetrics[6].Value.Float64()
	start := time.Now()
	done := make(chan error, cfg.Producers)
	for p := 0; p < cfg.Producers; p++ {
		go func(p int) {
			var sequence uint64 = 2
			for batch := 0; ; batch++ {
				if (batchesPerProducer > 0 && batch >= batchesPerProducer) || (batchesPerProducer == 0 && time.Since(start) >= duration) {
					break
				}
				batches := make([]coreBatch, 0, len(metrics)/cfg.Producers)
				for i, m := range metrics {
					if i%cfg.Producers != p {
						continue
					}
					rs := make([]row, 32)
					for j := range rs {
						timestamp := 100 + int64((sequence+uint64(j))%60)
						rs[j] = row{m.ID, timestamp, math.Float64bits(float64(timestamp)), sequence + uint64(j)}
					}
					batches = append(batches, coreBatch{ID: m.ID, Order: sequence, Clock: 200, Policy: []byte("1s:1d"), Points: rs})
				}
				sequence += 32
				if err := e.submitBatches(p, batches, sequence, false); err != nil {
					done <- err
					return
				}
			}
			done <- e.submitBatches(p, nil, sequence, true)
		}(p)
	}
	for range cfg.Producers {
		err = errors.Join(err, <-done)
	}
	elapsed := time.Since(start).Seconds()
	rmetrics.Read(gcMetrics)
	if writeErr := os.WriteFile(filepath.Join(root, "writer-done"), []byte("done"), 0600); writeErr != nil {
		t.Fatal(writeErr)
	}
	if err = errors.Join(err, isolationCounterBarrier(root, "writer-counters-stopped")); err != nil {
		t.Fatal(err)
	}
	sample := coreSample(e, "publisher_isolation", start, 0)
	sample["gc_assist_seconds"] = gcMetrics[0].Value.Float64() - assistStart
	sample["gc_cpu_seconds"] = gcMetrics[1].Value.Float64() - gcCPUStart
	sample["runtime_cpu_seconds"] = gcMetrics[2].Value.Float64() - cpuStart
	sample["writer_interval_gc_cycles"] = gcMetrics[3].Value.Uint64() - cyclesStart
	// Runtime total CPU is available processor capacity, including idle time.
	// This percentage is process-wide; it does not attribute assists to writers.
	sample["gc_assist_pct_capacity"] = 100 * sample["gc_assist_seconds"].(float64) / (elapsed * float64(runtime.GOMAXPROCS(0)))
	sample["allocated_bytes"] = gcMetrics[4].Value.Uint64() - allocStart
	sample["gc_pause_cpu_seconds"] = gcMetrics[5].Value.Float64() - pauseStart
	sample["gc_mark_dedicated_seconds"] = gcMetrics[6].Value.Float64() - markStart
	sample["reader_process"] = process != nil
	sample["reader_process_procs"] = os.Getenv("PARTS_PUBLISH_READER_PROCS")
	sample["gogc"] = os.Getenv("GOGC")
	sample["gomemlimit"] = os.Getenv("GOMEMLIMIT")
	sample["reader_context"] = "independent_cancel_contexts"
	sample["writer_seconds"] = elapsed
	sample["readers"] = readers
	sample["gomaxprocs"] = runtime.GOMAXPROCS(0)
	sample["queries"] = queries.Load()
	sample["read_errors"] = readErrors.Load()
	sample["metrics"] = len(metrics)
	sample["workload"] = "core_apply_batches_with_fixed24_durable_publisher"
	sample["writer_mps"] = float64(sample["committed"].(uint64)-uint64(len(metrics))) / elapsed / 1e6
	sample["batches_per_producer"] = batchesPerProducer
	if batchesPerProducer > 0 {
		expected := uint64(batchesPerProducer) * uint64(len(metrics)) * 32
		sample["expected_committed_points"] = expected
		if sample["committed"].(uint64)-uint64(len(metrics)) != expected {
			err = errors.Join(err, errors.New("fixed input committed count differs"))
		}
	}
	var planNS, commitNS, seals, points, sortNS, prepareNS uint64
	var maxPlanNS, maxPrepareNS, maxCommitNS uint64
	for _, o := range owners {
		planNS += o.planNS.Load()
		commitNS += o.commitNS.Load()
		seals += o.seals.Load()
		points += o.points.Load()
		sortNS += o.sortNS.Load()
		prepareNS += o.prepareNS.Load()
		maxPlanNS = max(maxPlanNS, o.maxPlanNS.Load())
		maxPrepareNS = max(maxPrepareNS, o.maxPrepareNS.Load())
		maxCommitNS = max(maxCommitNS, o.maxCommitNS.Load())
	}
	sample["publication_plan_ns"] = planNS
	sample["publication_commit_ns"] = commitNS
	sample["publication_seals"] = seals
	sample["publication_points"] = points
	sample["publication_sort_clone_ns"] = sortNS
	sample["publication_prepare_ns"] = prepareNS
	sample["publication_max_plan_ns"] = maxPlanNS
	sample["publication_max_prepare_ns"] = maxPrepareNS
	sample["publication_max_commit_ns"] = maxCommitNS
	// Readers stay active for the specified settle period; no input is offered.
	sample["settle_seconds"] = settle.Seconds()
	time.Sleep(settle)
	cancelAll()
	readerWG.Wait()
	if process != nil {
		result, processErr := process.stop()
		err = errors.Join(err, processErr)
		queries.Add(result.Queries)
		readErrors.Add(result.Errors)
		sample["reader_process_result"] = result
		for i, o := range owners {
			pin, pinErr := o.pub.PinEnvelope()
			if pinErr != nil {
				err = errors.Join(err, pinErr)
				continue
			}
			var envelope pp.Envelope
			decodeErr := json.Unmarshal(pin.Value, &envelope)
			pin.Close()
			if decodeErr != nil || result.Latest[i] != envelope.Generation {
				err = errors.Join(err, fmt.Errorf("child final generation mismatch owner %d", i), decodeErr)
			}
		}
	}
	err = errors.Join(err, e.close())
	closed = true
	sample["final_queries"] = queries.Load()
	sample["final_read_errors"] = readErrors.Load()
	_ = json.NewEncoder(os.Stdout).Encode(sample)
	if readErrors.Load() != 0 {
		err = errors.Join(err, errors.New("reader errors"))
	}
	if err != nil {
		t.Fatal(err)
	}
}
