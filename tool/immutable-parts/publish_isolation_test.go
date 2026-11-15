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
	pub                             *pp.Publisher
	catalog                         []byte
	names                           map[uint64]string
	refs                            []pp.Ref
	planNS, commitNS, seals, points atomic.Uint64
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
	plan, err := o.pub.PlanSeal(pp.Request{Envelope: pp.Envelope{Version: 1, Generation: generation, Catalog: o.catalog, Frontier: in.Next.Cursors, Parts: refs}, Added: map[string][]pr.BlockData{name: blocks}})
	if err != nil {
		return coreSealPlan{}, err
	}
	o.planNS.Add(uint64(time.Since(start)))
	o.seals.Add(1)
	o.points.Add(uint64(len(in.Rows)))
	return coreSealPlan{Files: [][]coreKV{{{Key: []byte{0x70, 'v'}, Value: plan.Value}}}, Commit: func() { start := time.Now(); plan.Commit(); o.refs = refs; o.commitNS.Add(uint64(time.Since(start))) }}, nil
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
	if _, err = os.Stat(root); !os.IsNotExist(err) {
		t.Fatal("benchmark directory must not exist")
	}
	if err = os.MkdirAll(root, 0700); err != nil {
		t.Fatal(err)
	}
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
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var readerWG sync.WaitGroup
	var queries, readErrors atomic.Uint64
	ready := make(chan error, readers)
	budget := pr.NewBudget(64 << 30)
	for n := 0; n < readers; n++ {
		readerWG.Add(1)
		go func(n int) {
			defer readerWG.Done()
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
					_, err = h.Render(ctx, m.Name, 99, 161, 200)
					h.Close()
				}
				if err != nil && ctx.Err() == nil {
					readErrors.Add(1)
				}
				queries.Add(1)
			}
		}(n)
	}
	for range readers {
		if err = <-ready; err != nil {
			cancel()
			readerWG.Wait()
			t.Fatal(err)
		}
	}
	start := time.Now()
	done := make(chan error, cfg.Producers)
	for p := 0; p < cfg.Producers; p++ {
		go func(p int) {
			var sequence uint64 = 2
			for time.Since(start) < duration {
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
	sample := coreSample(e, "publisher_isolation", start, 0)
	sample["writer_seconds"] = elapsed
	sample["readers"] = readers
	sample["gomaxprocs"] = runtime.GOMAXPROCS(0)
	sample["queries"] = queries.Load()
	sample["read_errors"] = readErrors.Load()
	sample["metrics"] = len(metrics)
	sample["workload"] = "core_apply_batches_with_fixed24_durable_publisher"
	sample["writer_mps"] = float64(sample["committed"].(uint64)-uint64(len(metrics))) / elapsed / 1e6
	var planNS, commitNS, seals, points uint64
	for _, o := range owners {
		planNS += o.planNS.Load()
		commitNS += o.commitNS.Load()
		seals += o.seals.Load()
		points += o.points.Load()
	}
	sample["publication_plan_ns"] = planNS
	sample["publication_commit_ns"] = commitNS
	sample["publication_seals"] = seals
	sample["publication_points"] = points
	// Readers stay active for the specified settle period; no input is offered.
	time.Sleep(3 * time.Second)
	cancel()
	readerWG.Wait()
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
