package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"math"
	"path/filepath"
	"testing"

	pf "github.com/go-graphite/go-carbon/internal/partsformat"
	pp "github.com/go-graphite/go-carbon/internal/partspublish"
	pr "github.com/go-graphite/go-carbon/internal/partsread"
)

// This is the first composition with the actual core PlanSeal/Commit path. The
// numeric input row ID is explicitly resolved by the test's durable name catalog;
// it is not claimed as the portable daemon backend or as a performance harness.
func TestCoreDurablePartPublisher(t *testing.T) {
	for _, blind := range []bool{false, true} {
		t.Run(map[bool]string{false: "sst", true: "batch"}[blind], func(t *testing.T) {
			root := t.TempDir()
			name := pf.ID("test.metric")
			a := pr.Archive{Step: 1, Points: 86400}
			decode := func(raw json.RawMessage) (pr.Catalog, func(pp.Ref, []byte) (uint64, error), int64, error) {
				var durable struct {
					Name    string
					Group   pf.Group
					Archive pr.Archive
				}
				if err := json.Unmarshal(raw, &durable); err != nil {
					return nil, nil, 0, err
				}
				catalog := pr.CatalogFuncs{LookupFunc: func(n string) (pr.Metric, bool) {
					return pr.Metric{ID: 900, Archives: []pr.Archive{durable.Archive}}, n == durable.Name
				}}
				resolve := func(ref pp.Ref, key []byte) (uint64, error) {
					k, err := pf.Decode(key)
					if err != nil {
						return 0, err
					}
					if string(k.Metric) != durable.Name || k.Group != durable.Group || k.Archive != 0 || !bytes.HasPrefix(key, ref.Scope) {
						return 0, errors.New("catalog/scope mismatch")
					}
					return 900, nil
				}
				return catalog, resolve, int64(len(raw) * 2), nil
			}
			pub, err := pp.New(pp.Options{Root: filepath.Join(root, "parts"), DecodeCatalog: decode})
			if err != nil {
				t.Fatal(err)
			}
			defer pub.Close()
			catalog, _ := json.Marshal(struct {
				Name    string
				Group   pf.Group
				Archive pr.Archive
			}{string(name), "test", a})
			envelopeKey := []byte{0x70, 'v'}
			var saved []byte
			cfg := coreTestConfig()
			cfg.Shards = 1
			cfg.Producers = 1
			cfg.BlindBatch = blind
			engine, err := openCoreWithHooks(filepath.Join(root, "db"), cfg, coreHooks{PlanSeal: func(owner int, in coreSealInput) (coreSealPlan, error) {
				var id pf.PartID
				if _, err := rand.Read(id[:]); err != nil {
					return coreSealPlan{}, err
				}
				scope, err := pf.PartitionPrefixForGroup("test", pf.Route(name), 0)
				if err != nil {
					return coreSealPlan{}, err
				}
				key, err := pf.Encode(pf.Key{Group: "test", VShard: pf.Route(name), Metric: name, Part: id})
				if err != nil {
					return coreSealPlan{}, err
				}
				samples := make([]pr.Sample, len(in.Rows))
				for i, r := range in.Rows {
					samples[i] = pr.Sample{Time: r.Time, Bits: r.Bits, Sequence: r.Seq}
				}
				ref := pp.Ref{Name: "first", Generation: in.Next.NextPart, Version: 1, Codec: pp.CodecV1, Scope: scope}
				request := pp.Request{Envelope: pp.Envelope{Version: 1, Generation: in.Next.NextPart, Catalog: catalog, Frontier: in.Next.Cursors, Parts: []pp.Ref{ref}}, Added: map[string][]pr.BlockData{"first": {{SlotBlock: pr.SlotBlock{ID: 7, Archive: a, Slots: pr.SlotRange{From: 0, Until: a.Points}, Key: key}, Samples: samples}}}}
				plan, err := pub.PlanSeal(request)
				if err != nil {
					return coreSealPlan{}, err
				}
				saved = append([]byte(nil), plan.Value...)
				return coreSealPlan{Files: [][]coreKV{{{Key: envelopeKey, Value: plan.Value}}}, Commit: plan.Commit}, nil
			}})
			if err != nil {
				t.Fatal(err)
			}
			if err = engine.submitBatches(0, []coreBatch{{ID: 7, Order: 1, Clock: 110, Policy: []byte("test-policy"), Points: []row{{7, 100, 0x8000000000000000, 1}}}}, 1, true); err != nil {
				t.Fatal(err)
			}
			snapshot := engine.owners[0].db.NewSnapshot()
			raw, err := coreRead(snapshot, 7)
			_ = snapshot.Close()
			if err != nil || len(raw) != 1 {
				t.Fatalf("raw%v %v", raw, err)
			}
			value, closer, err := engine.owners[0].db.Get(envelopeKey)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(value, saved) {
				t.Fatal("envelope mismatch")
			}
			_ = closer.Close()
			if engine.durableFrontiers()[0][0] != 1 {
				t.Fatal("frontier mismatch")
			}
			if err = engine.close(); err != nil {
				t.Fatal(err)
			}
			pub.Close()
			next, err := pp.New(pp.Options{Root: filepath.Join(root, "parts"), DecodeCatalog: decode})
			if err != nil {
				t.Fatal(err)
			}
			defer next.Close()
			if err = next.Recover(saved); err != nil {
				t.Fatal(err)
			}
			h, err := next.Pin(pr.NewBudget(1<<30), 1<<20)
			if err != nil {
				t.Fatal(err)
			}
			defer h.Close()
			series, err := h.Render(context.Background(), string(name), 99, 101, 110)
			if err != nil || series == nil {
				t.Fatal(series, err)
			}
			if math.Float64bits(series.Values[0]) != 0x8000000000000000 {
				t.Fatal(series.Values)
			}
		})
	}
}
