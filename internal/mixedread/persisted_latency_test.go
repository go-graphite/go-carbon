package mixedread

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	pf "github.com/go-graphite/go-carbon/internal/partsformat"
	pp "github.com/go-graphite/go-carbon/internal/partspublish"
	pr "github.com/go-graphite/go-carbon/internal/partsread"
)

// This fixture exercises real fsynced file sources and the persisted publisher.
// Its envelope file is the commit authority; it is not the daemon's SST ingest.
type persistedLatency struct {
	p            *pp.Publisher
	publications atomic.Uint64
	stop         chan struct{}
	done         chan error
}

func (r *persistedLatency) close(t *testing.T) {
	close(r.stop)
	err := <-r.done
	r.p.Close()
	fmt.Printf("PERSISTED publications=%d cadence_ms=10 envelope=file-fsync\n", r.publications.Load())
	if err != nil {
		t.Error(err)
	}
}

func newPersistedLatency(t *testing.T, root string, cat pr.Catalog, metric pr.Metric, records []pr.Record, ingest bool) *persistedLatency {
	t.Helper()
	const name = "bench.metric"
	id := pf.ID(name)
	scope, err := pf.PartitionPrefixForGroup("benchmark", pf.Route(id), 0)
	if err != nil {
		t.Fatal(err)
	}
	r := &persistedLatency{stop: make(chan struct{}), done: make(chan error, 1)}
	r.p, err = pp.New(pp.Options{Root: root, DecodeCatalog: func(raw json.RawMessage) (pr.Catalog, func(pp.Ref, []byte) (uint64, error), int64, error) {
		if string(raw) != `{"fixture":1}` {
			return nil, nil, 0, fmt.Errorf("unknown fixture checkpoint")
		}
		return cat, func(ref pp.Ref, key []byte) (uint64, error) {
			k, err := pf.Decode(key)
			if err != nil {
				return 0, err
			}
			if k.Group != "benchmark" || k.Metric != id || !bytes.HasPrefix(key, ref.Scope) || int(k.Archive) >= len(metric.Archives) {
				return 0, fmt.Errorf("foreign fixture key")
			}
			return metric.ID, nil
		}, int64(len(records)*40 + 4096*96), nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	seal := func(n uint64) error {
		partName := fmt.Sprintf("part-%08d", n)
		var partID pf.PartID
		binary.BigEndian.PutUint64(partID[8:], n)
		var blocks []pr.BlockData
		for ai, a := range metric.Archives {
			var samples []pr.Sample
			for _, record := range records {
				if record.Step == a.Step {
					samples = append(samples, record.Sample)
				}
			}
			if len(samples) == 0 {
				continue
			}
			key, err := pf.Encode(pf.Key{Group: "benchmark", VShard: pf.Route(id), Metric: id, Part: partID, Archive: uint32(ai)})
			if err != nil {
				return err
			}
			blocks = append(blocks, pr.BlockData{SlotBlock: pr.SlotBlock{ID: metric.ID, Archive: a, Slots: pr.SlotRange{From: 0, Until: a.Points}, Key: key}, Samples: samples})
		}
		plan, err := r.p.PlanSeal(pp.Request{Envelope: pp.Envelope{Version: 1, Generation: n, Catalog: json.RawMessage(`{"fixture":1}`), Frontier: []uint64{n}, Parts: []pp.Ref{{Name: partName, Generation: n, Version: 1, Codec: pp.CodecV1, Scope: scope}}}, Added: map[string][]pr.BlockData{partName: blocks}})
		if err != nil {
			return err
		}
		// Commit the complete envelope before exposing its immutable generation.
		f, err := os.OpenFile(filepath.Join(root, "envelope.tmp"), os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
		if err != nil {
			plan.Abort()
			return err
		}
		_, err = f.Write(plan.Value)
		if err == nil {
			err = f.Sync()
		}
		closeErr := f.Close()
		if err == nil {
			err = closeErr
		}
		if err == nil {
			err = os.Rename(filepath.Join(root, "envelope.tmp"), filepath.Join(root, "envelope"))
		}
		if err == nil {
			var d *os.File
			d, err = os.Open(root)
			if err == nil {
				err = d.Sync()
				_ = d.Close()
			}
		}
		if err != nil {
			plan.Abort()
			return err
		}
		plan.Commit()
		r.publications.Add(1)
		return r.p.Collect()
	}
	if err = seal(1); err != nil {
		r.p.Close()
		t.Fatal(err)
	}
	if ingest {
		go func() {
			tick := time.NewTicker(10 * time.Millisecond)
			defer tick.Stop()
			for n := uint64(2); ; n++ {
				select {
				case <-r.stop:
					r.done <- nil
					return
				case <-tick.C:
					if err := seal(n); err != nil {
						r.done <- err
						return
					}
				}
			}
		}()
	} else {
		r.done <- nil
	}
	return r
}
