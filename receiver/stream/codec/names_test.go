package codec

import (
	"bytes"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"testing/synctest"
	"time"
)

func TestSharedNameReferencesAndWire(t *testing.T) {
	for _, full := range []bool{true, false} {
		for _, flags := range []byte{0, FlagZstd} {
			t.Run(fmt.Sprintf("exact=%v/flags=%d", full, flags), func(t *testing.T) {
				table, err := NewNameTable(1<<20, full)
				if err != nil {
					t.Fatal(err)
				}
				opts := Options{Flags: flags, MaxNames: 2, DictionaryBytes: 4096}
				peers := make([]*Encoder, 3)
				for i := range peers {
					peers[i], err = NewSharedEncoder(opts, table)
					if err != nil {
						t.Fatal(err)
					}
					defer peers[i].Close()
				}
				oracle := encoder(t, opts)
				var want []byte
				var wires [3][]byte
				for i := range 500 {
					name := fmt.Appendf(nil, "example.shared.%d", i%7)
					token := table.Resolve(name, 0) // forced routing-hash collisions, including zero
					add(t, oracle, string(name), float64(i)/8, int64(i)-100)
					for _, peer := range peers {
						if _, _, err := peer.AddGlobal(name, 0, token, float64(i)/8, int64(i)-100); err != nil {
							t.Fatal(err)
						}
						if peer.RetainedBytes() > opts.DictionaryBytes {
							t.Fatal("connection byte budget exceeded")
						}
					}
					clear(name)
					if i%13 == 0 {
						want = flush(t, oracle, want)
						for j, peer := range peers {
							wires[j] = flush(t, peer, wires[j])
							if !bytes.Equal(want, wires[j]) {
								t.Fatal("shared encoder changed v1 bytes")
							}
						}
					}
				}
				want = flush(t, oracle, want)
				for i, peer := range peers {
					wires[i] = flush(t, peer, wires[i])
					if !bytes.Equal(want, wires[i]) {
						t.Fatal("wire differs")
					}
				}
				count := peers[1].NameCount()
				if err := peers[0].Reset(); err != nil {
					t.Fatal(err)
				}
				if peers[1].NameCount() != count {
					t.Fatal("reset changed a peer")
				}
			})
		}
	}
}
func TestSharedTableChurnBudgetAndStaleToken(t *testing.T) {
	table, err := NewNameTable(16384, false)
	if err != nil {
		t.Fatal(err)
	}
	stale := table.Resolve([]byte("old.name"), 0)
	peers := make([]*Encoder, 3)
	for i := range peers {
		peers[i], err = NewSharedEncoder(Options{DictionaryBytes: 4096}, table)
		if err != nil {
			t.Fatal(err)
		}
	}
	for i := range 5000 {
		name := fmt.Appendf(nil, "metric.%d", i)
		token := table.Resolve(name, 0)
		for _, peer := range peers {
			if _, _, err := peer.AddGlobal(name, 0, token, float64(i), int64(i)); err != nil {
				t.Fatal(err)
			}
			if i%16 == 0 {
				if _, err := peer.Flush(nil); err != nil {
					t.Fatal(err)
				}
			}
			if peer.RetainedBytes() > 4096 {
				t.Fatal("connection exceeds byte share")
			}
		}
		if table.Stats().Bytes > 16384 {
			t.Fatal("global byte budget exceeded")
		}
	}
	if table.Valid(stale, 0) {
		t.Fatal("unused queued hint never expired under pressure")
	}
	e := peers[0]
	if err := e.Reset(); err != nil {
		t.Fatal(err)
	}
	_, _ = e.Flush(nil)
	if _, _, err := e.AddGlobal([]byte("old.name"), 0, stale, 42, 7); err != nil {
		t.Fatal(err)
	}
	var got []observation
	decodeAll(t, decoder(t, Options{}), flush(t, e, nil), observe(&got))
	if len(got) != 1 || got[0].name != "old.name" {
		t.Fatal("stale hint aliased a reused global id")
	}
	for _, peer := range peers {
		peer.Close()
	}
	for i := range table.shards {
		s := &table.shards[i]
		s.mu.Lock()
		s.sweep(true)
		s.mu.Unlock()
	}
	if table.Stats().Names != 0 {
		t.Fatal("close retained global references")
	}
	if table.Stats().Evictions == 0 {
		t.Fatal("churn did not reclaim names")
	}
}
func TestSharedTableConcurrentConnections(t *testing.T) {
	table, err := NewNameTable(1<<20, false)
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for connection := range 40 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			e, err := NewSharedEncoder(Options{DictionaryBytes: 8192, MaxNames: 70}, table)
			if err != nil {
				t.Error(err)
				return
			}
			defer e.Close()
			for i := range 2000 {
				name := fmt.Appendf(nil, "metric.%d", (i+connection)%100)
				hash := nameHash(name)
				token := table.Resolve(name, hash)
				if _, _, err := e.AddGlobal(name, hash, token, float64(i), int64(i)); err != nil {
					t.Error(err)
					return
				}
				if e.Ready() {
					if _, err := e.Flush(nil); err != nil {
						t.Error(err)
						return
					}
				}
			}
		}()
	}
	wg.Wait()
	for i := range table.shards {
		s := &table.shards[i]
		s.mu.Lock()
		s.sweep(true)
		s.mu.Unlock()
	}
	if table.Stats().Names != 0 {
		t.Fatal("connection leaked a reference")
	}
}

func TestSharedTableUnusedGrace(t *testing.T) {
	for _, full := range []bool{false, true} {
		synctest.Test(t, func(t *testing.T) {
			table, err := NewNameTable(64<<10, full)
			if err != nil {
				t.Fatal(err)
			}
			held := table.Resolve([]byte("held.name"), 1)
			unused := table.Resolve([]byte("unused.name"), 2)
			if !table.Acquire(held, 1) {
				t.Fatal("could not acquire live name")
			}
			table.Sweep()
			time.Sleep(59 * time.Second)
			table.Sweep()
			if !table.Valid(unused, 2) {
				t.Fatal("unused name expired before its grace period")
			}
			time.Sleep(2 * time.Second)
			table.Sweep()
			if table.Valid(unused, 2) || !table.Valid(held, 1) {
				t.Fatal("sweep did not distinguish unused and referenced names")
			}
			table.Release(held)
			time.Sleep(61 * time.Second)
			table.Sweep()
			if table.Valid(held, 1) || table.Stats().Names != 0 {
				t.Fatal("released name did not expire after its own grace period")
			}
		})
	}
}

type testSeries struct{ name []byte }

func (s *testSeries) StreamName() []byte { return s.name }
func TestExternalSeriesValidationSnapshotAndReset(t *testing.T) {
	for _, flags := range []byte{0, FlagZstd} {
		identities := map[string]*testSeries{}
		calls := 0
		resolve := func(name []byte) any {
			calls++
			s := identities[string(name)]
			if s == nil {
				s = &testSeries{bytes.Clone(name)}
				identities[string(name)] = s
			}
			return s
		}
		e := encoder(t, Options{Flags: flags})
		d := decoder(t, Options{Flags: flags})
		d.SetNameResolver(resolve)
		for i := range 20 {
			add(t, e, fmt.Sprintf("name.%d", i%3), float64(i)/8, int64(i))
		}
		wire := flush(t, e, nil)
		decodeAll(t, d, wire, func(_ uint32, name []byte, _ float64, _ int64, slot *any) {
			if (*slot).(*testSeries) != identities[string(name)] {
				t.Fatal("wire id not bound to shared series")
			}
		})
		if calls != 3 {
			t.Fatal("resolved per point rather than per definition")
		}
		snapshot, err := d.Snapshot(nil)
		if err != nil {
			t.Fatal(err)
		}
		next := decoder(t, Options{Flags: flags})
		next.SetNameResolver(resolve)
		decodeAll(t, next, snapshot, nil)
		add(t, e, "name.1", 123, 456)
		wire = flush(t, e, nil)
		var a, b []observation
		decodeAll(t, d, wire, observe(&a))
		decodeAll(t, next, wire, observe(&b))
		if !reflect.DeepEqual(a, b) {
			t.Fatal("external snapshot changed predictors")
		}
		bad := bytes.Clone(wire)
		bad[len(bad)-1] ^= 1
		before := calls
		rejected := decoder(t, Options{Flags: flags})
		rejected.SetNameResolver(resolve)
		if _, err := rejected.Decode(bad, nil); err == nil {
			t.Fatal("bad frame accepted")
		}
		if calls != before {
			t.Fatal("resolver ran for a rejected frame")
		}
	}
}

func TestWireHashFilterCollision(t *testing.T) {
	seen := map[uint64]uint32{}
	var a, b uint32
	for key := uint32(1); b == 0; key++ {
		h := mixHash(uint64(key))
		signature := (h>>56)<<4 | (h & 15)
		if first := seen[signature]; first != 0 {
			a, b = first, key
		} else {
			seen[signature] = key
		}
	}
	states := wireStates{}
	index := make(wireIndex, 16)
	for i, key := range []uint32{a, b} {
		id := states.add(uint64(i+1)<<24 | uint64(key))
		index.insert(key, id)
	}
	for i, key := range []uint32{a, b} {
		id, ok := index.find(key, &states)
		if !ok || id != uint32(i) {
			t.Fatal("hash filter aliased two global ids")
		}
	}
	index.grow(100, &states)
	for i, key := range []uint32{a, b} {
		id, ok := index.find(key, &states)
		if !ok || id != uint32(i) {
			t.Fatal("growth changed wire ids")
		}
	}
}

func TestStandaloneEncoderByteBudget(t *testing.T) {
	for _, budget := range []int{4096, 16384, 1 << 20} {
		e := encoder(t, Options{DictionaryBytes: budget, MaxRows: 16})
		for i := range 500 {
			name := fmt.Sprintf("example.%s.%d", string(bytes.Repeat([]byte{'x'}, i%400)), i)
			if err := e.Add([]byte(name), float64(i), int64(i)); err != nil {
				t.Fatal(err)
			}
			if e.Ready() {
				if _, err := e.Flush(nil); err != nil {
					t.Fatal(err)
				}
			}
			if e.RetainedBytes() > budget {
				t.Fatalf("standalone dictionary retained %d > %d", e.RetainedBytes(), budget)
			}
		}
	}
}
