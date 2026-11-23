package whisperio

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"sync"
	"syscall"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/contentiontest"
)

// This fixture also runs unmodified against the pre-change master binary.
// It hashes every history/header byte after every correction, including all
// archives. The optional sparse-repair certificate is independently tested and
// excluded so this remains comparable with the pre-certificate master oracle.
func TestLateReplayMasterDifferential(t *testing.T) {
	h := sha256.New()
	for _, method := range []store.AggregationMethod{store.Average, store.Sum, store.Min, store.Max, store.Last, store.First} {
		for _, xff := range []float32{0, .5, 1} {
			m, kv, base := tailClassicSeed(t, method, xff)
			codec := &ContinuationCodec{}
			bits := []uint64{0, 1 << 63, 1, 0x7ff0000000000000, 0xfff0000000000000, 0x7ff8000000000012, 0x7ff0000000000001, 0x3ff0000000000001}
			for round := range 12 {
				var input []store.Point
				for j := 0; j < 564; j++ {
					// All points are late, out of order, with repeated corrections to one slot.
					ts := base - int64(1+(j*73+round*17)%10000)*60
					input = append(input, store.Point{Timestamp: ts, Value: math.Float64frombits(bits[(j+round)%len(bits)])})
				}
				input = append(input, input[0], store.Point{Timestamp: input[0].Timestamp, Value: float64(round) + .125})
				if err := codec.Update(t.Context(), kv, m, input, -1, func() time.Time { return time.Unix(base+1, 0) }, nil); err != nil {
					t.Fatal(err)
				}
				keys := make([]string, 0, len(kv))
				for key := range kv {
					if len(key) == 10 && key[0] == 'f' && key[9] == 'R' {
						continue
					}
					keys = append(keys, key)
				}
				sort.Strings(keys)
				for _, key := range keys {
					binary.Write(h, binary.BigEndian, uint64(len(key)))
					h.Write([]byte(key))
					binary.Write(h, binary.BigEndian, uint64(len(kv[key])))
					h.Write(kv[key])
				}
			}
		}
	}
	// Identical correction batches make the expected serial result independent
	// of scheduling, while 192 writers contend on this same classic metric.
	m, seed, base := tailClassicSeed(t, store.Average, .5)
	db, err := store.Open(filepath.Join(t.TempDir(), "same-metric"), store.Options{ExactBlocks: true, Continuation: &ContinuationCodec{}, TimeBuckets: true, SyncInterval: time.Hour, Now: func() time.Time { return time.Unix(base+1, 0) }})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var keys []store.RelayoutKV
	for key, value := range seed {
		keys = append(keys, store.RelayoutKV{Key: []byte(key), Value: value})
	}
	sort.Slice(keys, func(i, j int) bool { return bytes.Compare(keys[i].Key, keys[j].Key) < 0 })
	if _, err := db.ImportTransfer(t.Context(), []store.TransferMetric{{Config: m.MetricConfig, Version: store.TransferVersion{Name: m.Name}, Continuation: true, Keys: keys}}); err != nil {
		t.Fatal(err)
	}
	input := []store.Point{{Timestamp: base - 60, Value: math.Float64frombits(0x7ff8000000000012)}, {Timestamp: base - 120, Value: math.Copysign(0, -1)}, {Timestamp: base - 60, Value: 42}}
	var wg sync.WaitGroup
	for worker := range 192 {
		wg.Go(func() {
			var err error
			if worker%2 == 0 {
				err = db.UpdateMany(t.Context(), m.Name, input)
			} else {
				err = db.UpdateBatch(t.Context(), []store.MetricUpdate{{Name: m.Name, Points: input}}, nil)
			}
			if err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	out, err := db.ExportTransfer(t.Context(), []string{m.Name})
	if err != nil {
		t.Fatal(err)
	}
	for _, item := range out[0].Keys {
		if len(item.Key) == 10 && item.Key[0] == 'f' && item.Key[9] == 'R' {
			continue
		}
		h.Write(item.Key)
		h.Write(item.Value)
	}
	result := fmt.Sprintf("%x\n", h.Sum(nil))
	t.Logf("persisted-byte digest %s", result)
	// Captured against pre-change master on darwin/arm64. Floating-point NaN
	// payload selection is architecture dependent; other platforms can supply
	// a locally captured master oracle with GO_CARBON_LATE_ORACLE below.
	const masterDigest = "180b88468aea43e4acaafb7986539cfbe4c59968fc013b895a6b1748b4207517\n"
	if runtime.GOOS == "darwin" && runtime.GOARCH == "arm64" && result != masterDigest {
		t.Fatalf("stored bytes differ from master: want %s, got %s", masterDigest, result)
	}
	if path := os.Getenv("GO_CARBON_LATE_ORACLE"); path != "" {
		if os.Getenv("GO_CARBON_WRITE_ORACLE") == "1" {
			if err := os.WriteFile(path, []byte(result), 0600); err != nil {
				t.Fatal(err)
			}
		} else {
			want, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(want, []byte(result)) {
				t.Fatalf("master digest %s changed to %s", want, result)
			}
		}
	}
}

// Each worker owns eight distinct metrics; all writes merge into populated
// classic ring slots and propagate to two coarser archives. The fixture has
// eight days of history, same-timestamp corrections, and nonmonotonic input.
func BenchmarkLateClassicReplay192(b *testing.B) {
	const workers, group, batch = 192, 8, 564
	m, seed, base := tailClassicSeed(b, store.Average, .5)
	db, err := store.Open(filepath.Join(b.TempDir(), "store"), store.Options{ExactBlocks: true, TimeBuckets: true, Continuation: &ContinuationCodec{}, Now: func() time.Time { return time.Unix(base+1, 0) }, SyncInterval: time.Hour, CacheSize: 64 << 20, MetadataCacheSize: 32 << 20, MemTableSize: 32 << 20})
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	db.StopBackground()
	var keys []store.RelayoutKV
	for key, value := range seed {
		keys = append(keys, store.RelayoutKV{Key: []byte(key), Value: value})
	}
	sort.Slice(keys, func(i, j int) bool { return bytes.Compare(keys[i].Key, keys[j].Key) < 0 })
	names := make([]string, workers*group)
	// Amortize the import's durable ID reservation and WAL barrier in setup.
	for start := 0; start < len(names); start += 128 {
		var records []store.TransferMetric
		for i := start; i < min(start+128, len(names)); i++ {
			cfg := m.MetricConfig
			cfg.Name = fmt.Sprintf("replay.metric.%05d", i)
			names[i] = cfg.Name
			records = append(records, store.TransferMetric{Config: cfg, Version: store.TransferVersion{Name: cfg.Name}, Continuation: true, Keys: keys})
		}
		if _, err := db.ImportTransfer(context.Background(), records); err != nil {
			b.Fatal(err)
		}
	}
	if err := db.Flush(); err != nil {
		b.Fatal(err)
	}
	inputs := make([][]store.Point, workers)
	for w := range workers {
		for j := range batch {
			inputs[w] = append(inputs[w], store.Point{Timestamp: base - int64(1+(j*73+w*17)%10000)*60, Value: float64(j%97) + .125})
		}
	}
	runtime.GC()
	var before, after syscall.Rusage
	var memBefore, memAfter runtime.MemStats
	cpu := func(r syscall.Rusage) float64 {
		return float64(r.Utime.Sec+r.Stime.Sec) + float64(r.Utime.Usec+r.Stime.Usec)/1e6
	}
	runtime.ReadMemStats(&memBefore)
	syscall.Getrusage(syscall.RUSAGE_SELF, &before)
	wait := contentiontest.WaitSeconds()
	b.ReportAllocs()
	b.ResetTimer()
	var wg sync.WaitGroup
	for w := range workers {
		wg.Go(func() {
			updates := make([]store.MetricUpdate, group)
			for j := range updates {
				updates[j] = store.MetricUpdate{Name: names[w*group+j], Points: inputs[w]}
			}
			for i := w; i < b.N; i += workers {
				if err := db.UpdateBatch(context.Background(), updates, nil); err != nil {
					b.Error(err)
					return
				}
			}
		})
	}
	wg.Wait()
	b.StopTimer()
	syscall.Getrusage(syscall.RUSAGE_SELF, &after)
	runtime.ReadMemStats(&memAfter)
	points := float64(b.N * group * batch)
	b.ReportMetric((cpu(after)-cpu(before))*1e6/points, "CPU-s/Mpoint")
	b.ReportMetric(float64(memAfter.Mallocs-memBefore.Mallocs)/points, "allocs/point")
	b.ReportMetric(float64(memAfter.TotalAlloc-memBefore.TotalAlloc)/points, "bytes/point")
	b.ReportMetric((contentiontest.WaitSeconds()-wait)*1e6/points, "wait-s/Mpoint")
}
