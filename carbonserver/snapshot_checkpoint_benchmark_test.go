package carbonserver

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/recovery"
	"github.com/go-graphite/go-carbon/points"
)

// This opt-in shutdown probe uses captured catalogue names and a deterministic
// point history. It writes only to a separate scratch directory. Unlike a small
// query benchmark, it includes millions of cache keys, concurrent-source record
// chains, durable source files, checkpoint construction, and validated opening.
func TestCapturedPendingCheckpoint(t *testing.T) {
	dir, out := os.Getenv("GO_CARBON_SNAPSHOT_DIR"), os.Getenv("GO_CARBON_CHECKPOINT_DIR")
	if dir == "" || out == "" {
		t.Skip("set prepared snapshot and checkpoint scratch directories")
	}
	count := 14000000
	if text := os.Getenv("GO_CARBON_CHECKPOINT_METRICS"); text != "" {
		var err error
		count, err = strconv.Atoi(text)
		if err != nil || count < 1 {
			t.Fatal("invalid checkpoint metric count")
		}
	}
	if err := os.MkdirAll(out, 0700); err != nil {
		t.Fatal(err)
	}
	cache, root := filepath.Join(dir, "files.gzip"), filepath.Join(dir, "data-root")
	s, err := openIndexSnapshot(cache, root)
	if err != nil {
		t.Fatal(err)
	}
	defer s.close()
	listener := NewCarbonserverListener(nil)
	ti := newTrie(".wsp", 0, nil)
	ti.snapshot = s
	listener.UpdateFileIndex(&fileIndex{trieIdx: ti})
	builder := recovery.NewConcurrentBuilder(listener.SavedMetricLookups(), 8)
	writers := make([]*recovery.Writer, 2)
	defer func() {
		for _, w := range writers {
			if w != nil {
				_, _ = w.Close()
			}
		}
	}()
	for file, name := range []string{"cache.1.2.bin", "input.1.2.bin"} {
		writers[file], err = recovery.NewWriter(filepath.Join(out, name), file, 1<<20, builder)
		if err != nil {
			t.Fatal(err)
		}
	}
	reader, err := NewFileListCache(cache, FLCVersionUnspecified, 'r')
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	digests := [2]hash.Hash{sha256.New(), sha256.New()}
	var digestScratch []byte
	digestPoints := func(h hash.Hash, p *points.Points) {
		digestScratch = binary.LittleEndian.AppendUint64(digestScratch[:0], uint64(len(p.Metric)))
		digestScratch = append(digestScratch, p.Metric...)
		digestScratch = binary.LittleEndian.AppendUint64(digestScratch, uint64(len(p.Data)))
		for _, v := range p.Data {
			digestScratch = binary.LittleEndian.AppendUint64(digestScratch, uint64(v.Timestamp))
			digestScratch = binary.LittleEndian.AppendUint64(digestScratch, math.Float64bits(v.Value))
		}
		_, _ = h.Write(digestScratch)
	}
	var pointCount uint64
	samples := make(map[string][]points.Point)
	started := time.Now()
	for n := 0; n < count; n++ {
		e, err := reader.Read()
		if err != nil {
			t.Fatalf("catalogue ended at %d: %v", n, err)
		}
		name := strings.ReplaceAll(strings.TrimSuffix(strings.TrimPrefix(e.Path, "/"), ".wsp"), "/", ".")
		if n%1000 == 0 {
			name += ".codex_checkpoint_unseen"
		}
		p := points.Points{Metric: name, Data: []points.Point{{Timestamp: 1700000000, Value: float64(n)}, {Timestamp: 1700000001, Value: float64(n) + 0.5}}}
		if n%3 == 0 {
			p.Data = append(p.Data, points.Point{Timestamp: 1700000002, Value: float64(n) + 0.25})
		}
		if err = writers[0].WritePoints(&p); err != nil {
			t.Fatal(err)
		}
		digestPoints(digests[0], &p)
		pointCount += uint64(len(p.Data))
		if n%65536 == 0 || n%1000 == 0 {
			samples[name] = append([]points.Point(nil), p.Data...)
		}
		if n%64 == 0 {
			p.Data = []points.Point{{Timestamp: 1700000001, Value: -float64(n) - 1}}
			if err = writers[1].WritePoints(&p); err != nil {
				t.Fatal(err)
			}
			digestPoints(digests[1], &p)
			pointCount++
			if samples[name] != nil {
				samples[name] = append(samples[name], p.Data...)
			}
		}
	}
	files := make([]recovery.File, 2)
	for i, w := range writers {
		files[i], err = w.Close()
		if err != nil {
			t.Fatal(err)
		}
	}
	dumpSeconds := time.Since(started).Seconds()
	prepareStarted := time.Now()
	builder.Prepare()
	prepareSeconds := time.Since(prepareStarted).Seconds()
	checkpointStarted := time.Now()
	index, err := recovery.WriteIndex(out, builder)
	if err != nil {
		t.Fatal(err)
	}
	if err = recovery.Publish(out, root, files[0], files[1], index, "captured-checkpoint-test"); err != nil {
		t.Fatal(err)
	}
	checkpointSeconds := time.Since(checkpointStarted).Seconds()
	writers = nil
	runtime.GC()
	openStarted := time.Now()
	bundle, err := recovery.OpenBundle(out, root)
	if err != nil {
		t.Fatal(err)
	}
	defer bundle.Close()
	openSeconds := time.Since(openStarted).Seconds()
	if bundle.Points() != pointCount || bundle.Metrics() != uint64(count) {
		t.Fatal("checkpoint aggregate differs")
	}
	newNames := 0
	if err = bundle.NewNames(func(name string) error {
		newNames++
		if !strings.HasSuffix(name, ".codex_checkpoint_unseen") {
			return fmt.Errorf("unexpected new metric: %q", name)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if newNames != (count+999)/1000 {
		t.Fatal("new metric catalogue differs", newNames)
	}
	for name, expected := range samples {
		slot, found, err := bundle.Find(name)
		if err != nil || !found {
			t.Fatal("missing sample", name, err)
		}
		got, err := bundle.Read(slot)
		if err != nil || len(got.Data) != len(expected) {
			t.Fatal("sample count differs", name, err)
		}
		for i, v := range expected {
			if got.Data[i] != v {
				t.Fatal("sample history differs", name, i)
			}
		}
	}
	// Full streamed legacy decode, with a field-wise oracle independent of the
	// binary encoder, checks every metric name, timestamp and float bit pattern.
	for i, file := range files {
		f, err := os.Open(filepath.Join(out, file.Name))
		if err != nil {
			t.Fatal(err)
		}
		digest := sha256.New()
		err = points.ReadBinary(f, func(p *points.Points) { digestPoints(digest, p) })
		closeErr := f.Close()
		if err != nil || closeErr != nil {
			t.Fatal(err, closeErr)
		}
		if string(digest.Sum(nil)) != string(digests[i].Sum(nil)) {
			t.Fatal("legacy source content differs", i)
		}
	}
	result := map[string]any{"metrics": count, "points": pointCount, "new_names": newNames, "dump_seconds": dumpSeconds, "prepare_seconds": prepareSeconds, "checkpoint_seconds": checkpointSeconds, "open_seconds": openSeconds, "source_bytes": files[0].Size + files[1].Size, "index_bytes": index.Size, "sampled_histories": len(samples)}
	raw, _ := json.Marshal(result)
	t.Log(string(raw))
	if err = os.WriteFile(filepath.Join(out, "result.json"), raw, 0600); err != nil {
		t.Fatal(err)
	}
}

func TestCapturedPendingCheckpointOpen(t *testing.T) {
	dir, out := os.Getenv("GO_CARBON_SNAPSHOT_DIR"), os.Getenv("GO_CARBON_CHECKPOINT_DIR")
	if dir == "" || out == "" {
		t.Skip("set prepared snapshot and checkpoint scratch directories")
	}
	started := time.Now()
	bundle, err := recovery.OpenBundle(out, filepath.Join(dir, "data-root"))
	if err != nil {
		t.Fatal(err)
	}
	defer bundle.Close()
	t.Logf("open_seconds=%.6f metrics=%d points=%d", time.Since(started).Seconds(), bundle.Metrics(), bundle.Points())
}
