package chunkstore

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"sync/atomic"
	"testing"
	"time"
)

// Frozen v1/v2 framing reader: intentionally independent of the parallel path.
// A predecessor/rollback accepts the same header and checks every raw record.
func serialCheckpointRecords(t *testing.T, data []byte) map[string][]byte {
	t.Helper()
	if string(data[:8]) != checkpointMagic && string(data[:8]) != checkpointLegacyMagic {
		t.Fatal("predecessor cannot recognize header")
	}
	if binary.LittleEndian.Uint32(data[48:]) != crc32.ChecksumIEEE(data[:48]) {
		t.Fatal("invalid header checksum")
	}
	r := bytes.NewReader(data[52:])
	records := make(map[string][]byte)
	for {
		var frame [8]byte
		if _, err := io.ReadFull(r, frame[:]); err != nil {
			t.Fatal(err)
		}
		n := binary.LittleEndian.Uint32(frame[:4])
		if n == 0 {
			break
		}
		record := make([]byte, n)
		if _, err := io.ReadFull(r, record); err != nil {
			t.Fatal(err)
		}
		if binary.LittleEndian.Uint32(frame[4:]) != crc32.ChecksumIEEE(record) {
			t.Fatal("invalid record checksum")
		}
		name := string(recordKey(record))
		// Also exercise the unchanged predecessor's packed-state decoder.
		if decodeStateRecord(record).meta.Name != name {
			t.Fatal("invalid decoded name")
		}
		records[name] = record
	}
	return records
}

func TestParallelCheckpointCompatibilityAndContents(t *testing.T) {
	for _, magic := range []string{checkpointLegacyMagic, checkpointMagic} {
		for _, workers := range []int{1, 2, 8} {
			t.Run(fmt.Sprintf("%s/%d", magic, workers), func(t *testing.T) {
				dir := t.TempDir()
				opts := checkpointOptions()
				opts.MetadataCacheSize = 512 << 20
				db, err := Open(dir, opts)
				if err != nil {
					t.Fatal(err)
				}
				want := make(map[string][]byte)
				rng := rand.New(rand.NewSource(17))
				for i := range 1025 {
					name := fmt.Sprintf("parallel.%04d", i)
					m, err := db.Create(context.Background(), MetricConfig{Name: name, Retentions: []Retention{{Step: 1, Count: 100}}, AggregationMethod: Average})
					if err != nil {
						t.Fatal(err)
					}
					st := newMetricState(m)
					data := make([]byte, 16000)
					if i%16 != 0 {
						if _, err := rng.Read(data); err != nil {
							t.Fatal(err)
						}
					}
					st.set([]byte("header"), data)
					st.seal()
					if i%16 == 0 {
						st.packed = packStateKeys(st.kenc)
					}
					want[name] = appendStateRecord(nil, name, st)
					sh := &db.stateCache.shards[db.metricStripe(name)]
					sh.mu.Lock()
					db.stateCache.put(sh, db.stateCache.hash(name), name, st, sh.epoch.Load())
					sh.mu.Unlock()
				}
				if err := db.PrepareMetadataCheckpointWithOptions(context.Background(), PrewarmOptions{Workers: workers}); err != nil {
					t.Fatal(err)
				}
				data, err := os.ReadFile(filepath.Join(dir, checkpointFile))
				if err != nil {
					t.Fatal(err)
				}
				if got := serialCheckpointRecords(t, data); !reflect.DeepEqual(got, want) {
					t.Fatal("parallel prepare differs from serial predecessor")
				}
				if err := db.Close(); err != nil {
					t.Fatal(err)
				}
				if magic == checkpointLegacyMagic {
					legacyCheckpoint(t, dir)
				}
				db, err = Open(dir, opts)
				if err != nil {
					t.Fatal(err)
				}
				defer db.Close()
				progress := new(PrewarmProgress)
				stats, err := db.LoadMetadataCheckpointAfter(context.Background(), PrewarmOptions{Workers: workers, Progress: progress, Batch: 17})
				if err != nil || !stats.Valid || stats.Entries != int64(len(want)) {
					t.Fatal(stats, err)
				}
				if magic == checkpointMagic && (stats.Fenced != stats.Entries || stats.Validated != 0) || magic == checkpointLegacyMagic && (stats.Validated != stats.Entries || stats.Fenced != 0) {
					t.Fatal(stats)
				}
				if work, total := progress.Snapshot(); work != uint64(len(data)) || total != work || progress.CheckpointLoading() {
					t.Fatal("incomplete progress", work, total)
				}
				for name, record := range want {
					sh := &db.stateCache.shards[db.metricStripe(name)]
					_, slot := sh.find(db.stateCache.hash(name), name)
					if slot < 0 || sh.slots[slot].epoch&provisionalState != 0 || !bytes.Equal(sh.record(slot), record) {
						t.Fatal("parallel load differs from serial records", name)
					}
				}
			})
		}
	}
}

func TestCheckpointBlockBoundariesAndErrors(t *testing.T) {
	frame := func(n int) []byte {
		buf := binary.LittleEndian.AppendUint32(nil, uint32(n))
		buf = binary.LittleEndian.AppendUint32(buf, 0)
		return append(buf, make([]byte, n)...)
	}
	for _, size := range []int{1, checkpointBlockSize - 9, checkpointBlockSize - 8, checkpointBlockSize - 7, 2 * checkpointBlockSize} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			data := append(frame(size), make([]byte, 8)...)
			r := bytes.NewReader(data)
			buf, last, err := readCheckpointBlock(r, make([]byte, checkpointBlockSize))
			if err != nil || !bytes.Equal(buf, data[:len(buf)]) {
				t.Fatal(len(buf), last, err)
			}
			if !last {
				buf, last, err = readCheckpointBlock(r, buf)
				if err != nil || !last || len(buf) != 8 {
					t.Fatal(len(buf), last, err)
				}
			}
		})
	}
	for _, data := range [][]byte{nil, {1}, frame(100)[:99], frame(10), binary.LittleEndian.AppendUint32(make([]byte, 0, 8), maxCheckpointRecordSize+1)} {
		if _, _, err := readCheckpointBlock(bytes.NewReader(data), make([]byte, checkpointBlockSize)); err == nil {
			t.Fatal("accepted incomplete stream")
		}
	}
	badSize := append(binary.LittleEndian.AppendUint32(nil, maxCheckpointRecordSize+1), make([]byte, 4)...)
	if _, _, err := readCheckpointBlock(bytes.NewReader(badSize), make([]byte, checkpointBlockSize)); err == nil {
		t.Fatal("accepted oversized record")
	}
}

func TestParallelCheckpointSaveErrorJoinsWorkers(t *testing.T) {
	dir := t.TempDir()
	seedCheckpoint(t, dir)
	db, err := Open(dir, checkpointOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.LoadMetadataCheckpoint(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	want := errors.New("injected checkpoint write failure")
	var active atomic.Int64
	options := PrewarmOptions{Workers: 8, CheckpointWorker: func() CheckpointPacer {
		active.Add(1)
		return checkpointTestPacer{finish: func() { active.Add(-1) }}
	}}
	done := make(chan error, 1)
	go func() { done <- db.saveCheckpointRecords(context.Background(), checkpointErrorWriter{want}, options) }()
	select {
	case err := <-done:
		if !errors.Is(err, want) || active.Load() != 0 {
			t.Fatal(err, active.Load())
		}
	case <-time.After(time.Minute):
		t.Fatal("save error stranded producers")
	}
}

type checkpointErrorWriter struct{ err error }

func (w checkpointErrorWriter) Write([]byte) (int, error) { return 0, w.err }

type checkpointShortWriter struct{}

func (checkpointShortWriter) Write(p []byte) (int, error) { return len(p) - 1, nil }

type checkpointErrorReader struct{ err error }

func (r checkpointErrorReader) Read([]byte) (int, error) { return 0, r.err }

func TestCheckpointPipelineIOErrors(t *testing.T) {
	dir := t.TempDir()
	seedCheckpoint(t, dir)
	db, err := Open(dir, checkpointOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.LoadMetadataCheckpoint(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	if err := db.saveCheckpointRecords(context.Background(), checkpointShortWriter{}, PrewarmOptions{Workers: 8}); !errors.Is(err, io.ErrShortWrite) {
		t.Fatal("short write not reported", err)
	}
	want := errors.New("injected checkpoint read failure")
	var active atomic.Int64
	options := PrewarmOptions{Workers: 8, CheckpointWorker: func() CheckpointPacer {
		active.Add(1)
		return checkpointTestPacer{finish: func() { active.Add(-1) }}
	}}
	stats := new(MetadataCheckpointStats)
	if err := db.loadCheckpointRecords(context.Background(), checkpointErrorReader{want}, options, stats); !errors.Is(err, want) || active.Load() != 0 {
		t.Fatal(err, active.Load())
	}
}

func TestParallelCheckpointWorkerStop(t *testing.T) {
	for _, prepare := range []bool{false, true} {
		t.Run(fmt.Sprint(prepare), func(t *testing.T) {
			dir := t.TempDir()
			seedCheckpoint(t, dir)
			db, err := Open(dir, checkpointOptions())
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			if prepare {
				if _, err := db.LoadMetadataCheckpoint(context.Background(), nil); err != nil {
					t.Fatal(err)
				}
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			entered := make(chan struct{}, 8)
			var active atomic.Int64
			options := PrewarmOptions{Workers: 8, CheckpointWorker: func() CheckpointPacer {
				active.Add(1)
				return checkpointTestPacer{wait: func(ctx context.Context) error { entered <- struct{}{}; <-ctx.Done(); return ctx.Err() }, finish: func() { active.Add(-1) }}
			}}
			done := make(chan error, 1)
			go func() {
				if prepare {
					done <- db.PrepareMetadataCheckpointWithOptions(ctx, options)
				} else {
					_, err := db.LoadMetadataCheckpointAfter(ctx, options)
					done <- err
				}
			}()
			<-entered
			if prepare {
				cancel()
			} else {
				db.StopBackground()
			}
			select {
			case err := <-done:
				if !errors.Is(err, context.Canceled) || active.Load() != 0 {
					t.Fatal(err, active.Load())
				}
			case <-time.After(time.Minute):
				t.Fatal("stop left a checkpoint worker running")
			}
			if _, err := os.Stat(filepath.Join(dir, checkpointFile+".tmp")); !os.IsNotExist(err) {
				t.Fatal("temporary checkpoint survived cancellation", err)
			}
		})
	}
}

// An unknown future format follows the predecessor's safe refusal path.
func TestCheckpointUnknownFormatFallback(t *testing.T) {
	dir := t.TempDir()
	seedCheckpoint(t, dir)
	path := filepath.Join(dir, checkpointFile)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	copy(data[:8], "GCSTATE9")
	binary.LittleEndian.PutUint32(data[48:], crc32.ChecksumIEEE(data[:48]))
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	db, err := Open(dir, checkpointOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	stats, err := db.LoadMetadataCheckpointAfter(context.Background(), PrewarmOptions{Workers: 8})
	if err != nil || stats.Valid || stats.Entries != 0 {
		t.Fatal(stats, err)
	}
	if _, err := db.Metadata(context.Background(), "a"); err != nil {
		t.Fatal("durable fallback failed", err)
	}
}

// Function-backed pacer for cancellation and cleanup fault injection.
type checkpointTestPacer struct {
	wait   func(context.Context) error
	finish func()
}

func (p checkpointTestPacer) Wait(ctx context.Context) error {
	if p.wait != nil {
		return p.wait(ctx)
	}
	return ctx.Err()
}
func (p checkpointTestPacer) Idle(ctx context.Context) error { return ctx.Err() }
func (p checkpointTestPacer) Finish()                        { p.finish() }
