//go:build darwin || linux

package chunkstore

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"
)

// Small, incompressible old blocks, with descriptors and a live tail in the
// same metric span. Each four-metric group copies 4 MiB. Driving groups directly
// scales the production 64 MiB boundary down without changing the LSM policy.
type spaceRelayoutCodec struct {
	relayoutCodec
	value []byte
}

func spaceGroupKey(i int) []byte { return binary.BigEndian.AppendUint32([]byte("c"), uint32(i)) }
func (c spaceRelayoutCodec) Create(_ context.Context, io ContinuationIO, _ Metadata, _ func() time.Time) error {
	if err := io.Set([]byte("b"), []byte("v6")); err != nil {
		return err
	}
	if err := io.Set([]byte("live"), []byte("tail")); err != nil {
		return err
	}
	for i := 0; i < 128; i++ {
		if err := io.Set(spaceGroupKey(i), c.value); err != nil {
			return err
		}
	}
	return nil
}
func (c spaceRelayoutCodec) RelayoutPlan(_ context.Context, io ContinuationIO) (RelayoutPlan, error) {
	old, err := io.Get([]byte("b"))
	if err != nil || string(old) != "v6" {
		return RelayoutPlan{}, err
	}
	b := RelayoutBlock{Descriptor: []byte("b"), Old: old, New: []byte("v7")}
	p := RelayoutPlan{}
	for i := 0; i < 128; i++ {
		k := spaceGroupKey(i)
		v, err := io.Get(k)
		if err != nil {
			return p, err
		}
		b.Removed = append(b.Removed, k)
		b.Copies = append(b.Copies, RelayoutKV{Key: ContinuationBucketKey(0, k), Value: v})
		p.Bytes += len(k) + len(v)
	}
	p.Blocks = []RelayoutBlock{b}
	return p, nil
}

type spaceSample struct {
	Stage                      string
	Disk, Directory            uint64
	Levels                     [7]int64
	PointDeletes, RangeDeletes uint64
	Debt                       uint64
}

func sampleSpace(t *testing.T, s *Store, stage string) spaceSample {
	t.Helper()
	m := s.db.Metrics()
	v := spaceSample{Stage: stage, Disk: m.DiskSpaceUsage(), Debt: m.Compact.EstimatedDebt}
	for i, l := range m.Levels {
		v.Levels[i] = l.Size
	}
	tables, err := s.db.SSTables(pebble.WithProperties())
	if err != nil {
		t.Fatal(err)
	}
	for _, level := range tables {
		for _, tab := range level {
			v.PointDeletes += tab.Properties.NumDeletions - tab.Properties.NumRangeDeletions
			v.RangeDeletes += tab.Properties.NumRangeDeletions
		}
	}
	err = filepath.Walk(s.dir, func(_ string, info os.FileInfo, err error) error {
		if os.IsNotExist(err) {
			return nil
		}
		if err == nil && !info.IsDir() {
			v.Directory += uint64(info.Size())
		}
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	b, _ := json.Marshal(v)
	t.Log(string(b))
	return v
}
func spaceCPU() time.Duration {
	var r syscall.Rusage
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &r)
	return time.Duration(r.Utime.Sec+r.Stime.Sec)*time.Second + time.Duration(r.Utime.Usec+r.Stime.Usec)*time.Microsecond
}

// Opt-in on-disk experiment: keep routine race runs small. The committed
// regression below uses the same L6 shape on an in-memory filesystem.
func TestRelayoutSpaceOptions(t *testing.T) {
	if os.Getenv("RELAYOUT_SPACE_MEASURE") == "" {
		t.Skip("set RELAYOUT_SPACE_MEASURE=1 for on-disk option measurements")
	}
	metrics := 64
	if raw := os.Getenv("RELAYOUT_SPACE_METRICS"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 64 || n > 256 || n%4 != 0 {
			t.Fatal("RELAYOUT_SPACE_METRICS must be a multiple of four in 64..256")
		}
		metrics = n
	}
	for _, mode := range []string{"point", "range", "sized", "compact", "implemented", "parallel"} {
		t.Run(mode, func(t *testing.T) {
			usage, err := vfs.Default.GetDiskUsage(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			if usage.AvailBytes < 25_000_000_000 {
				t.Fatal("stop: less than 25 GB free")
			}
			value := make([]byte, 8192)
			r := rand.New(rand.NewPCG(1, 2))
			for i := range value {
				value[i] = byte(r.Uint32())
			}
			codec := spaceRelayoutCodec{value: value}
			workers := 1
			if mode == "parallel" {
				workers = 4
			}
			s, _ := openTestStore(t, t.TempDir(), Options{RelayoutWorkers: workers, fs: vfs.Default, ExactBlocks: true, TimeBuckets: true, Continuation: codec, CacheSize: 4 << 20, MemTableSize: 4 << 20, TargetFileSize: 2 << 20})
			defer closeTestStore(t, s)
			var metas []Metadata
			for i := 0; i < metrics; i++ {
				metas = append(metas, createTestMetric(t, s, fmt.Sprintf("space.%04d", i)))
			}
			if err := s.db.Compact([]byte{0}, []byte{255}, false); err != nil {
				t.Fatal(err)
			}
			for deadline := time.Now().Add(2 * time.Second); time.Now().Before(deadline); {
				m := s.db.Metrics()
				if m.Table.ObsoleteCount == 0 && m.Table.ZombieCount == 0 {
					break
				}
				time.Sleep(time.Millisecond)
			}
			if mode == "sized" {
				if err := s.db.RatchetFormatMajorVersion(pebble.FormatDeleteSizedAndObsolete); err != nil {
					t.Fatal(err)
				}
			}
			before := sampleSpace(t, s, "before")
			if before.Levels[6] < 60<<20 {
				t.Fatal("fixture not in L6")
			}
			cpu, start := spaceCPU(), time.Now()
			ioBefore := s.fsStats.Stats()
			var peakStats *pebble.Metrics
			var peak atomic.Uint64
			peak.Store(before.Disk)
			stop, stopped := make(chan struct{}), make(chan struct{})
			go func() {
				defer close(stopped)
				ticker := time.NewTicker(time.Millisecond)
				defer ticker.Stop()
				for {
					select {
					case <-stop:
						return
					case <-ticker.C:
						m := s.db.Metrics()
						n := m.DiskSpaceUsage()
						for old := peak.Load(); n > old; old = peak.Load() {
							if peak.CompareAndSwap(old, n) {
								peakStats = m
								break
							}
						}
					}
				}
			}()
			var stopOnce sync.Once
			stopSampling := func() { stopOnce.Do(func() { close(stop); <-stopped }) }
			defer stopSampling()
			copied := 0
			if mode == "parallel" {
				if err := s.RelayoutContinuation(context.Background(), 0); err != nil {
					t.Fatal(err)
				}
				copied = int(s.RelayoutStats().Bytes)
			} else {
				for i := 0; i < len(metas); i += 4 {
					var items []relayoutItem
					for _, m := range metas[i : i+4] {
						p, err := codec.RelayoutPlan(context.Background(), s.newContinuationIO(context.Background(), s.db, nil, m))
						if err != nil {
							t.Fatal(err)
						}
						copied += p.Bytes
						items = append(items, relayoutItem{m: m, plan: p})
					}
					if mode == "implemented" {
						if err := s.relayoutGroup(context.Background(), items); err != nil {
							t.Fatal(err)
						}
					} else {
						// Same ingest/switch ordering, varying only deletion representation.
						path := s.fs.PathJoin(s.dir, "option.sst")
						f, err := s.fs.Create(path)
						if err != nil {
							t.Fatal(err)
						}
						w := sstable.NewWriter(objstorageprovider.NewFileWritable(f), s.pebbleOptions.MakeWriterOptions(6, s.db.FormatMajorVersion().MaxTableFormat()))
						for _, item := range items {
							for _, b := range item.plan.Blocks {
								for _, kv := range b.Copies {
									if err := w.Set(appendContinuationKey(nil, continuationPrefix(item.m.ID, item.m.Generation), kv.Key), kv.Value); err != nil {
										t.Fatal(err)
									}
								}
							}
						}
						if err := w.Close(); err != nil {
							t.Fatal(err)
						}
						if err := s.db.Ingest([]string{path}); err != nil {
							t.Fatal(err)
						}
						batch := s.db.NewBatch()
						for _, item := range items {
							prefix := continuationPrefix(item.m.ID, item.m.Generation)
							for _, b := range item.plan.Blocks {
								if err := batch.Set(appendContinuationKey(nil, prefix, b.Descriptor), b.New, nil); err != nil {
									t.Fatal(err)
								}
								if mode == "range" {
									lo := appendContinuationKey(nil, prefix, b.Removed[0])
									hi := append(appendContinuationKey(nil, prefix, b.Removed[len(b.Removed)-1]), 0)
									if err := batch.DeleteRange(lo, hi, nil); err != nil {
										t.Fatal(err)
									}
								} else {
									for j, k := range b.Removed {
										key := appendContinuationKey(nil, prefix, k)
										var err error
										if mode == "sized" {
											err = batch.DeleteSized(key, uint32(len(b.Copies[j].Value)), nil)
										} else {
											err = batch.Delete(key, nil)
										}
										if err != nil {
											t.Fatal(err)
										}
									}
								}
							}
						}
						if err := s.commit(batch); err != nil {
							t.Fatal(err)
						}
						_ = batch.Close()
					}
					if err := s.db.Flush(); err != nil {
						t.Fatal(err)
					}
					if mode == "compact" {
						lo := continuationPrefix(metas[i].ID, metas[i].Generation)
						hi := continuationPrefix(metas[i+3].ID+1, 0)
						if err := s.db.Compact(lo, hi, false); err != nil {
							t.Fatal(err)
						}
					}
					if i%16 == 12 {
						sampleSpace(t, s, fmt.Sprintf("after-%d-groups", i/4+1))
					}
				}
			}
			if mode == "implemented" {
				if err := s.reclaimRelayout(context.Background(), true, 0); err != nil {
					t.Fatal(err)
				}
			}
			// Allow automatic compactions and file deletion to finish, without forcing
			// compaction of the old span in the automatic-policy experiments.
			for deadline := time.Now().Add(time.Second); time.Now().Before(deadline); {
				if s.db.Metrics().Compact.NumInProgress == 0 {
					time.Sleep(20 * time.Millisecond)
					if s.db.Metrics().Compact.NumInProgress == 0 {
						break
					}
				}
				time.Sleep(time.Millisecond)
			}
			stopSampling()
			after := sampleSpace(t, s, "after")
			if (mode == "implemented" || mode == "parallel") && int64(after.Disk)-int64(before.Disk) > 8<<20 {
				t.Fatal("reclaimed path retained more than two 4 MiB groups")
			}
			ioAfter := s.fsStats.Stats()
			var read, written uint64
			for k := 0; k < numFSKinds; k++ {
				read += ioAfter.Bytes[FSRead][k] - ioBefore.Bytes[FSRead][k]
				written += ioAfter.Bytes[FSWrite][k] - ioBefore.Bytes[FSWrite][k]
			}
			if peakStats != nil {
				var levels [7]int64
				for i, l := range peakStats.Levels {
					levels[i] = l.Size
				}
				t.Logf("peak_levels=%v peak_obsolete_bytes=%d peak_zombie_bytes=%d peak_compaction_output_bytes=%d peak_wal_bytes=%d", levels, peakStats.Table.ObsoleteSize, peakStats.Table.ZombieSize, peakStats.Compact.InProgressBytes, peakStats.WAL.PhysicalSize)
			}
			t.Logf("mode=%s copied=%d growth=%d peak_growth=%d elapsed=%s cpu=%s fs_read=%d fs_write=%d", mode, copied, int64(after.Disk)-int64(before.Disk), int64(peak.Load())-int64(before.Disk), time.Since(start), spaceCPU()-cpu, read, written)
		})
	}
}

// Exercise real ingestion, descriptor switching and deletion all the way to L6
// after every group, rather than relying on an eventual full-store compaction.
func TestRelayoutReclaimsL6WhileRunning(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, codec, metas := relayoutSpaceFixture(t, 1)
	liveBytes := func() int64 {
		var n int64
		for _, l := range s.db.Metrics().Levels {
			n += l.Size
		}
		return n
	}
	before := liveBytes()
	if s.db.Metrics().Levels[6].Size < 4<<20 {
		t.Fatal("old blocks not in L6")
	}
	for i := 0; i < len(metas); i += 2 {
		var items []relayoutItem
		for _, m := range metas[i : i+2] {
			p, err := codec.RelayoutPlan(ctx, s.newContinuationIO(ctx, s.db, nil, m))
			if err != nil {
				t.Fatal(err)
			}
			items = append(items, relayoutItem{m: m, plan: p})
		}
		if err := s.relayoutGroup(ctx, items); err != nil {
			t.Fatal(err)
		}
		// 16 groups copy 32 MiB in all. Allow two 2 MiB groups for table
		// framing/overlap, while the unfixed path retains the whole 32 MiB.
		if growth := liveBytes() - before; growth > 6<<20 {
			t.Fatalf("group %d: growth=%d, want <= configured batch plus overlap", i/2, growth)
		}
		for _, m := range metas[i : i+2] {
			io := s.newContinuationIO(ctx, s.db, nil, m)
			if v, err := io.Get([]byte("live")); err != nil || string(v) != "tail" {
				t.Fatalf("live tail: %q %v", v, err)
			}
			for k := 0; k < 128; k++ {
				if _, err := io.Get(spaceGroupKey(k)); !errors.Is(err, ErrNotFound) {
					t.Fatalf("old key: %v", err)
				}
				if v, err := io.Get(ContinuationBucketKey(0, spaceGroupKey(k))); err != nil || !bytes.Equal(v, codec.value) {
					t.Fatalf("copy differs: %v", err)
				}
			}
		}
	}
	if err := s.reclaimRelayout(ctx, true, 0); err != nil {
		t.Fatal(err)
	}
	tables, err := s.db.SSTables(pebble.WithProperties())
	if err != nil {
		t.Fatal(err)
	}
	for _, level := range tables {
		for _, tab := range level {
			if tab.Properties.NumDeletions != 0 {
				t.Fatalf("tombstones left after reclamation: %d", tab.Properties.NumDeletions)
			}
		}
	}
	if s.db.FormatMajorVersion() != pebble.FormatMostCompatible {
		t.Fatalf("reclamation upgraded Pebble format: %v", s.db.FormatMajorVersion())
	}
}

// relayoutCancelFS observes the first compaction output, after TableCreated has
// marked it in the existing pacing layer. It can hold a write for the ordering
// test or cancel immediately without delaying IO for the latency measurement.
type relayoutCancelFS struct {
	vfs.FS
	store    atomic.Pointer[Store]
	armed    atomic.Bool
	onOutput func()
}

type relayoutCancelFile struct {
	vfs.File
	fs   *relayoutCancelFS
	name string
}

func (fs *relayoutCancelFS) Create(name string) (vfs.File, error) {
	f, err := fs.FS.Create(name)
	if err != nil || fsKind(name) != FSTable {
		return f, err
	}
	return &relayoutCancelFile{File: f, fs: fs, name: filepath.Clean(name)}, nil
}

func (f *relayoutCancelFile) Write(p []byte) (int, error) {
	if f.fs.armed.Load() {
		if s := f.fs.store.Load(); s != nil {
			s.compactionPacing.mu.Lock()
			output := s.compactionPacing.files[f.name]
			compacting := output != nil && output.compacting.Load()
			s.compactionPacing.mu.Unlock()
			if compacting && f.fs.armed.CompareAndSwap(true, false) {
				f.fs.onOutput()
			}
		}
	}
	return f.File.Write(p)
}

func TestRelayoutCancelDuringReclaim(t *testing.T) {
	t.Parallel()
	entered, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	releaseWrite := func() { releaseOnce.Do(func() { close(release) }) }
	fs := &relayoutCancelFS{FS: vfs.NewMem(), onOutput: func() { close(entered); <-release }}
	// Each metric forces a separate group, so cancellation must also stop the
	// next copy, not merely leave the cursor behind a completed whole run.
	s, _ := openTestStore(t, "/store", Options{fs: fs, ExactBlocks: true, TimeBuckets: true, Continuation: &controlledRelayoutCodec{}, RelayoutReclaimBytes: 1})
	fs.store.Store(s)
	m := createTestMetric(t, s, "cancel.first")
	next := createTestMetric(t, s, "cancel.second")
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	joined := false
	defer func() {
		cancel()
		releaseWrite()
		if !joined {
			<-done
		}
		closeTestStore(t, s)
	}()
	fs.armed.Store(true)
	go func() { done <- s.RelayoutContinuation(ctx, 0) }()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("reclaim did not reach a compaction output")
	}
	unlocked := make(chan struct{})
	go func() { unlock := s.lockMetricMutation(m.Name); unlock(); close(unlocked) }()
	select {
	case <-unlocked:
	case <-time.After(time.Second):
		t.Fatal("reclamation held metric lock")
	}
	if cursor, done, err := s.relayoutCursor(); err != nil || cursor != 0 || done {
		t.Fatalf("cursor passed unreclaimed group: %d %v %v", cursor, done, err)
	}
	cancel()
	// Stock Compact remains synchronous even after cancellation. Its current
	// write must finish before the worker returns; no detached work survives.
	select {
	case err := <-done:
		joined = true
		t.Fatalf("worker detached from in-flight compaction: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	releaseWrite()
	select {
	case err := <-done:
		joined = true
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("worker did not return after its current compaction")
	}
	if st := s.RelayoutStats(); st.Done || st.Cursor != 0 || st.Paused || st.Blocks != 1 {
		t.Fatalf("cancel: %+v", st)
	}
	if v, err := s.newContinuationIO(context.Background(), s.db, nil, next).Get([]byte("b")); err != nil || string(v) != "v6" {
		t.Fatalf("next group started after cancellation: %q %v", v, err)
	}
	if err := s.RelayoutContinuation(context.Background(), 0); err != nil {
		t.Fatal(err)
	}
	io := s.newContinuationIO(context.Background(), s.db, nil, m)
	if v, err := io.Get(ContinuationBucketKey(0, []byte("c"))); err != nil || string(v) != "group" {
		t.Fatalf("resume lost copy: %q %v", v, err)
	}
}

// Unlike the ordering test, this uses real on-disk IO without holding or
// slowing a write: cancel at the first compaction output and time the join.
func TestRelayoutCancellationLatency(t *testing.T) {
	if os.Getenv("RELAYOUT_SPACE_MEASURE") == "" {
		t.Skip("set RELAYOUT_SPACE_MEASURE=1 for on-disk cancellation latency")
	}
	for _, workers := range []int{1, 4} {
		t.Run(fmt.Sprintf("K=%d", workers), func(t *testing.T) {
			dir := t.TempDir()
			usage, err := vfs.Default.GetDiskUsage(dir)
			if err != nil {
				t.Fatal(err)
			}
			if usage.AvailBytes < 25_000_000_000 {
				t.Fatal("stop: less than 25 GB free")
			}
			value := make([]byte, 8192)
			r := rand.New(rand.NewPCG(1, 2))
			for i := range value {
				value[i] = byte(r.Uint32())
			}
			codec := spaceRelayoutCodec{value: value}
			ctx, cancel := context.WithCancel(context.Background())
			cancelledAt := make(chan time.Time, 1)
			fs := &relayoutCancelFS{FS: vfs.Default, onOutput: func() { at := time.Now(); cancel(); cancelledAt <- at }}
			s, _ := openTestStore(t, dir, Options{fs: fs, Continuation: codec, TimeBuckets: true, ExactBlocks: true, RelayoutWorkers: workers, CacheSize: 4 << 20, MemTableSize: 4 << 20, TargetFileSize: 2 << 20})
			fs.store.Store(s)
			for i := 0; i < 64; i++ {
				createTestMetric(t, s, fmt.Sprintf("latency.%04d", i))
			}
			if err := s.db.Compact([]byte{0}, []byte{255}, false); err != nil {
				t.Fatal(err)
			}
			tables, err := s.db.SSTables()
			if err != nil {
				t.Fatal(err)
			}
			var maxL6 uint64
			for _, tab := range tables[6] {
				maxL6 = max(maxL6, tab.Size)
			}
			type result struct {
				err error
				at  time.Time
			}
			done := make(chan result, 1)
			joined := false
			defer func() {
				cancel()
				if !joined {
					<-done
				}
				closeTestStore(t, s)
			}()
			fs.armed.Store(true)
			go func() { err := s.RelayoutContinuation(ctx, 0); done <- result{err: err, at: time.Now()} }()
			var at time.Time
			select {
			case at = <-cancelledAt:
			case <-time.After(10 * time.Second):
				t.Fatal("relayout never reached compaction output")
			}
			var finished result
			select {
			case finished = <-done:
				joined = true
			case <-time.After(10 * time.Second):
				t.Fatal("bounded compaction did not return")
			}
			if !errors.Is(finished.err, context.Canceled) {
				t.Fatalf("cancel: %v", finished.err)
			}
			stats := s.RelayoutStats()
			if stats.Done || stats.UnreclaimedBytes == 0 {
				t.Fatalf("cancelled batch lost its durable obligation: %+v", stats)
			}
			t.Logf("workers=%d cancel_to_worker_return=%s copied=%d blocks=%d l6_max_file_bytes=%d l6_target_file_bytes=%d", workers, finished.at.Sub(at), stats.Bytes, stats.Blocks, maxL6, s.pebbleOptions.Levels[6].TargetFileSize)
		})
	}
}

func relayoutSpaceFixture(t *testing.T, workers int) (*Store, spaceRelayoutCodec, []Metadata) {
	t.Helper()
	value := make([]byte, 8192)
	r := rand.New(rand.NewPCG(3, 4))
	for i := range value {
		value[i] = byte(r.Uint32())
	}
	codec := spaceRelayoutCodec{value: value}
	s, _ := openTestStore(t, "/store", Options{fs: vfs.NewMem(), Continuation: codec, TimeBuckets: true, ExactBlocks: true, MemTableSize: 1 << 20, TargetFileSize: 128 << 10, RelayoutReclaimBytes: 5 << 20, RelayoutWorkers: workers, CacheSize: 1 << 20})
	t.Cleanup(func() { closeTestStore(t, s) })
	var metas []Metadata
	for i := 0; i < 32; i++ {
		metas = append(metas, createTestMetric(t, s, fmt.Sprintf("bounded.%03d", i)))
	}
	if err := s.db.Compact([]byte{0}, []byte{255}, false); err != nil {
		t.Fatal(err)
	}
	return s, codec, metas
}

func TestRelayoutParallelReclaimsL6(t *testing.T) {
	t.Parallel()
	s, codec, metas := relayoutSpaceFixture(t, 4)
	before := s.db.Metrics().Levels[6].Size
	if before < 30<<20 {
		t.Fatal("fixture old data not in L6")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := s.RelayoutContinuation(ctx, 0); err != nil {
		t.Fatal(err)
	}
	var live int64
	for _, l := range s.db.Metrics().Levels {
		live += l.Size
	}
	if growth := live - before; growth > 4<<20 {
		t.Fatalf("parallel growth %d, want at most four 1 MiB groups (32 MiB copied)", growth)
	}
	if st := s.RelayoutStats(); !st.Done || st.Blocks != uint64(len(metas)) || st.Cursor != metas[len(metas)-1].ID {
		t.Fatalf("parallel completion: %+v", st)
	}
	for _, m := range metas {
		io := s.newContinuationIO(ctx, s.db, nil, m)
		for _, k := range []int{0, 127} {
			if _, err := io.Get(spaceGroupKey(k)); !errors.Is(err, ErrNotFound) {
				t.Fatalf("old group: %v", err)
			}
			if v, err := io.Get(ContinuationBucketKey(0, spaceGroupKey(k))); err != nil || !bytes.Equal(v, codec.value) {
				t.Fatalf("parallel copy differs: %v", err)
			}
		}
	}
}
