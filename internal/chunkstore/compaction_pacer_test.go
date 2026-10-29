package chunkstore

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
)

func TestCompactionPacerSharesTokens(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p := newCompactionPacer(10000)
		start := time.Now()
		var wg sync.WaitGroup
		for range 4 {
			wg.Go(func() {
				for remaining := 1000; remaining > 0; {
					n, paced := p.take(remaining)
					if !paced || n > 100 {
						t.Errorf("take = %d, %v", n, paced)
					}
					remaining -= n
				}
			})
		}
		wg.Wait()
		if elapsed := time.Since(start); elapsed < 390*time.Millisecond || elapsed > 391*time.Millisecond {
			t.Fatalf("shared 4000 bytes at 10000 B/s took %v", elapsed)
		}
		if p.wait.Load() == 0 {
			t.Fatal("missing wait time")
		}
	})
}

func TestCompactionPacerReloadWakesWriters(t *testing.T) {
	for _, rate := range []int64{0, 100000} {
		t.Run(fmt.Sprint(rate), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				p := newCompactionPacer(1)
				p.take(1)
				done := make(chan int, 1)
				go func() { n, _ := p.take(100); done <- n }()
				synctest.Wait()
				start := time.Now()
				p.setRate(rate)
				if n := <-done; n != 100 {
					t.Fatalf("reload took %d bytes", n)
				}
				if elapsed := time.Since(start); elapsed > time.Millisecond {
					t.Fatalf("reload left writer waiting for %v", elapsed)
				}
			})
		})
	}
}

func TestCompactionPacerBurstAndLowerRate(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p := newCompactionPacer(10 << 20)
		if n, _ := p.take(1 << 20); n != 64<<10 {
			t.Fatalf("large write acquired %d bytes", n)
		}
		time.Sleep(time.Second)
		if n, _ := p.take(1 << 20); n != 64<<10 {
			t.Fatalf("idle bucket acquired %d bytes", n)
		}
		p.setRate(1000)
		start := time.Now()
		for left := 100; left > 0; {
			n, _ := p.take(left)
			left -= n
		}
		if elapsed := time.Since(start); elapsed < 100*time.Millisecond || elapsed > 101*time.Millisecond {
			t.Fatalf("lowered rate took %v", elapsed)
		}
	})
}

func TestCompactionPacingFSSelectsCompactions(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		fs := newCompactionPacingFS(vfs.NewMem(), 10000)
		payload := bytes.Repeat([]byte("data"), 250)
		for _, c := range []struct{ name, reason string }{
			{"000001.log", ""}, {"MANIFEST-000002", ""},
			{"000003.sst", "flushing"}, {"ingest/stage.sst", "ingesting"},
			{"000004.sst", "compacting"},
		} {
			if err := fs.MkdirAll(fs.PathDir(c.name), 0755); err != nil {
				t.Fatal(err)
			}
			f, err := fs.Create(c.name)
			if err != nil {
				t.Fatal(err)
			}
			fs.tableCreated(pebble.TableCreateInfo{Path: c.name, Reason: c.reason})
			start := time.Now()
			n, err := f.Write(bytes.Clone(payload))
			if err != nil || n != len(payload) {
				t.Fatalf("%s write = %d, %v", c.name, n, err)
			}
			elapsed := time.Since(start)
			if c.reason == "compacting" {
				if elapsed < 90*time.Millisecond || elapsed > 91*time.Millisecond {
					t.Fatalf("compaction took %v", elapsed)
				}
			} else if elapsed != 0 {
				t.Fatalf("%s was paced for %v", c.name, elapsed)
			}
			if err := f.Close(); err != nil {
				t.Fatal(err)
			}
			r, err := fs.Open(c.name)
			if err != nil {
				t.Fatal(err)
			}
			got, err := io.ReadAll(r)
			_ = r.Close()
			if err != nil || !bytes.Equal(got, payload) {
				t.Fatalf("%s changed output: %v", c.name, err)
			}
		}
		if fs.pacer.bytes.Load() != uint64(len(payload)) || len(fs.files) != 0 {
			t.Fatalf("paced bytes=%d, open files=%d", fs.pacer.bytes.Load(), len(fs.files))
		}
		if fs.output.Load() != uint64(len(payload)) {
			t.Fatal("controller output count included flush, WAL or ingest bytes")
		}
		fs.pacer.setRate(0)
		if n, paced := fs.pacer.take(1 << 20); n != 1<<20 || paced {
			t.Fatalf("disabled take = %d, %v", n, paced)
		}
	})
}

type pacerShortFile struct {
	vfs.File
	err error
}

func (f pacerShortFile) Write(b []byte) (int, error) { return len(b) / 2, f.err }

func TestCompactionPacingFSWriteErrors(t *testing.T) {
	failed := errors.New("write failed")
	for _, err := range []error{nil, failed} {
		fs := newCompactionPacingFS(vfs.NewMem(), 100000)
		f := &compactionPacingFile{File: pacerShortFile{err: err}, fs: fs}
		f.compacting.Store(true)
		n, got := f.Write(make([]byte, 100))
		want := err
		if want == nil {
			want = io.ErrShortWrite
		}
		if n != 50 || !errors.Is(got, want) || fs.pacer.bytes.Load() != 50 {
			t.Fatalf("write = %d, %v; paced=%d", n, got, fs.pacer.bytes.Load())
		}
	}
}

func TestCompactionRateRejectsInvalid(t *testing.T) {
	t.Parallel()
	if db, err := Open(t.TempDir(), Options{CompactionRate: -2}); err == nil {
		_ = db.Close()
		t.Fatal("negative rate accepted")
	}
	db, err := Open(t.TempDir(), Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.SetCompactionRate(-2); err == nil || db.Stats().CompactionRate != 0 {
		t.Fatal("negative reload changed the rate")
	}
}

func TestCompactionPacingStore(t *testing.T) {
	t.Parallel()
	for _, parallelSyncs := range []int{0, 2} {
		t.Run(fmt.Sprint(parallelSyncs), func(t *testing.T) {
			db, err := Open(t.TempDir(), Options{fs: vfs.NewMem(), ExactBlocks: true,
				CompactionRate: 1 << 20, ParallelTableSyncs: parallelSyncs, L0CompactionThreshold: 10, L0StopWritesThreshold: 20})
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			payload := bytes.Repeat([]byte("retained block"), 100)
			for generation := range 2 {
				for i := range 16 {
					key := fmt.Appendf(nil, "p%04d", i)
					if err := db.db.Set(key, append([]byte{byte(generation)}, payload...), pebble.NoSync); err != nil {
						t.Fatal(err)
					}
				}
				if err := db.Flush(); err != nil {
					t.Fatal(err)
				}
			}
			if st := db.Stats(); st.CompactionPacerBytes != 0 || st.CompactionPacerWaitNanos != 0 || st.FlushWriteBytes == 0 {
				t.Fatalf("flush was paced: %+v", st)
			}
			if err := db.Compact(); err != nil {
				t.Fatal(err)
			}
			st := db.Stats()
			if st.CompactionPacerBytes == 0 || st.CompactionPacerBytes != st.CompactionWriteBytes {
				t.Fatalf("compaction event did not pace all output: paced=%d compacted=%d", st.CompactionPacerBytes, st.CompactionWriteBytes)
			}
			for i := range 16 {
				got, closer, err := db.db.Get(fmt.Appendf(nil, "p%04d", i))
				if err != nil {
					t.Fatal(err)
				}
				equal := bytes.Equal(got, append([]byte{1}, payload...))
				_ = closer.Close()
				if !equal {
					t.Fatal("compaction changed a block")
				}
			}
		})
	}
}

func TestCompactionPacingCloseReleasesWrites(t *testing.T) {
	t.Parallel()
	db, err := Open(t.TempDir(), Options{fs: vfs.NewMem(), ExactBlocks: true,
		CompactionRate: 1, L0CompactionThreshold: 10, L0StopWritesThreshold: 20})
	if err != nil {
		t.Fatal(err)
	}
	closed := false
	defer func() {
		if !closed {
			_ = db.Close()
		}
	}()
	for generation := range 2 {
		if err := db.db.Set([]byte("p.block"), bytes.Repeat([]byte{byte(generation)}, 1024), pebble.NoSync); err != nil {
			t.Fatal(err)
		}
		if err := db.Flush(); err != nil {
			t.Fatal(err)
		}
	}
	compacted := make(chan error, 1)
	go func() { compacted <- db.Compact() }()
	deadline := time.Now().Add(time.Second)
	for {
		db.compactionPacing.mu.Lock()
		var compacting bool
		for _, f := range db.compactionPacing.files {
			compacting = compacting || f.compacting.Load()
		}
		db.compactionPacing.mu.Unlock()
		if compacting {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("compaction did not start")
		}
		time.Sleep(time.Millisecond)
	}
	err = db.Close()
	closed = true
	if err != nil {
		t.Fatal(err)
	}
	if err := <-compacted; err != nil && !errors.Is(err, pebble.ErrClosed) && !errors.Is(err, pebble.ErrCancelledCompaction) {
		t.Fatal(err)
	}
}
