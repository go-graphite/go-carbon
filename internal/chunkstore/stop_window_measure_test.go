package chunkstore

import (
	"fmt"
	"math/rand/v2"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
)

// TestMeasureStopWindow replays a dump stop's store work: writers fill the
// memtables as a persister does, keep writing while the stop warms names,
// stop (the persister stopping before the dump), the dump runs, then input
// closes and the flush and close run, then the successor opens the store.
// "late" flushes only after input closed, as before; "early" flushes when
// the stop begins and when the writers stop, leaving the remainder. Run it
// explicitly, with the memtable size in MB:
//
//	STORE_MEASURE=256 go test -run TestMeasureStopWindow -v ./internal/chunkstore/
func TestMeasureStopWindow(t *testing.T) {
	mb, _ := strconv.Atoi(os.Getenv("STORE_MEASURE"))
	if mb <= 0 {
		t.Skip("set STORE_MEASURE=memtable MB to run")
	}
	for _, early := range []bool{false, true} {
		dir := t.TempDir()
		opts := Options{MemTableSize: uint64(mb) << 20, SyncInterval: time.Second}
		s, err := Open(dir, opts)
		if err != nil {
			t.Fatal(err)
		}
		var written atomic.Int64
		write := func(stop <-chan struct{}, limit int64) {
			var wg sync.WaitGroup
			for w := range 8 {
				wg.Go(func() {
					r := rand.New(rand.NewPCG(uint64(w), uint64(time.Now().UnixNano())))
					value := make([]byte, 1024)
					for {
						select {
						case <-stop:
							return
						default:
						}
						if limit > 0 && written.Load() >= limit {
							return
						}
						b := s.db.NewBatch()
						for range 64 {
							for i := range value {
								value[i] = byte(r.Uint32() & 0x3f)
							}
							_ = b.Set(fmt.Appendf(nil, "k/%016x", r.Uint64()), value, nil)
						}
						if err := b.Commit(pebble.NoSync); err != nil {
							t.Error(err)
							return
						}
						written.Add(64 * 1040)
					}
				})
			}
			wg.Wait()
		}
		// Grow the memtables to their size, then fill the current one.
		write(nil, int64(4*mb)<<20)
		mem := s.db.Metrics().MemTable.Size
		var flushes []<-chan struct{}
		flush := func() {
			done, err := s.AsyncFlush()
			if err != nil {
				t.Fatal(err)
			}
			flushes = append(flushes, done)
		}
		if early {
			flush() // the stop begins
		}
		stop := make(chan struct{})
		time.AfterFunc(2*time.Second, func() { close(stop) }) // names warm
		write(stop, 0)
		if early {
			flush() // the persister stopped
		}
		time.Sleep(time.Second) // the dump, with input diverted
		closed := time.Now()    // input closes
		flush()
		<-flushes[len(flushes)-1]
		flushed := time.Now()
		if err = s.CloseForExit(); err != nil {
			t.Fatal(err)
		}
		ct := s.LastCloseTimings()
		closedStore := time.Now()
		s2, err := Open(dir, opts)
		if err != nil {
			t.Fatal(err)
		}
		opened := time.Now()
		t.Logf("early=%v: memtables %d MB at stop; with input closed: flush %v, close %v (pebble %v), successor open %v (pebble %v)",
			early, mem>>20, flushed.Sub(closed).Round(time.Millisecond), closedStore.Sub(flushed).Round(time.Millisecond),
			ct.PebbleSteps, opened.Sub(closedStore).Round(time.Millisecond), s2.OpenTimings().Pebble.Round(time.Millisecond))
		_ = s2.Close()
	}
}
