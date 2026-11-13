package carbonserver

import (
	"fmt"
	"runtime"
	"slices"
	"sync"
	"testing"
	"time"
)

// Run with -benchtime=1x. Input construction is outside the timed section.
// Both variants insert all five million names; the old one blocks its producer
// on a full channel so the throughput comparison does not reward lost names.
func BenchmarkRealtimeBurst5M(b *testing.B) {
	const count = 5000000
	names := make([]string, count)
	for i := range names {
		names[i] = fmt.Sprintf("burst.group%04d.metric%06d", i/1000, i%1000)
	}
	for _, mode := range []string{"channel-single", "pending-batch"} {
		b.Run(mode, func(b *testing.B) {
			for range b.N {
				b.StopTimer()
				runtime.GC()
				l := NewCarbonserverListener(nil)
				l.SetWhisperData("/tmp")
				l.SetTrieIndex(true)
				l.SetConcurrentIndex(true)
				l.SetRealtimeIndex(1)
				ti := newTrie(".wsp", 0, nil)
				l.UpdateFileIndex(&fileIndex{trieIdx: ti})
				start := time.Now()
				b.StartTimer()
				if mode == "channel-single" {
					ch := make(chan string, 393216)
					done := make(chan struct{})
					go func() {
						for name := range ch {
							l.metricStoreIndexMu.Lock()
							path, ok := l.storableMetricPath(name)
							if ok {
								if _, err := ti.insert(path, 0, 0, 0, 0); err != nil {
									panic(err)
								}
							}
							l.metricStoreIndexMu.Unlock()
						}
						close(done)
					}()
					for _, name := range names {
						ch <- name
					}
					close(ch)
					<-done
				} else {
					q := l.newMetricsQueue
					done := make(chan struct{})
					go func() {
						for {
							<-q.Ready()
							for {
								l.metricStoreIndexMu.Lock()
								batch := q.Take(4096)
								slices.Sort(batch)
								for _, name := range batch {
									l.insertRealtimeMetric(ti, name)
								}
								q.Ack(batch)
								l.metricStoreIndexMu.Unlock()
								if len(batch) == 0 {
									break
								}
							}
							if ti.getFileCount() == count {
								close(done)
								return
							}
						}
					}()
					for _, name := range names {
						q.Add(name)
					}
					<-done
				}
				b.StopTimer()
				elapsed := time.Since(start)
				if ti.getFileCount() != count {
					b.Fatalf("inserted=%d", ti.getFileCount())
				}
				b.ReportMetric(float64(count)/elapsed.Seconds(), "names/s")
			}
		})
	}
}

func BenchmarkRealtimeInsertLock(b *testing.B) {
	for _, batchSize := range []int{1, 4096} {
		b.Run(fmt.Sprint(batchSize), func(b *testing.B) {
			names := make([]string, b.N)
			for i := range names {
				names[i] = fmt.Sprintf("burst.group%04d.metric%06d", i/1000, i%1000)
			}
			l := NewCarbonserverListener(nil)
			l.SetWhisperData("/tmp")
			ti := newTrie(".wsp", 0, nil)
			var mu sync.Mutex
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < len(names); i += batchSize {
				mu.Lock()
				for _, name := range names[i:min(i+batchSize, len(names))] {
					l.insertRealtimeMetric(ti, name)
				}
				mu.Unlock()
			}
		})
	}
}
