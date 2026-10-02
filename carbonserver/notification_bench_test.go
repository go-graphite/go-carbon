package carbonserver

import (
	"bufio"
	"fmt"
	"os"
	"testing"

	"github.com/go-graphite/go-carbon/cache"
	"github.com/go-graphite/go-carbon/points"
)

func BenchmarkColdNotificationReplay(b *testing.B) {
	names := make([]string, 10000)
	for i := range names {
		names[i] = fmt.Sprintf("namespace.service%d.host%d.counter%d.value", i/1000, i/10, i%10)
	}
	if path := os.Getenv("NOTIFY_METRIC_SAMPLE"); path != "" {
		f, err := os.Open(path)
		if err != nil {
			b.Fatal(err)
		}
		defer f.Close()
		s := bufio.NewScanner(f)
		names = nil
		for s.Scan() {
			names = append(names, s.Text())
		}
		if err := s.Err(); err != nil {
			b.Fatal(err)
		}
	}
	for _, mode := range []string{"draining", "blocked", "new"} {
		b.Run(mode, func(b *testing.B) {
			l := NewCarbonserverListener(nil)
			l.SetTrieIndex(true)
			l.SetConcurrentIndex(true)
			trie := newTrie(".wsp", 0, nil)
			for _, name := range names {
				l.insertRealtimeMetric(trie, name)
			}
			l.UpdateFileIndex(&fileIndex{trieIdx: trie})
			var attempts float64
			b.ReportAllocs()
			b.ResetTimer()
			for round := 0; round < b.N; round++ {
				b.StopTimer()
				c := cache.New()
				c.SetMaxSize(0)
				c.SetBloomSize(uint64(len(names) * 20))
				ch := make(chan string, 1)
				c.SetNewMetricsChan(ch)
				// Allows exactly the same harness to run against the unmodified baseline.
				if target, ok := interface{}(c).(interface{ SetMetricExists(func(string) bool) }); ok {
					if index, ok := interface{}(l).(interface{ MetricExists(string) bool }); ok {
						target.SetMetricExists(index.MetricExists)
					}
				}
				if mode == "blocked" {
					ch <- "queue.full"
				}
				b.StartTimer()
				for pass := 0; pass < 10; pass++ {
					for i, name := range names {
						if mode == "new" {
							name = fmt.Sprintf("fresh%d.%d.%d", round, pass, i)
						}
						c.Add(points.OnePoint(name, 1, 1))
						c.Pop(name)
						if mode != "blocked" {
							select {
							case m := <-ch:
								l.insertRealtimeMetric(trie, m)
								attempts++
							default:
							}
						}
					}
				}
				b.StopTimer()
				c.Stat(func(name string, value float64) {
					if name == "droppedRealtimeIndex" {
						attempts += value
					}
				})
				b.StartTimer()
			}
			b.ReportMetric(attempts/float64(b.N*len(names)*10), "notifications/point")
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*len(names)*10), "ns/point")
		})
	}
}
