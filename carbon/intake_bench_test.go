package carbon

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/go-graphite/go-carbon/cache"
	"github.com/go-graphite/go-carbon/internal/recovery"
	"github.com/go-graphite/go-carbon/points"
)

// Include the real capacity check: BenchmarkReplayInputTail bypasses it.
// This synthetic fixture matches the incident's single-point tail records;
// it is not a captured production tail.
func intakeTailFixture(b testing.TB, records, metrics int, checkpoint bool, history, padding int) (*App, string) {
	b.Helper()
	suffix := strings.Repeat("x", padding)
	dir := b.TempDir()
	path := filepath.Join(dir, "input.1.2.tail.bin")
	f, err := os.Create(path)
	if err != nil {
		b.Fatal(err)
	}
	w := bufio.NewWriterSize(f, 1<<20)
	var raw []byte
	for i := range records {
		name := fmt.Sprintf("servers.host%05d.application.component.requests.by_region.region%02d.by_status.success.total%s", i%metrics, i%3, suffix)
		raw = points.OnePoint(name, float64(i), 1700000000+int64(i/metrics)).AppendBinary(raw[:0])
		if _, err := w.Write(raw); err != nil {
			b.Fatal(err)
		}
	}
	if err := w.Flush(); err != nil {
		b.Fatal(err)
	}
	if err := f.Close(); err != nil {
		b.Fatal(err)
	}
	app := New(TestConfig(b.TempDir()))
	if err := app.ParseConfig(); err != nil {
		b.Fatal(err)
	}
	app.Config.Dump.Enabled = true
	app.Config.Dump.Path = dir
	app.Config.Cache.MaxSize = 800000000
	// Keep the immutable tail owned by recovery, as on the incident hosts.
	app.pendingDone = make(chan struct{})
	if checkpoint {
		builder := recovery.NewBuilder(nil)
		writer, err := recovery.NewWriter(filepath.Join(dir, "cache.1.2.bin"), 0, 1<<20, builder)
		if err != nil {
			b.Fatal(err)
		}
		for i := range metrics {
			name := fmt.Sprintf("servers.host%05d.application.component.requests.by_region.region%02d.by_status.success.total%s", i, i%3, suffix)
			p := points.OnePoint(name, -1, 1699999999)
			for j := 1; j < history; j++ {
				p.Add(float64(-j), 1699999999-int64(j))
			}
			if err := writer.WritePoints(p); err != nil {
				b.Fatal(err)
			}
		}
		cacheFile, err := writer.Close()
		if err != nil {
			b.Fatal(err)
		}
		wal, err := recovery.NewWriter(filepath.Join(dir, "input.1.2.bin"), 1, 1<<20, builder)
		if err != nil {
			b.Fatal(err)
		}
		walFile, err := wal.Close()
		if err != nil {
			b.Fatal(err)
		}
		index, err := recovery.WriteIndex(dir, builder)
		if err != nil {
			b.Fatal(err)
		}
		if err := recovery.Publish(dir, dir, cacheFile, walFile, index, "bench"); err != nil {
			b.Fatal(err)
		}
	}
	return app, path
}

func BenchmarkIntakeTail(b *testing.B) {
	records := 50000
	if v := os.Getenv("GO_CARBON_TAIL_RECORDS"); v != "" {
		var err error
		records, err = strconv.Atoi(v)
		if err != nil || records <= 0 {
			b.Fatal("invalid GO_CARBON_TAIL_RECORDS")
		}
	}
	metrics := min(records, 100000)
	if v := os.Getenv("GO_CARBON_TAIL_METRICS"); v != "" {
		var err error
		metrics, err = strconv.Atoi(v)
		if err != nil || metrics <= 0 || metrics > records {
			b.Fatal("invalid GO_CARBON_TAIL_METRICS")
		}
	}
	checkpoint := os.Getenv("GO_CARBON_TAIL_CHECKPOINT") == "1"
	history := 11
	if v := os.Getenv("GO_CARBON_CHECKPOINT_POINTS"); v != "" {
		var err error
		history, err = strconv.Atoi(v)
		if err != nil || history <= 0 {
			b.Fatal("invalid GO_CARBON_CHECKPOINT_POINTS")
		}
	}
	padding := 0
	if v := os.Getenv("GO_CARBON_TAIL_METRIC_PADDING"); v != "" {
		var err error
		padding, err = strconv.Atoi(v)
		if err != nil || padding < 0 || padding > 10000 {
			b.Fatal("invalid GO_CARBON_TAIL_METRIC_PADDING")
		}
	}
	app, path := intakeTailFixture(b, records, metrics, checkpoint, history, padding)
	dir := filepath.Dir(path)
	for _, guarded := range []bool{true, false} {
		name := "unguarded"
		if guarded {
			name = "guarded"
		}
		b.Run(name, func(b *testing.B) {
			for b.Loop() {
				core := cache.New()
				core.SetMaxSize(app.Config.Cache.MaxSize)
				var bundle *recovery.Bundle
				if checkpoint {
					var err error
					bundle, err = recovery.OpenBundle(dir, dir)
					if err != nil {
						b.Fatal(err)
					}
					if err = core.AttachPendingRecovery(bundle); err != nil {
						b.Fatal(err)
					}
				}
				if guarded {
					if err := app.loadInputTails(core); err != nil {
						b.Fatal(err)
					}
				} else {
					count, err := replayInputTail(path, 32, core.AddRecovered)
					if err != nil || count != records {
						b.Fatalf("count %d: %v", count, err)
					}
				}
				want := int64(records)
				if checkpoint {
					// All saved names with the matching region are claimed by their
					// first tail write; the two other region variants have no history.
					want += int64(metrics * history)
				}
				if got := core.Size(); got != want {
					b.Fatalf("points %d != %d", got, want)
				}
				if bundle != nil {
					if err := bundle.Close(); err != nil {
						b.Fatal(err)
					}
				}
			}
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*records), "ns/point")
		})
	}
}
