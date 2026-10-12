package carbon

import (
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/cache"
	"github.com/go-graphite/go-carbon/points"
)

func tailFixture(t testing.TB, metrics, rounds int) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "input.1.2.tail.bin")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	var raw []byte
	for round := range rounds {
		for i := range metrics {
			// Repeated timestamps exercise last-write-wins corrections; batches also
			// contain out-of-order points that must remain in their original order.
			p := points.OnePoint(fmt.Sprintf("tail.metric.%d", i), float64(round), 1700000000)
			p.Add(float64(i), 1699999999)
			raw = p.AppendBinary(raw[:0])
			if _, err := f.Write(raw); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestReplayInputTailMatchesSerial(t *testing.T) {
	path := tailFixture(t, 257, 19)
	want := make(map[string][]points.Point)
	if err := points.ReadFromFile(path, func(p *points.Points) {
		want[p.Metric] = append(want[p.Metric], p.Data...)
	}); err != nil {
		t.Fatal(err)
	}
	for _, workers := range []int{1, 2, 8, 32} {
		t.Run(fmt.Sprint(workers), func(t *testing.T) {
			core := cache.New()
			// Recovered input must still bypass live admission limits.
			core.SetMaxSize(1)
			count, err := replayInputTail(path, workers, core.AddRecovered)
			if err != nil {
				t.Fatal(err)
			}
			if count != 257*19*2 {
				t.Fatal("point count", count)
			}
			for name, expected := range want {
				if got := core.Get(name); !reflect.DeepEqual(got, expected) {
					t.Fatalf("%s: got %v, want %v", name, got, expected)
				}
			}
		})
	}
}

func TestReplayInputTailDrainsWorkersOnError(t *testing.T) {
	path := filepath.Join(t.TempDir(), "input.1.2.tail.bin")
	first, second := "first", "second"
	for crc32.ChecksumIEEE([]byte(first))%2 == crc32.ChecksumIEEE([]byte(second))%2 {
		second += "x"
	}
	raw := points.OnePoint(first, 1, 1).AppendBinary(nil)
	raw = points.OnePoint(second, 2, 1).AppendBinary(raw)
	raw = append(raw, 0x80) // truncated next record
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	release := make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	progressed := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		count, err := replayInputTail(path, 2, func(p *points.Points) {
			if p.Metric == first {
				<-release
			} else {
				close(progressed)
			}
		})
		if count != 2 {
			t.Errorf("point count = %d", count)
		}
		done <- err
	}()
	select {
	case <-progressed:
	case <-time.After(5 * time.Second):
		t.Fatal("independent metric blocked behind another worker")
	}
	select {
	case err := <-done:
		t.Fatalf("returned before queued write finished: %v", err)
	default:
	}
	once.Do(func() { close(release) })
	select {
	case err := <-done:
		if !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Fatalf("read error = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("replay workers did not drain")
	}
}

func BenchmarkReplayInputTail(b *testing.B) {
	path := tailFixture(b, 100000, 5)
	for _, workers := range []int{1, 8, 16, 32} {
		b.Run(fmt.Sprint(workers), func(b *testing.B) {
			for b.Loop() {
				core := cache.New()
				count, err := replayInputTail(path, workers, core.AddRecovered)
				if err != nil || count != 1000000 {
					b.Fatalf("%d points: %v", count, err)
				}
			}
		})
	}
}
