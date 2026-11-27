package v2daemon

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2core"
	"github.com/go-graphite/go-carbon/internal/v2part"
	"github.com/go-graphite/go-carbon/points"
)

// Opt-in real daemon/journal/core fixture; the normal focused gate skips it.
func TestRead15PartAccumulation(t *testing.T) {
	d, err := time.ParseDuration(os.Getenv("V2_READ15_DURATION"))
	if err != nil {
		t.Skip("set V2_READ15_DURATION=10m")
	}
	root := t.TempDir()
	s, err := Open(root, Config{Layer: 3, Producers: 1, Shards: 16, MaxCoreRows: 256, GroupInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	var apps []Application
	var chosen [16]bool
	for i := 0; len(apps) < 16; i++ {
		name := fmt.Sprintf("measure.live.%d", i)
		owner := int(v2part.Route(v2part.ID(name))) % 16
		if chosen[owner] {
			continue
		}
		chosen[owner] = true
		policy, err := v2core.EncodeRollupPolicy(v2core.RollupPolicy{Name: v2part.ID(name), Group: "test", Config: sealrollup.Config{Method: sealrollup.Last, Archives: []sealrollup.Archive{{Step: 1, Count: 86400}}}})
		if err != nil {
			t.Fatal(err)
		}
		apps = append(apps, Application{Name: name, Policy: policy, Points: []points.Point{{Value: 42}}, Sequence: []uint64{0}})
	}
	start := time.Now()
	next := start
	sample := start
	var seals uint64
	for time.Since(start) < d {
		seals++
		now := time.Now().Unix()
		for i := range apps {
			apps[i].Order = seals*16 + uint64(i)
			apps[i].Clock = now
			apps[i].Points[0].Timestamp = now - 1
			apps[i].Sequence[0] = seals
		}
		if err := s.Producers[0].AppendApplications(apps); err != nil {
			t.Fatal(err)
		}
		if err := s.Producers[0].Seal(); err != nil {
			t.Fatal(err)
		}
		if err := s.Producers[0].Drain(); err != nil {
			t.Fatal(err)
		}
		if !time.Now().Before(sample) || time.Since(start) >= d {
			entries, err := filepath.Glob(filepath.Join(root, "placement-group-00000001", "core", "*", "parts", "*.sst"))
			if err != nil {
				t.Fatal(err)
			}
			var m runtime.MemStats
			runtime.ReadMemStats(&m)
			fmt.Printf("READ15 elapsed=%.3f seal_rounds=%d live_files=%d heap=%d\n", time.Since(start).Seconds(), seals, len(entries), m.HeapAlloc)
			sample = sample.Add(30 * time.Second)
		}
		next = next.Add(time.Second / 4)
		time.Sleep(time.Until(next))
	}
}
