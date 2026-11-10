package carbon

import (
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/points"
)

// This measures only capture append/sync/readback, not a restored-cache restart.
// Per-metric random-looking names prevent repeated padding from hiding the
// compression CPU and physical-byte requirements of long production names.
func TestStartupCaptureEntropyBandwidth(t *testing.T) {
	if os.Getenv("CAPTURE_SCALE") != "1" {
		t.Skip("set CAPTURE_SCALE=1 for the isolated IO-stage measurement")
	}
	repeats := 128
	if value := os.Getenv("CAPTURE_ENTROPY_REPEATS"); value != "" {
		var err error
		repeats, err = strconv.Atoi(value)
		if err != nil || repeats < 1 || repeats > 1024 {
			t.Fatal("CAPTURE_ENTROPY_REPEATS must be1..1024")
		}
	}
	const batch = 8192
	names := make([]string, batch)
	var raw []byte
	for i := range names {
		components := make([]string, 5)
		for j := range components {
			digest := sha256.Sum256([]byte(fmt.Sprintf("metric%d/component%d", i, j)))
			components[j] = base64.RawURLEncoding.EncodeToString(digest[:])
		}
		names[i] = fmt.Sprintf("servers.host%06d.application.requests.%s", i, strings.Join(components, "."))
		raw = points.OnePoint(names[i], float64(i), 1700000000).AppendBinary(raw)
	}
	s, _ := captureFixture(t)
	var wg sync.WaitGroup
	started := time.Now()
	for _, shard := range s.shards {
		wg.Go(func() {
			for range repeats {
				if err := shard.Write(raw, batch); err != nil {
					t.Error(err)
					return
				}
			}
		})
	}
	wg.Wait()
	appendTime := time.Since(started)
	s.receiver.Stop()
	if err := s.close(); err != nil {
		t.Fatal(err)
	}
	durableTime := time.Since(started)
	seen := make([]int, batch)
	invalid := false
	err := visitCapture(s, func(p *points.Points) {
		if invalid {
			return
		}
		for _, point := range p.Data {
			i := int(point.Value)
			if i < 0 || i >= batch || math.Float64bits(point.Value) != math.Float64bits(float64(i)) ||
				point.Timestamp != 1700000000 || p.Metric != names[i] {
				t.Error("capture changed an entropy fixture record")
				invalid = true
				return
			}
			seen[i]++
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	for i, count := range seen {
		if count != repeats*len(s.shards) {
			t.Fatalf("record%d count%d, want%d", i, count, repeats*len(s.shards))
		}
	}
	t.Logf("scope=IO-stage shards=%d raw_bytes_per_point=%.1f raw_bytes=%d physical_bytes=%d append_GB_s=%.3f through_sync_GB_s=%.3f records=%d repeats=%d append=%v through_sync=%v",
		len(s.shards), float64(len(raw))/batch, s.rawBytes.Load(), s.size.Load(),
		float64(s.rawBytes.Load())/appendTime.Seconds()/1e9, float64(s.rawBytes.Load())/durableTime.Seconds()/1e9,
		int64(batch*repeats*len(s.shards)), repeats, appendTime, durableTime)
}
