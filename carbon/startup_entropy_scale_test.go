package carbon

import (
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/recovery"
	"github.com/go-graphite/go-carbon/points"
)

// This measures only capture append/sync/readback, not a restored-cache restart.
// Random-looking names prevent repeated padding from hiding physical byte cost.
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
	var duration time.Duration
	var rate float64
	if value := os.Getenv("CAPTURE_ENTROPY_SECONDS"); value != "" {
		seconds, err := strconv.Atoi(value)
		if err != nil || seconds < 1 || seconds > 180 {
			t.Fatal("CAPTURE_ENTROPY_SECONDS must be1..180")
		}
		duration = time.Duration(seconds) * time.Second
		rate, err = strconv.ParseFloat(os.Getenv("CAPTURE_ENTROPY_RATE"), 64)
		if err != nil || math.IsNaN(rate) || math.IsInf(rate, 0) || rate <= 0 || rate > 100_000_000 {
			t.Fatal("CAPTURE_ENTROPY_RATE must be finite, positive and at most100M")
		}
	} else if os.Getenv("CAPTURE_ENTROPY_RATE") != "" {
		t.Fatal("CAPTURE_ENTROPY_RATE requires CAPTURE_ENTROPY_SECONDS")
	}
	const batch = 8192
	const timestamp = 1700000000
	names := make([]string, batch)
	longest := 0
	for i := range names {
		components := make([]string, 5)
		for j := range components {
			digest := sha256.Sum256([]byte(fmt.Sprintf("metric%d/component%d", i, j)))
			components[j] = base64.RawURLEncoding.EncodeToString(digest[:])
		}
		names[i] = fmt.Sprintf("servers.host%06d.application.requests.%s", i, strings.Join(components, "."))
		longest = max(longest, len(names[i]))
	}
	s, _ := captureFixture(t)
	interval := time.Duration(0)
	if duration > 0 {
		interval = time.Duration(float64(batch*len(s.shards)) / rate * float64(time.Second))
		repeats = int(math.Ceil(float64(duration) / float64(interval)))
	}
	records := int64(batch) * int64(repeats) * int64(len(s.shards))
	if records*int64(longest+32) > s.policy.spoolBytes {
		t.Fatal("entropy stage lacks conservative uncompressed spool headroom", records, s.policy.spoolBytes)
	}
	if duration > 0 {
		s.startMonitor(s.policy.spoolBytes)
	} // production one-second sync cadence
	var wg sync.WaitGroup
	written := make([]int, len(s.shards))
	lag := make([]time.Duration, len(s.shards))
	started := time.Now()
	for id, shard := range s.shards {
		wg.Go(func() {
			var raw []byte
			for sequence := range repeats {
				due := started.Add(time.Duration(sequence) * interval)
				if duration > 0 {
					time.Sleep(max(0, time.Until(due)))
				}
				raw = raw[:0]
				for i, name := range names {
					identity := int64(sequence*len(s.shards)+id)*batch + int64(i)
					p := points.Points{Metric: name, Data: []points.Point{{Value: float64(identity), Timestamp: timestamp + int64(sequence)}}}
					raw = p.AppendBinary(raw)
				}
				if err := shard.Write(raw, batch); err != nil {
					t.Error(err)
					return
				}
				written[id]++
				if duration > 0 {
					lag[id] = max(lag[id], time.Since(due))
					if lag[id] > time.Second {
						t.Error("entropy scheduled append exceeded1s lag", id, lag[id])
						return
					}
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
	readbackStarted := time.Now()
	// Verify independent streams in parallel, retaining each stream's segment
	// order and an exact per-name sequence. Every value identifies its producer,
	// batch and row; equal total counts cannot hide a missing/replaced record.
	for id, shard := range s.shards {
		wg.Go(func() {
			seen := make([]int64, batch)
			invalid := false
			paths, err := filepath.Glob(shard.path + ".*.tail.bin")
			if err != nil {
				t.Error(err)
				return
			}
			for _, path := range paths {
				_, err := recovery.ReadCaptureFile(path, func(p *points.Points) {
					if invalid {
						return
					}
					for _, point := range p.Data {
						identity := int64(point.Value)
						if identity < 0 || math.Float64bits(point.Value) != math.Float64bits(float64(identity)) {
							t.Error("capture changed entropy value bits")
							invalid = true
							return
						}
						i := int(identity % batch)
						producer := int(identity / batch % int64(len(s.shards)))
						sequence := identity / int64(batch*len(s.shards))
						if producer != id || sequence != seen[i] || point.Timestamp != timestamp+sequence || p.Metric != names[i] {
							t.Error("capture changed entropy identity/order", id, identity, sequence, seen[i])
							invalid = true
							return
						}
						seen[i]++
					}
				})
				if err != nil {
					t.Error(err)
					return
				}
			}
			if invalid {
				return
			}
			for i, count := range seen {
				if count != int64(written[id]) {
					t.Errorf("shard%d record%d count%d, want%d", id, i, count, written[id])
					return
				}
			}
		})
	}
	wg.Wait()
	readbackTime := time.Since(readbackStarted)
	var actual int64
	var maxLag time.Duration
	for i, count := range written {
		actual += int64(count * batch)
		maxLag = max(maxLag, lag[i])
	}
	if actual != records {
		t.Error("entropy offered records incomplete", actual, records)
	}
	minimumRate := rate * .95
	if rate >= 10_000_000 {
		minimumRate = max(10_000_000, minimumRate)
	}
	if duration > 0 && float64(actual)/durableTime.Seconds() < minimumRate {
		t.Error("entropy stage did not sustain declared input through sync", float64(actual)/durableTime.Seconds(), rate)
	}
	t.Logf("scope=IO-stage shards=%d raw_bytes_per_point=%.1f raw_bytes=%d physical_bytes=%d append_GB_s=%.3f through_sync_GB_s=%.3f physical_sync_GB_s=%.3f validated_readback_GB_s=%.3f records=%d repeats=%d scheduled_rate=%.0f scheduled_duration=%v max_append_lag=%v append=%v through_sync=%v readback=%v",
		len(s.shards), float64(s.rawBytes.Load())/float64(max(1, actual)), s.rawBytes.Load(), s.size.Load(), float64(s.rawBytes.Load())/appendTime.Seconds()/1e9, float64(s.rawBytes.Load())/durableTime.Seconds()/1e9, float64(s.size.Load())/durableTime.Seconds()/1e9, float64(s.rawBytes.Load())/readbackTime.Seconds()/1e9, actual, repeats, rate, duration, maxLag, appendTime, durableTime, readbackTime)
}
