package cache

import (
	"fmt"
	"os"
	"runtime"
	"runtime/pprof"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/zeebo/xxh3"
)

var pointRefFixture struct {
	sync.Once
	c      *Cache
	names  []string
	hashes []uint64
	items  []*item
	refs   []PointRef
}

// Opt in with POINT_REF_METRICS=1000000 or 5000000. Names are 150-300
// bytes, with separate backing storage from the cache's keys. Each writer
// walks a disjoint random partition, representing connection-owned series.
func pointRefCache(b *testing.B) {
	n, _ := strconv.Atoi(os.Getenv("POINT_REF_METRICS"))
	if n == 0 {
		b.Skip("set POINT_REF_METRICS=1000000 or 5000000")
	}
	pointRefFixture.Do(func() {
		c := New()
		c.SetMaxSize(0)
		c.SetWriteoutBatching(960, 960*time.Second)
		c.SetWriteoutSpread(.2)
		c.SetBloomSize(uint64(n * 2))
		c.SetNewMetricsChan(make(chan string, 1))
		c.SetMetricExists(func(string) bool { return true })
		pointRefFixture.c = c
		pointRefFixture.names = make([]string, n)
		pointRefFixture.hashes = make([]uint64, n)
		pointRefFixture.items = make([]*item, n)
		pointRefFixture.refs = make([]PointRef, n)
		for i := range n {
			prefix := fmt.Sprintf("services.namespace-%04d.host-%08d.component.requests.region.zone.endpoint.", i%1000, i)
			name := prefix + strings.Repeat("x", 150+i%151-len(prefix))
			pointRefFixture.names[i] = name
			hash := xxh3.HashString(name)
			pointRefFixture.hashes[i] = hash
			c.AddPoint([]byte(name), 1, 1790000000)
			it := c.data[hash&(shardCount-1)].items.get(name)
			pointRefFixture.items[i] = it
			shard := uint32(hash & (shardCount - 1))
			slots := c.data[shard].items.slots
			mask := len(slots) - 1
			for slot := int(hash>>shardBits) & mask; ; slot = (slot + 1) & mask {
				if slots[slot].item == it {
					pointRefFixture.refs[i] = PointRef{cache: c, generation: c.data[shard].streamIdentity(it), shard: shard, slot: uint32(slot)}
					break
				}
			}
		}
		// Later insertions can rehash earlier refs. Refresh them once before
		// measuring the stable-ID path; exclude this construction from timing.
		for i, hash := range pointRefFixture.hashes {
			ref := &pointRefFixture.refs[i]
			slots := c.data[ref.shard].items.slots
			mask := len(slots) - 1
			for slot := int(hash>>shardBits) & mask; ; slot = (slot + 1) & mask {
				if slots[slot].item != nil && slots[slot].item.streamGeneration == ref.generation {
					ref.slot = uint32(slot)
					break
				}
			}
		}
		runtime.GC()
	})
}

// POINT_REF_PROFILE writes CPU profiles only during the timed phase, excluding
// construction and GC of the multi-million-series fixture. Use a fixed -benchtime
// (e.g. 10000000x) to avoid benchmark calibration profiles overwriting one another.
func BenchmarkPointRef(b *testing.B) {
	pointRefCache(b)
	stages := []string{"Input", "Hash", "Lookup", "LockedLookup", "Accounting", "BloomKnown", "AddPoint", "AddPointRef"}
	if only := os.Getenv("POINT_REF_STAGE"); only != "" {
		stages = strings.Split(only, ",")
	}
	// Interleave API pairs, alternating which runs first, when other work on
	// the machine makes widely separated wall-time comparisons unreliable.
	if pairs, _ := strconv.Atoi(os.Getenv("POINT_REF_PAIRS")); pairs > 0 {
		for pair := range pairs {
			for _, writers := range []int{1, 8, 32} {
				for j := range stages {
					stage := stages[(j+pair)%len(stages)]
					b.Run(fmt.Sprintf("%s/writers=%d/pair=%d", stage, writers, pair), func(b *testing.B) { benchmarkPointRefStage(b, stage, writers) })
				}
			}
		}
		return
	}
	for _, stage := range stages {
		for _, writers := range []int{1, 8, 32} {
			b.Run(fmt.Sprintf("%s/writers=%d", stage, writers), func(b *testing.B) { benchmarkPointRefStage(b, stage, writers) })
		}
	}
}

func pointRefCPU() int64 {
	var usage syscall.Rusage
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &usage)
	return (int64(usage.Utime.Sec)+int64(usage.Stime.Sec))*1e9 + (int64(usage.Utime.Usec)+int64(usage.Stime.Usec))*1e3
}

func benchmarkPointRefStage(b *testing.B, stage string, writers int) {
	c, f := pointRefFixture.c, &pointRefFixture
	s := c.settings.Load().(*cacheSettings)
	var profile *os.File
	if dir := os.Getenv("POINT_REF_PROFILE"); dir != "" && b.N > 1 {
		var err error
		profile, err = os.Create(fmt.Sprintf("%s/%s-%d.pprof", dir, stage, writers))
		if err != nil {
			b.Fatal(err)
		}
		if err = pprof.StartCPUProfile(profile); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	cpu := pointRefCPU()
	var wg sync.WaitGroup
	for w := range writers {
		wg.Go(func() {
			var buf [300]byte
			rng, sink := uint64(w+1), uint64(0)
			partition := (len(f.names)-1-w)/writers + 1
			for op := w; op < b.N; op += writers {
				rng = rng*6364136223846793005 + 1
				i := int((rng>>32)%uint64(partition))*writers + w
				name := f.names[i]
				copy(buf[:], name)
				input := buf[:len(name)]
				switch stage {
				case "Input":
					sink += uint64(input[0])
				case "Hash":
					sink += xxh3.Hash(input)
				case "Lookup":
					h := f.hashes[i]
					sink += uint64(c.data[h&(shardCount-1)].items.getBytesHashed(h, input).spread)
				case "LockedLookup":
					h := f.hashes[i]
					sh := c.data[h&(shardCount-1)]
					sh.mu.Lock()
					sink += uint64(sh.items.getBytesHashed(h, input).spread)
					sh.mu.Unlock()
				case "Accounting", "BloomKnown":
					sh := c.data[f.hashes[i]&(shardCount-1)]
					sh.mu.Lock()
					if stage == "Accounting" {
						c.addSizeLocked(sh, 1, s.sizeSlack)
					} else {
						filter := c.newMetricCf.Load()
						if f.items[i].indexGeneration != filter.generation {
							b.Error("unwarmed filter")
						}
					}
					sh.mu.Unlock()
				case "AddPoint":
					c.AddPoint(input, 1, 1790000000)
				case "AddPointRef":
					c.AddPointRef(&f.refs[i], input, 1, 1790000000)
				default:
					b.Errorf("unknown stage %q", stage)
					return
				}
			}
			if sink == ^uint64(0) {
				b.Error("unreachable sink")
			}
		})
	}
	wg.Wait()
	b.StopTimer()
	cpu = pointRefCPU() - cpu
	if profile != nil {
		pprof.StopCPUProfile()
		profile.Close()
	}
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N), "ns/point")
	b.ReportMetric(float64(cpu)/float64(b.N), "cpu-ns/point")
}
