package cache

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math"
	"math/rand/v2"
	"os"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"
	"unsafe"

	"github.com/go-graphite/go-carbon/internal/inputlog"
	"github.com/go-graphite/go-carbon/points"
	"github.com/zeebo/xxh3"
)

func TestCacheInsertMetricMemoryBound(t *testing.T) {
	if unsafe.Sizeof(uintptr(0)) != 8 {
		t.Skip("64-bit layout bound")
	}
	// The queued source hint avoids repeated recovery-map probes. Its 8 bytes
	// are accounted along with the atomic journal residence timestamp.
	if size := unsafe.Sizeof(item{}); size != 208 {
		t.Fatalf("item bookkeeping grew to %d bytes", size)
	}
}

func TestItemMapBytesLookupLongNameNoAllocation(t *testing.T) {
	var m itemMap
	// Include full-hash collisions and equal long prefixes: neither the control
	// byte nor the exact hash is a substitute for complete name equality.
	names := [][]byte{[]byte(strings.Repeat("shared.namespace.", 32) + "a"), []byte(strings.Repeat("shared.namespace.", 32) + "b")}
	for _, name := range names {
		m.setHashed(7, newItem(string(name), 0))
	}
	if allocations := testing.AllocsPerRun(1000, func() {
		for _, name := range names {
			if it := m.getBytesHashed(7, name); it == nil || it.Metric != string(name) {
				t.Fatal("collision changed identity")
			}
		}
	}); allocations != 0 {
		t.Fatalf("long name lookup allocated %g objects", allocations)
	}
	m.removeHashed(7, string(names[0]))
	if m.getBytesHashed(7, names[0]) != nil || m.getBytesHashed(7, names[1]) == nil {
		t.Fatal("tombstone changed collision lookup")
	}
}

// Run the same deterministic workflow against the pinned original items.go via
// a Go overlay and compare its receipt byte-for-byte with the optimized build.
func TestCacheInsertDifferentialReceipt(t *testing.T) {
	path := os.Getenv("CACHE_INSERT_ORACLE")
	if path == "" {
		t.Skip("set CACHE_INSERT_ORACLE to write an original-code parity receipt")
	}
	c := New()
	c.SetMaxSize(2000)
	c.SetMaxBytes(2 << 20)
	c.EnableInputLog()
	c.SetBloomSize(1024)
	c.SetNewMetricsChan(make(chan string, 8))
	c.SetMetricExists(func(string) bool { return true })
	if err := c.SetWriteoutBatching(16, 960*time.Second); err != nil {
		t.Fatal(err)
	}
	digest := sha256.New()
	put := func(n uint64) { var b [8]byte; binary.LittleEndian.PutUint64(b[:], n); digest.Write(b[:]) }
	text := func(s string) { put(uint64(len(s))); digest.Write([]byte(s)) }
	c.SetThrottleWithHash(func(p *points.Points, exists bool, hash uint64, state *any) bool {
		text(p.Metric)
		put(hash)
		put(uint64(len(p.Data)))
		if exists {
			put(1)
		} else {
			put(0)
		}
		n, _ := (*state).(uint64)
		*state = n + 1
		put(n)
		return p.Data[0].Value < 0
	})
	names := make([]string, 512)
	for i := range names {
		names[i] = strings.Repeat("namespace.component.", i%20+1) + "metric." + strconv.Itoa(i)
	}
	rng := rand.New(rand.NewPCG(8144, 5))
	var diverted bytes.Buffer
	var batchNames [][]byte
	var batchData []points.Point
	useBatch := os.Getenv("CACHE_INSERT_BATCH") == "1"
	flush := func() {
		if len(batchData) > 0 {
			c.AddPointBatch(batchNames, batchData)
			batchNames, batchData = nil, nil
		}
	}
	addPoint := func(name []byte, value float64, ts int64) {
		if useBatch {
			batchNames = append(batchNames, name)
			batchData = append(batchData, points.Point{Value: value, Timestamp: ts})
			if len(batchData) == lookupBatchSize {
				flush()
			}
		} else {
			c.AddPoint(name, value, ts)
		}
	}
	drain := func(gen uint64) {
		for sh := range shardCount {
			batch := c.TakeLogBatch(sh, gen, inputlog.Batch{})
			for _, p := range batch.Entries(nil) {
				text(p.Metric())
				put(math.Float64bits(p.Value))
				put(uint64(p.Timestamp))
			}
			put(uint64(batch.Len()))
		}
	}
	for step := range 12000 {
		name := names[rng.IntN(len(names))]
		ts := int64(step/3 - 2000)
		value := float64(rng.IntN(41) - 2)
		if step%97 == 0 {
			value = math.Float64frombits(0x7ff8000000000042)
		}
		if step%101 == 0 {
			value = math.Copysign(0, -1)
		}
		switch rng.IntN(12) {
		case 0:
			flush()
			p, ok := c.PopNotConfirmed(name)
			if ok {
				if step%2 == 0 {
					c.Requeue(p)
				} else {
					c.Confirm(p)
					c.Confirm(p)
				}
			}
		case 1:
			flush()
			c.Pop(name)
		case 2:
			flush()
			c.AddPoints([]byte(name), []points.Point{{Value: value, Timestamp: ts}, {Value: value, Timestamp: ts - 17}, {Value: value, Timestamp: ts}})
		default:
			addPoint([]byte(name), value, ts)
		}
		if step%701 == 0 || step%701 == 3 || step%997 == 0 || step%997 == 7 || step%1000 == 0 {
			flush()
		}
		if step%701 == 0 {
			c.SetTagsEnabled(true)
		}
		if step%701 == 3 {
			c.SetTagsEnabled(false)
		}
		if step%997 == 0 {
			c.DivertToXlog(&diverted)
		}
		if step%997 == 7 {
			c.DivertToXlog(nil)
		}
		if step%1000 == 0 {
			drain(uint64(step + 1))
		}
	}
	flush()
	c.DivertToXlog(nil)
	drain(20000)
	text(diverted.String())
	put(uint64(c.Size()))
	put(uint64(c.Len()))
	put(uint64(c.EstimatedBytes()))
	put(uint64(c.PointBytes()))
	put(uint64(c.stat.overflowCnt))
	put(c.LogDropped())
	slices.Sort(names)
	for _, name := range names {
		text(name)
		for _, p := range c.Get(name) {
			put(math.Float64bits(p.Value))
			put(uint64(p.Timestamp))
		}
		sh := c.GetShard(name)
		it := sh.items.get(name)
		if it == nil {
			put(0)
			continue
		}
		put(1)
		put(it.hash)
		put(uint64(it.spread))
		put(uint64(it.slot))
		put(uint64(it.flags))
		put(it.logFirst)
		put(uint64(it.count()))
		put(uint64(it.compact.firstTimestamp()))
		put(uint64(it.compact.encodedBytes()))
		state, _ := it.throttle.(uint64)
		put(state)
		if it.hash != xxh3.HashString(name) {
			t.Fatal("identity changed")
		}
	}
	result := fmt.Sprintf("%s\n", hex.EncodeToString(digest.Sum(nil)))
	if err := os.WriteFile(path, []byte(result), 0600); err != nil {
		t.Fatal(err)
	}
	t.Log(result)
}
