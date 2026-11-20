package cache

import (
	"bytes"
	"fmt"
	"math"
	"math/rand/v2"
	"reflect"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-graphite/go-carbon/internal/inputlog"

	"github.com/go-graphite/go-carbon/points"
)

func TestStreamBatchDifferential(t *testing.T) {
	for _, mode := range []string{"plain", "quota-transition", "overflow", "diversion", "tags", "recovery"} {
		t.Run(mode, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				cs := [2]*Cache{New(), New()}
				var calls [2][]uint64
				var logs [2]bytes.Buffer
				var slots [2][3]any
				names := [][]byte{[]byte("metric"), []byte("other"), []byte("series;a=z;b=y")}
				for k, c := range cs {
					c.SetMaxSize(0)
					c.EnableInputLog()
					if mode == "overflow" {
						c.SetMaxSize(17)
					}
					if mode == "tags" {
						c.SetTagsEnabled(true)
					}
					if mode == "recovery" {
						if err := c.AttachPendingRecovery(pendingFixture(t)); err != nil {
							t.Fatal(err)
						}
					}
					for j, n := range names {
						slots[k][j] = c.ResolveStreamName(n)
					}
					if mode == "quota-transition" || mode == "diversion" {
						n := 0
						c.SetThrottleWithHash(func(p *points.Points, exists bool, h uint64, state *any) bool {
							n++
							if len(p.Data) != 1 {
								t.Fatal("vector admission changed quota boundary")
							}
							calls[k] = append(calls[k], h, math.Float64bits(p.Data[0].Value), uint64(p.Data[0].Timestamp), uint64(n))
							if mode == "diversion" && n == 13 {
								c.DivertToXlog(&logs[k])
							}
							*state = n
							return mode == "quota-transition" && n%11 >= 4 && n%11 <= 7
						})
					}
				}
				r := rand.New(rand.NewPCG(12, 34))
				edge := []uint64{0, 1, 0x8000000000000000, 0x7ff0000000000000, 0xfff0000000000000, 0x7ff8000000001234, 0xfff8000000005678, 0x0010000000000000}
				for iteration := range 35 {
					j := iteration % len(names)
					data := make([]points.Point, 1+r.IntN(140))
					for i := range data {
						bits := r.Uint64()
						if i < len(edge) {
							bits = edge[i]
						}
						data[i] = points.Point{Value: math.Float64frombits(bits), Timestamp: int64(r.IntN(9)) - 4}
					}
					if iteration == 10 {
						for _, c := range cs {
							c.Pop(string(names[j]))
						}
					} // stale generation
					for _, p := range data {
						cs[0].AddStreamPoint(names[j], p.Value, p.Timestamp, &slots[0][j])
					}
					cs[1].AddStreamPoints(names[j], data, &slots[1][j])
					if !reflect.DeepEqual(calls[0], calls[1]) || !bytes.Equal(logs[0].Bytes(), logs[1].Bytes()) {
						t.Fatal("admission/diversion transcript differs")
					}
					comparePointRefCaches(t, cs[0], cs[1])
				}
			})
		})
	}
}

func TestStreamBatchPanicUnlocks(t *testing.T) {
	c := New()
	name := []byte("panic.metric")
	slot := c.ResolveStreamName(name)
	c.AddStreamPoint(name, 1, 1, &slot)
	c.SetThrottleWithHash(func(*points.Points, bool, uint64, *any) bool { panic("quota") })
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("missing callback panic")
			}
		}()
		c.AddStreamPoints(name, []points.Point{{Value: 2, Timestamp: 2}, {Value: 3, Timestamp: 3}}, &slot)
	}()
	c.SetThrottleWithHash(nil)
	c.AddStreamPoints(name, []points.Point{{Value: 4, Timestamp: 4}}, &slot)
	if len(c.Get(string(name))) != 2 {
		t.Fatal("panic lost/duplicated points")
	}
}

func BenchmarkStreamBatch(b *testing.B) {
	for _, run := range []int{1, 2, 8, 64} {
		for _, batch := range []bool{false, true} {
			b.Run(fmt.Sprintf("run%d/batch%v", run, batch), func(b *testing.B) {
				c := New()
				c.SetMaxSize(0)
				name := []byte("stream.benchmark")
				slot := c.ResolveStreamName(name)
				c.AddStreamPoint(name, 1, 1, &slot)
				data := make([]points.Point, run)
				for i := range data {
					data[i] = points.Point{Value: 1, Timestamp: 1}
				}
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if batch {
						c.AddStreamPoints(name, data, &slot)
					} else {
						for _, p := range data {
							c.AddStreamPoint(name, p.Value, p.Timestamp, &slot)
						}
					}
					if i%1024 == 0 {
						c.Pop(string(name))
					}
				}
			})
		}
	}
}

// Drive the real journal owner: waiting for capacity must release the shard,
// retain every original bit and preserve the order that durable replay observes.
func TestStreamBatchLogPressureAndReopen(t *testing.T) {
	c := New()
	c.SetMaxSize(0)
	dir := t.TempDir()
	l, _ := startInputLog(t, dir, c, inputlog.Options{SyncInterval: time.Hour, SegmentSize: 1 << 20})
	defer c.DisableInputLog()
	defer l.Kill()
	name := []byte("stream.batch.pressure")
	slot := c.ResolveStreamName(name)
	data := make([]points.Point, 2*maxShardLogEntries+17)
	for i := range data {
		data[i] = points.Point{Value: math.Float64frombits(uint64(i) * 0x9e3779b97f4a7c15), Timestamp: int64(i/3 - i%5)}
	}
	done := make(chan struct{})
	go func() { c.AddStreamPoints(name, data, &slot); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		c.DisableInputLog()
		<-done
		t.Fatal("batch held shard while awaiting log capacity")
	}
	if err := l.Sync(); err != nil {
		t.Fatal(err)
	}
	l.Kill()
	reopened, err := inputlog.Open(dir, inputlog.Options{SegmentSize: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	n := 0
	_, err = reopened.Replay(func(p *points.Points) {
		for _, point := range p.Data {
			if p.Metric != string(name) || n >= len(data) || point.Timestamp != data[n].Timestamp || math.Float64bits(point.Value) != math.Float64bits(data[n].Value) {
				t.Fatal("durable replay bits/order differ", n)
			}
			n++
		}
	})
	if err != nil || n != len(data) {
		t.Fatal("durable replay lost points", n, err)
	}
}
