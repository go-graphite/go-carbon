package tcp

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/points"
	"github.com/go-graphite/go-carbon/receiver/stream/codec"
	"go.uber.org/zap"
)

type streamCaptureShards []Capture

func (s streamCaptureShards) Write([]byte, int) error  { panic("unsharded capture write") }
func (s streamCaptureShards) CaptureShards() []Capture { return s }

func TestStreamCaptureSwitchToDirect(t *testing.T) {
	for _, flags := range []byte{0, codec.FlagZstd} {
		for _, sharded := range []bool{false, true} {
			for _, switchBeforeConnect := range []bool{false, true} {
				for _, hook := range []string{"fallback", "point", "stream"} {
					t.Run(fmt.Sprintf("flags%d/sharded%v/newConnection%v/%s", flags, sharded, switchBeforeConnect, hook), func(t *testing.T) {
						var sealed atomic.Bool
						var captured atomic.Int64
						got := make(chan *points.Points, 4096)
						out := func(p *points.Points) { got <- p }
						writer := captureFunc(func(data []byte, n int) error {
							if sealed.Load() {
								return errors.New("startup capture partition is sealed")
							}
							captured.Add(int64(n))
							return points.ReadBinary(bytes.NewReader(data), out)
						})
						var capture Capture = writer
						if sharded {
							capture = streamCaptureShards{writer, writer, writer, writer}
						}
						r := &TCP{logger: zap.NewNop(), decompressor: newDecompressor(""), capture: capture, out: out}
						r.Start()
						defer r.Stop()
						var fast atomic.Int64
						switchDirect := func() {
							t.Helper()
							if hook == "point" {
								r.SetPointOut(func(name []byte, value float64, ts int64) {
									fast.Add(1)
									out(points.OnePoint(string(name), value, ts))
								})
							} else if hook == "stream" {
								r.SetStreamPointOut(func(name []byte, value float64, ts int64, slot *any) {
									if *slot == nil {
										*slot = string(name)
									} else if (*slot).(string) != string(name) {
										t.Error("stream slot changed metric")
									}
									fast.Add(1)
									out(points.OnePoint(string(name), value, ts))
								})
							}
							if err := r.SwitchCapture(out, func() error { sealed.Store(true); return nil }); err != nil {
								t.Fatal(err)
							}
						}
						if switchBeforeConnect {
							switchDirect()
						}
						server, client := net.Pipe()
						defer client.Close()
						done := make(chan struct{})
						go func() { r.HandleConnection(server); close(done) }()
						e, err := codec.NewEncoder(codec.Options{Flags: flags})
						if err != nil {
							t.Fatal(err)
						}
						defer e.Close()
						wire, err := codec.AppendPreamble(nil, flags)
						if err != nil {
							t.Fatal(err)
						}
						writeFrame := func(first, count int) {
							t.Helper()
							for i := first; i < first+count; i++ {
								if err := e.Add([]byte("capture.stream.same.metric"), math.Float64frombits(uint64(i)|0x8000000000000000), 1700000000+int64(i)); err != nil {
									t.Fatal(err)
								}
							}
							wire, err = e.Flush(wire)
							if err != nil {
								t.Fatal(err)
							}
							_ = client.SetWriteDeadline(time.Now().Add(3 * time.Second))
							if _, err := client.Write(wire); err != nil {
								t.Fatal(err)
							}
							wire = wire[:0]
						}
						checkPoint := func(i int) {
							t.Helper()
							select {
							case p := <-got:
								if p.Metric != "capture.stream.same.metric" || len(p.Data) != 1 || math.Float64bits(p.Data[0].Value) != uint64(i)|0x8000000000000000 || p.Data[0].Timestamp != 1700000000+int64(i) {
									t.Fatalf("point %d differs: %#v", i, p)
								}
							case <-time.After(3 * time.Second):
								t.Fatalf("point %d lost across direct fence", i)
							}
						}
						writeFrame(0, 1)
						checkPoint(0)
						if !switchBeforeConnect {
							switchDirect()
						}
						before := captured.Load()
						// Exceeds the local capture buffer: shard-triggered flushes
						// inside a frame must also observe the direct fence.
						writeFrame(1, 3000)
						_ = client.Close()
						for i := 1; i <= 3000; i++ {
							checkPoint(i)
						}
						select {
						case <-done:
						case <-time.After(3 * time.Second):
							t.Fatal("stream handler did not finish")
						}
						if captured.Load() != before {
							t.Fatal("capture writer used after direct switch")
						}
						if hook != "fallback" {
							want := int64(3000)
							if switchBeforeConnect {
								want++
							}
							if fast.Load() != want {
								t.Fatalf("direct points bypassed %s sink: got %d want %d", hook, fast.Load(), want)
							}
						}
						select {
						case p := <-got:
							t.Fatalf("duplicate point: %#v", p)
						default:
						}
					})
				}
			}
		}
	}
}
