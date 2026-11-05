package cache

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
	"weak"

	"github.com/go-graphite/go-carbon/points"
	"github.com/go-graphite/go-carbon/receiver/stream/codec"
)

// This replay also runs unchanged against the parent sources using go -overlay.
// STREAM_REPLAY_OUTPUT records the complete canonical transcript for cmp, rather
// than just comparing a checksum or decoding floating point values numerically.
func TestStreamWireDifferential(t *testing.T) {
	var transcript []byte
	for _, mode := range []string{"plain", "quota", "tags", "recovery"} {
		for _, flags := range []byte{0, codec.FlagZstd} {
			t.Run(fmt.Sprintf("%s/%d", mode, flags), func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					cs := [2]*Cache{New(), New()}
					var decisions [2][]byte
					var diverted [2]bytes.Buffer
					for k, c := range cs {
						c.SetMaxSize(0)
						c.SetWriteoutBatching(8, time.Minute)
						c.SetWriteoutSpread(.2)
						c.SetWriteoutSparseSpread(true)
						c.EnableInputLog()
						if mode == "tags" {
							c.SetTagsEnabled(true)
						}
						if mode == "recovery" {
							if err := c.AttachPendingRecovery(pendingFixture(t)); err != nil {
								t.Fatal(err)
							}
						}
						if mode == "quota" {
							c.SetThrottleWithHash(func(p *points.Points, exists bool, hash uint64, _ *any) bool {
								reject := math.Float64bits(p.Data[0].Value)%7 == 0
								decisions[k] = binary.AppendUvarint(decisions[k], uint64(len(p.Metric)))
								decisions[k] = append(decisions[k], p.Metric...)
								decisions[k] = binary.LittleEndian.AppendUint64(decisions[k], hash)
								decisions[k] = binary.LittleEndian.AppendUint64(decisions[k], math.Float64bits(p.Data[0].Value))
								decisions[k] = append(decisions[k], boolByte(exists), boolByte(reject))
								return reject
							})
						}
					}
					// Exercise the capped replacement path in every replay,
					// rather than only small, partially populated indexes.
					// Unrelated hashes must never resolve to this identity.
					seed := &StreamSeries{name: "unrelated.index.entry", cache: cs[1], hash: 1}
					ref := weak.Make(seed)
					for _, sh := range cs[1].data {
						sh.streamSeries = make([]seriesSlot, maxStreamSeriesSlots)
						for i := range sh.streamSeries {
							sh.streamSeries[i] = seriesSlot{1, ref}
						}
						sh.streamSeriesBytes.Store(int64(maxStreamSeriesSlots) * 16)
					}

					const connections = 140
					es, ds := make([]*codec.Encoder, connections), make([]*codec.Decoder, connections)
					resolved, defined := 0, 0
					resolve := func(name []byte) any { resolved++; return cs[1].ResolveStreamName(name) }
					for i := range es {
						o := codec.Options{Flags: flags, MaxRows: 1024}
						var err error
						es[i], err = codec.NewEncoder(o)
						if err != nil {
							t.Fatal(err)
						}
						ds[i], err = codec.NewDecoder(o)
						if err != nil {
							t.Fatal(err)
						}
						t.Cleanup(es[i].Close)
						t.Cleanup(ds[i].Close)
						ds[i].SetNameResolver(resolve)
					}
					visit := func(_ uint32, name []byte, value float64, ts int64, slot *any) {
						cs[0].AddPoint(name, value, ts)
						cs[1].AddStreamPoint(name, value, ts, slot)
					}
					decode := func(d *codec.Decoder, wire []byte) {
						r := bytes.NewReader(wire)
						for r.Len() > 0 {
							// TCP offers its installed resolver between every
							// frame, including frames without definitions.
							d.SetNameResolver(resolve)
							info, err := d.ReadFrame(r, visit)
							if err != nil {
								t.Fatal(err)
							}
							defined += int(info.Names)
						}
					}
					flush := func(i int) {
						wire, err := es[i].Flush(nil)
						if err != nil {
							t.Fatal(err)
						}
						decode(ds[i], wire)
					}
					bits := []uint64{0, 1, 0x8000000000000000, 0x7ff0000000000000, 0xfff0000000000000, 0x7ff8000000001234, 0xfff8000000005678, 0x0010000000000000, 0x3ff8000000000000}
					names := []string{"metric", "other"}
					for i := range 4096 {
						name := fmt.Sprintf("stream.replay.%04d", i)
						if mode == "tags" {
							name += ";z=last;a=first"
						}
						names = append(names, name)
					}
					checkpoint := func() {
						transcript = binary.LittleEndian.AppendUint32(transcript, atomic.LoadUint32(&cs[1].stat.overflowCnt))
						transcript = binary.LittleEndian.AppendUint32(transcript, atomic.LoadUint32(&cs[1].stat.tagsNormalizeErrors))
						comparePointRefCaches(t, cs[0], cs[1])
						if !bytes.Equal(decisions[0], decisions[1]) {
							t.Fatal("admission decisions differ")
						}
						// Sort names, then record compact bytes, value bits and per-name order.
						var keys []string
						for _, sh := range cs[1].data {
							for name := range sh.items.all {
								keys = append(keys, name)
							}
						}
						slices.Sort(keys)
						for _, key := range keys {
							transcript = binary.AppendUvarint(transcript, uint64(len(key)))
							transcript = append(transcript, key...)
							h := cs[1].GetShard(key)
							it := h.items.get(key)
							transcript = binary.AppendUvarint(transcript, uint64(len(it.compact.buf)))
							transcript = append(transcript, it.compact.buf...)
							for _, p := range it.compact.appendDecoded(nil) {
								transcript = binary.LittleEndian.AppendUint64(transcript, math.Float64bits(p.Value))
								transcript = binary.LittleEndian.AppendUint64(transcript, uint64(p.Timestamp))
							}
						}
						transcript = append(transcript, decisions[1]...)
					}
					for epoch := range 3 {
						if epoch > 0 {
							for i := 2048; i < 4096; i++ {
								names[i+2] = fmt.Sprintf("stream.hour.%d.%04d", epoch, i)
								if mode == "tags" {
									names[i+2] += ";z=last;a=first"
								}
							}
						}
						for round := range 3 {
							// Round zero is the new-name burst. Later frames reuse definitions.
							for i, name := range names {
								peer := (i*13 + round*7) % connections
								if err := es[peer].Add([]byte(name), math.Float64frombits(bits[(i+round+epoch)%len(bits)]), int64(epoch*3600+i%23-round*31)); err != nil {
									t.Fatal(err)
								}
								if es[peer].Ready() {
									flush(peer)
								}
							}
							for peer := range es {
								flush(peer)
							}
							checkpoint()
						}
						// Writeout both evicts and replaces items while wire ids remain alive.
						for i, name := range names {
							if mode == "tags" && i >= 2 {
								name = strings.Replace(name, ";z=last;a=first", ";a=first;z=last", 1)
							}
							var popped [2]*points.Points
							for k, c := range cs {
								popped[k], _ = c.PopNotConfirmed(name)
							}
							var raw [2][]byte
							for k, p := range popped {
								if p != nil {
									raw[k] = p.AppendBinary(nil)
									if i%3 == 0 {
										cs[k].Requeue(p)
									} else {
										cs[k].Confirm(p)
									}
								}
							}
							if !bytes.Equal(raw[0], raw[1]) {
								t.Fatal("writeout bytes differ")
							}
							transcript = append(transcript, raw[1]...)
						}
						checkpoint()
						for peer := range es {
							// Reconnection/handoff state must preserve predictor state and bind
							// each inherited name exactly once, even without emitting a point.
							if peer%17 == 0 {
								state, err := ds[peer].Snapshot(nil)
								if err != nil {
									t.Fatal(err)
								}
								ds[peer].Close()
								ds[peer], err = codec.NewDecoder(codec.Options{Flags: flags})
								if err != nil {
									t.Fatal(err)
								}
								t.Cleanup(ds[peer].Close)
								ds[peer].SetNameResolver(resolve)
								decode(ds[peer], state)
							}
							if err := es[peer].Reset(); err != nil {
								t.Fatal(err)
							}
							flush(peer)
						}
						runtime.GC() // Names disappear from dictionaries, then return next hour.
						if resolved != defined {
							t.Fatalf("%d resolutions for %d definitions", resolved, defined)
						}
						if mode == "quota" && epoch == 0 {
							for _, c := range cs {
								c.SetMaxSize(1)
							}
						}
						if mode == "quota" && epoch == 1 {
							for _, c := range cs {
								c.SetMaxSize(0)
								c.SetMaxBytes(1)
							}
						}
					}
					for _, c := range cs {
						c.SetMaxBytes(0)
					}
					for k, c := range cs {
						c.DivertToBinaryXlog(&diverted[k])
					}
					for peer := range es {
						if err := es[peer].Add([]byte("diverted.name"), math.Float64frombits(bits[5]), -100); err != nil {
							t.Fatal(err)
						}
						flush(peer)
					}
					if !bytes.Equal(diverted[0].Bytes(), diverted[1].Bytes()) {
						t.Fatal("diversion bytes differ")
					}
					transcript = append(transcript, diverted[1].Bytes()...)
					runtime.KeepAlive(seed)
					t.Logf("resolved=%d defined=%d ratio=%.1f", resolved, defined, float64(resolved)/float64(defined))
				})
			})
		}
	}
	t.Logf("transcript %d bytes sha256 %x", len(transcript), sha256.Sum256(transcript))
	if path := os.Getenv("STREAM_REPLAY_OUTPUT"); path != "" {
		if err := os.WriteFile(path, transcript, 0600); err != nil {
			t.Fatal(err)
		}
	}
}

func boolByte(v bool) byte {
	if v {
		return 1
	}
	return 0
}

func TestStreamSeriesWeakIndexDoesNotRetainNames(t *testing.T) {
	c := New()
	// Keep the name out of the tiny allocator and leave only weak references.
	makeWeak := func() (weak.Pointer[StreamSeries], weak.Pointer[byte]) {
		s := c.ResolveStreamName(bytes.Repeat([]byte("ephemeral."), 20)).(*StreamSeries)
		return weak.Make(s), weak.Make(&s.StreamName()[0])
	}
	ref, name := makeWeak()
	for range 20 {
		runtime.GC()
		if ref.Value() == nil && name.Value() == nil {
			return
		}
	}
	t.Fatal("weak index retained an unreferenced series/name")
}

func TestStreamSeriesConcurrentEviction(t *testing.T) {
	c := New()
	c.SetMaxSize(0)
	const writers, names, perWriter = 8, 64, 4000
	var wg sync.WaitGroup
	done := make(chan struct{})
	var persisted int
	seen := make([]bool, writers*perWriter)
	collect := func(p *points.Points) {
		for _, point := range p.Data {
			w, i := int(point.Value), int(point.Timestamp)
			if w < 0 || w >= writers || i < 0 || i >= perWriter {
				t.Errorf("unexpected point %+v", point)
				continue
			}
			if p.Metric != fmt.Sprintf("stream.concurrent.%d", (i+w)%names) {
				t.Errorf("point routed to %s", p.Metric)
			}
			key := w*perWriter + i
			if seen[key] {
				t.Errorf("duplicate point %d/%d", w, i)
			}
			seen[key] = true
			persisted++
		}
	}
	wg.Go(func() {
		cycles := 0
		for {
			select {
			case <-done:
				return
			default:
			}
			cycles++
			if cycles%16 == 0 {
				runtime.GC()
			}
			for i := range names {
				if p, ok := c.PopNotConfirmed(fmt.Sprintf("stream.concurrent.%d", i)); ok {
					collect(p)
					c.Confirm(p)
				}
			}
		}
	})
	var producers sync.WaitGroup
	for w := range writers {
		producers.Go(func() {
			for i := range perWriter {
				name := fmt.Appendf(nil, "stream.concurrent.%d", (i+w)%names)
				slot := c.ResolveStreamName(name)
				c.AddStreamPoint(name, float64(w), int64(i), &slot)
			}
		})
	}
	producers.Wait()
	close(done)
	wg.Wait()
	for i := range names {
		if p, ok := c.Pop(fmt.Sprintf("stream.concurrent.%d", i)); ok {
			collect(p)
		}
	}
	if persisted != writers*perWriter || c.Size() != 0 {
		t.Fatalf("persisted %d of %d, remaining %d", persisted, writers*perWriter, c.Size())
	}
}
