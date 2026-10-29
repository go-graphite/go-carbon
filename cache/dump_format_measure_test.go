package cache

import (
	"encoding/binary"
	"math"
	"math/bits"
	"math/rand/v2"
	"os"
	"path/filepath"
	"runtime"
	"runtime/metrics"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"

	"github.com/go-graphite/go-carbon/internal/inputlog/loadgen"
	"github.com/go-graphite/go-carbon/internal/recovery"
	"github.com/go-graphite/go-carbon/points"
)

// buildMeasureCache fills a cache with metrics of the loadgen mix, each with
// about perMetric points at a 10 s step with occasional jitter.
func buildMeasureCache(metrics, perMetric int) *Cache {
	mix := loadgen.New(metrics, 1)
	c := New()
	c.SetMaxSize(0)
	var wg sync.WaitGroup
	workers := runtime.GOMAXPROCS(0)
	for w := range workers {
		wg.Go(func() {
			r := rand.New(rand.NewPCG(uint64(w), 9))
			data := make([]points.Point, 0, 2*perMetric)
			for i := w; i < metrics; i += workers {
				n := 1 + r.IntN(2*perMetric-1)
				ts := int64(1_790_000_000 + r.IntN(600))
				data = data[:0]
				for range n {
					step := int64(10)
					if r.IntN(50) == 0 {
						step += int64(r.IntN(3)) - 1
					}
					ts += step
					data = append(data, points.Point{Value: mix.Next(i, r), Timestamp: ts})
				}
				c.AddRestored(&points.Points{Metric: string(mix.Names[i]), Data: append([]points.Point(nil), data...)})
			}
		})
	}
	wg.Wait()
	return c
}

func appendXorM(dst []byte, x uint64) []byte {
	if x == 0 {
		return append(dst, 0)
	}
	lead, trail := bits.LeadingZeros64(x)/8, bits.TrailingZeros64(x)/8
	n := 8 - lead - trail
	dst = append(dst, byte(1+lead*8+n-1))
	for i := n - 1; i >= 0; i-- {
		dst = append(dst, byte(x>>(8*(trail+i))))
	}
	return dst
}

func TestMeasureDumpFormats(t *testing.T) {
	if os.Getenv("DUMP_MEASURE") == "" {
		t.Skip("set DUMP_MEASURE=metrics to measure")
	}
	metrics, _ := strconv.Atoi(os.Getenv("DUMP_MEASURE"))
	start := time.Now()
	c := buildMeasureCache(metrics, 189)
	t.Logf("built %d points over %d metrics in %v", c.Size(), metrics, time.Since(start))
	enc, _ := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedFastest), zstd.WithEncoderConcurrency(runtime.GOMAXPROCS(0)), zstd.WithEncoderCRC(false))
	type format struct {
		name string
		fill func(shard *Shard, buf []byte) []byte
		zstd bool
	}
	legacy := func(shard *Shard, buf []byte) []byte {
		var scratch points.Points
		_ = shard.visitLocked(&scratch, func(p *points.Points) error { buf = p.AppendBinary(buf); return nil })
		return buf
	}
	compact := func(shard *Shard, buf []byte) []byte {
		for name, it := range shard.items.all {
			buf = binary.AppendUvarint(buf, uint64(len(name)))
			buf = append(buf, name...)
			buf = binary.AppendUvarint(buf, uint64(it.compact.n))
			buf = binary.AppendUvarint(buf, uint64(len(it.compact.buf)))
			buf = append(buf, it.compact.buf...)
		}
		return buf
	}
	// stream: names first, then id, timestamp and value columns, as the
	// input log's codec 2 lays out one frame.
	stream := func(shard *Shard, buf []byte) []byte {
		var ids, stamps, values []byte
		var scratch []points.Point
		var prevTs int64
		id := 0
		for name, it := range shard.items.all {
			buf = binary.AppendUvarint(buf, uint64(len(name)))
			buf = append(buf, name...)
			scratch = appendDecoded(scratch[:0], it.compact.buf, it.compact.n)
			lastTs, lastBits := prevTs, uint64(0)
			for j, p := range scratch {
				d := 0
				if j == 0 && id > 0 {
					d = 1
				}
				ids = binary.AppendUvarint(ids, uint64(d))
				stamps = binary.AppendVarint(stamps, p.Timestamp-lastTs)
				b := math.Float64bits(p.Value)
				values = appendXorM(values, b^lastBits)
				lastTs, lastBits = p.Timestamp, b
			}
			prevTs = lastTs
			id++
		}
		buf = append(append(append(buf, ids...), stamps...), values...)
		return buf
	}
	for _, f := range []format{{"legacy", legacy, false}, {"compact", compact, false}, {"compact+zstd", compact, true}, {"stream", stream, false}, {"stream+zstd", stream, true}, {"legacy+zstd", legacy, true}} {
		var raw, out, cpu atomic.Int64
		var next atomic.Int64
		began := time.Now()
		var wg sync.WaitGroup
		for range runtime.GOMAXPROCS(0) {
			wg.Go(func() {
				var buf, z []byte
				for {
					i := int(next.Add(1) - 1)
					if i >= shardCount {
						return
					}
					s := time.Now()
					shard := c.data[i]
					shard.mu.RLock()
					buf = f.fill(shard, buf[:0])
					shard.mu.RUnlock()
					raw.Add(int64(len(buf)))
					if f.zstd {
						for b := buf; len(b) > 0; {
							k := min(len(b), 1<<20)
							z = enc.EncodeAll(b[:k], z[:0])
							out.Add(int64(len(z)))
							b = b[k:]
						}
					} else {
						out.Add(int64(len(buf)))
					}
					cpu.Add(int64(time.Since(s)))
				}
			})
		}
		wg.Wait()
		n := float64(c.Size())
		if f.zstd {
			// Random access to one metric decodes its whole block.
			dec, _ := zstd.NewReader(nil, zstd.WithDecoderConcurrency(1))
			var buf []byte
			c.data[0].mu.RLock()
			buf = f.fill(c.data[0], buf)
			c.data[0].mu.RUnlock()
			z := enc.EncodeAll(buf[:min(len(buf), 1<<20)], nil)
			began := time.Now()
			const rounds = 200
			for range rounds {
				buf, _ = dec.DecodeAll(z, buf[:0])
			}
			t.Logf("%-13s decode one %d KB block: %v", f.name, len(buf)>>10, time.Since(began)/rounds)
			dec.Close()
		}
		t.Logf("%-13s raw %6.2f B/pt  out %6.2f B/pt  wall %v  cpu %v (%.1f ns/pt)", f.name, float64(raw.Load())/n, float64(out.Load())/n, time.Since(began).Round(time.Millisecond), time.Duration(cpu.Load()).Round(time.Millisecond), float64(cpu.Load())/n)
	}
}

// phase measures wall time, process CPU, peak heap growth and allocations of fn.
func phase(t *testing.T, name string, fn func()) {
	t.Helper()
	runtime.GC()
	sample := []metrics.Sample{{Name: "/memory/classes/heap/objects:bytes"}}
	metrics.Read(sample)
	base := sample[0].Value.Uint64()
	var peak atomic.Uint64
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Go(func() {
		s := []metrics.Sample{{Name: "/memory/classes/heap/objects:bytes"}}
		for {
			metrics.Read(s)
			if v := s[0].Value.Uint64(); v > peak.Load() {
				peak.Store(v)
			}
			select {
			case <-stop:
				return
			case <-time.After(2 * time.Millisecond):
			}
		}
	})
	cpu := func() time.Duration {
		var ru syscall.Rusage
		_ = syscall.Getrusage(syscall.RUSAGE_SELF, &ru)
		return time.Duration(ru.Utime.Nano() + ru.Stime.Nano())
	}
	c0, w0 := cpu(), time.Now()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	fn()
	wall, used := time.Since(w0), cpu()-c0
	runtime.ReadMemStats(&after)
	close(stop)
	wg.Wait()
	growth := int64(peak.Load()) - int64(base)
	t.Logf("  %-22s wall %8v  cpu %8v  peak heap +%5d MB", name, wall.Round(time.Millisecond), used.Round(time.Millisecond), growth>>20)
	t.Logf("  %-22s allocated %d bytes in %d allocations", name, after.TotalAlloc-before.TotalAlloc, after.Mallocs-before.Mallocs)
}

// TestMeasureDumpRestore runs the stop's dump, checkpoint publication, the
// successor's checkpoint open, read overlay and claims, and an ordered
// restore, in both formats, on the realistic synthetic cache.
func TestMeasureDumpRestore(t *testing.T) {
	if os.Getenv("DUMP_MEASURE") == "" {
		t.Skip("set DUMP_MEASURE=metrics to measure")
	}
	n, _ := strconv.Atoi(os.Getenv("DUMP_MEASURE"))
	c := buildMeasureCache(n, 189)
	workers := runtime.GOMAXPROCS(0)
	t.Logf("cache: %d points over %d metrics, %d workers", c.Size(), n, workers)
	phase(t, "warm names (serial)", func() {
		b := recovery.NewBuilder(nil)
		b.Reserve(n)
		_ = c.VisitMetricNames(func(name string) error { _, err := b.ID(name); return err })
	})
	phase(t, "warm names (parallel)", func() {
		b := recovery.NewBuilder(nil)
		b.Reserve(n)
		_ = c.VisitMetricNamesParallel(workers, func(name string) error { _, err := b.ID(name); return err })
	})
	for _, f := range []recovery.Format{recovery.FormatLegacy, recovery.FormatCompact} {
		dir, root := t.TempDir(), t.TempDir()
		t.Logf("%s:", f)
		builder := recovery.NewBuilder(nil)
		builder.Reserve(n)
		var cacheFile, walFile recovery.File
		phase(t, "dump+sync", func() {
			dump, err := recovery.NewWriterFormat(filepath.Join(dir, recovery.CacheName(f, "1.2.bin")), 0, 1<<20, builder, f)
			if err != nil {
				t.Fatal(err)
			}
			err = dump.WriteSegments(ShardCount, workers, func(seg int, out *recovery.Segment) error {
				if f == recovery.FormatCompact {
					return c.DumpShardsCompact(seg, seg+1, out.WriteCompact)
				}
				return c.DumpShards(seg, seg+1, out.WritePoints)
			})
			if err == nil {
				cacheFile, err = dump.Close()
			}
			if err != nil {
				t.Fatal(err)
			}
		})
		wal, err := recovery.NewWriter(filepath.Join(dir, "input.1.2.bin"), 1, 4096, builder)
		if err == nil {
			walFile, err = wal.Close()
		}
		if err != nil {
			t.Fatal(err)
		}
		phase(t, "index write+publish", func() {
			index, err := recovery.WriteIndex(dir, builder)
			if err == nil {
				err = recovery.Publish(dir, root, cacheFile, walFile, index, "id")
			}
			if err != nil {
				t.Fatal(err)
			}
		})
		t.Logf("  dump bytes %d (%.2f B/pt)", cacheFile.Size, float64(cacheFile.Size)/float64(c.Size()))
		var bundle *recovery.Bundle
		phase(t, "checkpoint open", func() {
			if bundle, err = recovery.OpenBundle(dir, root); err != nil {
				t.Fatal(err)
			}
		})
		mix := loadgen.New(n, 1)
		phase(t, "read overlay (all)", func() {
			var wg sync.WaitGroup
			for w := range workers {
				wg.Go(func() {
					for i := w; i < n; i += workers {
						slot, ok, err := bundle.Find(string(mix.Names[i]))
						if err == nil && ok {
							_, err = bundle.Read(slot)
						}
						if err != nil || !ok {
							t.Error("read", ok, err)
							return
						}
					}
				})
			}
			wg.Wait()
		})
		next := New()
		next.SetMaxSize(0)
		phase(t, "claim all (restore)", func() {
			if err := next.AttachPendingRecovery(bundle); err != nil {
				t.Fatal(err)
			}
			var wg sync.WaitGroup
			for w := range workers {
				wg.Go(func() {
					for slot := uint64(w); slot < bundle.Slots(); slot += uint64(workers) {
						name, found, err := bundle.Name(slot)
						if err == nil && found {
							err = next.claimPendingMetric(next.pending.Load(), name, slot)
						}
						if err != nil {
							t.Error(err)
							return
						}
					}
				})
			}
			wg.Wait()
		})
		if next.Size() != c.Size() {
			t.Fatal("claimed points", next.Size(), c.Size())
		}
		if f == recovery.FormatLegacy {
			// The claim before this change: decode into a new slice, then
			// encode into a growing item.
			phase(t, "claim all (base code)", func() {
				var wg sync.WaitGroup
				for w := range workers {
					wg.Go(func() {
						for slot := uint64(w); slot < bundle.Slots(); slot += uint64(workers) {
							p, err := bundle.Read(slot)
							if err != nil {
								t.Error(err)
								return
							}
							if p != nil {
								values := newItem(p.Metric, 0)
								values.compact.appendPoints(p.Data)
								runtime.KeepAlive(values)
							}
						}
					})
				}
				wg.Wait()
			})
		}
		next = nil
		ordered := New()
		ordered.SetMaxSize(0)
		phase(t, "ordered restore", func() {
			path := filepath.Join(dir, cacheFile.Name)
			if f == recovery.FormatCompact {
				err = recovery.ReadCompactFile(path, ordered.AddRestored)
			} else {
				err = points.ReadFromFile(path, ordered.AddRestored)
			}
			if err != nil {
				t.Fatal(err)
			}
		})
		if ordered.Size() != c.Size() {
			t.Fatal("restored points", ordered.Size(), c.Size())
		}
		_ = bundle.Close()
	}
}

// TestMeasureCheckpointInput compares the input file a stop diverts input to
// during a compact checkpoint, legacy against compact: its bytes and write
// rate from parallel receivers, the work left once input closes, and the
// successor's checkpoint open, reads and claims. Input is three points per
// metric, as three seconds at one point per metric per second.
func TestMeasureCheckpointInput(t *testing.T) {
	if os.Getenv("DUMP_MEASURE") == "" {
		t.Skip("set DUMP_MEASURE=metrics to measure")
	}
	n, _ := strconv.Atoi(os.Getenv("DUMP_MEASURE"))
	c := buildMeasureCache(n, 189)
	mix := loadgen.New(n, 1)
	workers := runtime.GOMAXPROCS(0)
	for _, walFormat := range []recovery.Format{recovery.FormatLegacy, recovery.FormatCompact} {
		dir, root := t.TempDir(), t.TempDir()
		t.Logf("input %s:", walFormat)
		builder := recovery.NewBuilder(nil)
		builder.Reserve(n)
		_ = c.VisitMetricNamesParallel(workers, func(name string) error { _, err := builder.ID(name); return err })
		dump, err := recovery.NewWriterFormat(filepath.Join(dir, recovery.CacheName(recovery.FormatCompact, "1.2.bin")), 0, 1<<20, builder, recovery.FormatCompact)
		if err != nil {
			t.Fatal(err)
		}
		if err = dump.WriteSegments(ShardCount, workers, func(seg int, out *recovery.Segment) error {
			return c.DumpShardsCompact(seg, seg+1, out.WriteCompact)
		}); err != nil {
			t.Fatal(err)
		}
		cacheFile, err := dump.Close()
		if err != nil {
			t.Fatal(err)
		}
		wal, err := recovery.NewWriterFormat(filepath.Join(dir, recovery.InputName(walFormat, "1.2.bin")), 1, recovery.InputFileBuffer, builder, walFormat)
		if err != nil {
			t.Fatal(err)
		}
		divert := func(second int) {
			var wg sync.WaitGroup
			for w := range 8 {
				wg.Go(func() {
					r := rand.New(rand.NewPCG(uint64(w), uint64(second)))
					p := points.Points{Data: make([]points.Point, 1)}
					for i := w; i < n; i += 8 {
						p.Metric = string(mix.Names[i])
						p.Data[0] = points.Point{Value: mix.Next(i, r), Timestamp: int64(1_800_000_000 + second)}
						if err := wal.WritePoints(&p); err != nil {
							t.Error(err)
							return
						}
					}
				})
			}
			wg.Wait()
		}
		var index recovery.File
		phase(t, "divert 2 s of input", func() { divert(0); divert(1) })
		if walFormat == recovery.FormatCompact {
			phase(t, "index (input active)", func() {
				if index, err = recovery.WriteCompactIndex(dir, builder); err != nil {
					t.Fatal(err)
				}
			})
		}
		phase(t, "divert 1 s of input", func() { divert(2) })
		var walFile recovery.File
		phase(t, "input closed: finish", func() {
			if walFile, err = wal.Close(); err != nil {
				t.Fatal(err)
			}
			if walFormat == recovery.FormatLegacy {
				if index, err = recovery.WriteIndex(dir, builder); err != nil {
					t.Fatal(err)
				}
			}
			if err = recovery.Publish(dir, root, cacheFile, walFile, index, "id"); err != nil {
				t.Fatal(err)
			}
		})
		t.Logf("  input file %d bytes (%.2f B/pt), index %d bytes", walFile.Size, float64(walFile.Size)/float64(3*n), index.Size)
		var bundle *recovery.Bundle
		phase(t, "checkpoint open", func() {
			if bundle, err = recovery.OpenBundle(dir, root); err != nil {
				t.Fatal(err)
			}
		})
		phase(t, "read overlay (all)", func() {
			var wg sync.WaitGroup
			for w := range workers {
				wg.Go(func() {
					for i := w; i < n; i += workers {
						slot, ok, err := bundle.Find(string(mix.Names[i]))
						if err == nil && ok {
							_, err = bundle.Read(slot)
						}
						if err != nil || !ok {
							t.Error("read", ok, err)
							return
						}
					}
				})
			}
			wg.Wait()
		})
		next := New()
		next.SetMaxSize(0)
		phase(t, "claim all (restore)", func() {
			if err := next.AttachPendingRecovery(bundle); err != nil {
				t.Fatal(err)
			}
			var wg sync.WaitGroup
			for w := range workers {
				wg.Go(func() {
					for slot := uint64(w); slot < bundle.Slots(); slot += uint64(workers) {
						name, found, err := bundle.Name(slot)
						if err == nil && found && bundle.Count(slot) > 0 {
							err = next.claimPendingMetric(next.pending.Load(), name, slot)
						}
						if err != nil {
							t.Error(err)
							return
						}
					}
				})
			}
			wg.Wait()
		})
		if next.Size() != c.Size()+int64(3*n) {
			t.Fatal("claimed points", next.Size(), c.Size()+int64(3*n))
		}
		_ = bundle.Close()
	}
}
