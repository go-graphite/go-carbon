package v2recv

import (
	"context"
	"fmt"
	"hash/maphash"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/points"
)

type bytesBenchOwner struct {
	seed maphash.Seed
	hash uint64
}

func (o *bytesBenchOwner) Append(names []string, _ []uint64, _ []points.Point, _ []uint64, _ int64) error {
	for _, name := range names {
		o.hash ^= maphash.String(o.seed, name)
	}
	return nil
}
func (o *bytesBenchOwner) AppendBytes(text []byte, ends []int, _ []uint64, _ []points.Point, _ []uint64, _ int64) error {
	start := 0
	for _, end := range ends {
		o.hash ^= maphash.Bytes(o.seed, text[start:end])
		start = end
	}
	return nil
}
func (*bytesBenchOwner) Seal() error  { return nil }
func (*bytesBenchOwner) Drain() error { return nil }

// Includes plaintext parsing, name copies and the serial owner handoff.
// The owner hashes every name; storage and journal work are outside this probe.
func BenchmarkReceiverNameBytes(b *testing.B) {
	for _, size := range []int{32, 280} {
		b.Run(fmt.Sprintf("name%d", size), func(b *testing.B) {
			const count = 1024
			var payload strings.Builder
			for i := range count {
				fmt.Fprintf(&payload, "%s.%04d 1.25 1000\n", strings.Repeat("n", size-5), i)
			}
			wire := []byte(payload.String())
			o := &bytesBenchOwner{seed: maphash.MakeSeed()}
			l, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				b.Fatal(err)
			}
			s, err := Start(l, []Owner{o}, Config{BatchSize: count, SyncInterval: time.Hour})
			if err != nil {
				b.Fatal(err)
			}
			c, err := net.Dial("tcp", l.Addr().String())
			if err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.SetBytes(int64(len(wire)))
			b.ResetTimer()
			for range b.N {
				if _, err = c.Write(wire); err != nil {
					b.Fatal(err)
				}
			}
			c.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			if err = s.Wait(ctx); err != nil {
				b.Fatal(err)
			}
			b.StopTimer()
			if s.Stats().Accepted != uint64(b.N*count) {
				b.Fatal(s.Stats())
			}
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*count), "ns/point")
		})
	}
}

// Hands BatchSize-point batches to one owner through its inbox. V2_BENCH_BYTES=1
// hands plaintext bytes to a byte owner; the default hands names to a string owner.
func BenchmarkReceiverNameHandoff(b *testing.B) {
	for _, size := range []int{32, 280} {
		b.Run(fmt.Sprintf("name%d", size), func(b *testing.B) {
			const count = 1024
			text := []byte(strings.Repeat("n", size*count))
			ends := make([]int, count)
			for i := range ends {
				ends[i] = (i + 1) * size
			}
			var o Owner = stringBenchOwner{&bytesBenchOwner{seed: maphash.MakeSeed()}}
			if os.Getenv("V2_BENCH_BYTES") == "1" {
				o = &bytesBenchOwner{seed: maphash.MakeSeed()}
			}
			s := ownerOnly(o, Config{BatchSize: count, SyncInterval: time.Hour})
			p := &batch{hashes: make([]uint64, count), data: make([]points.Point, count)}
			b.ReportAllocs()
			b.SetBytes(int64(len(text)))
			b.ResetTimer()
			for range b.N {
				p.hashes, p.data = p.hashes[:count], p.data[:count]
				p.text = append(p.text[:0], text...)
				p.ends = append(p.ends[:0], ends...)
				if err := s.handOff(0, p); err != nil {
					b.Fatal(err)
				}
				p.reset()
			}
			close(s.closing)
			s.workers.Wait()
			b.StopTimer()
			if s.Stats().Accepted != uint64(b.N*count) {
				b.Fatal(s.Stats())
			}
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*count), "ns/point")
		})
	}
}

// stringBenchOwner hides AppendBytes, so the inbox keeps names as strings.
type stringBenchOwner struct{ *bytesBenchOwner }

func (o stringBenchOwner) Append(names []string, hashes []uint64, data []points.Point, sequence []uint64, clock int64) error {
	return o.bytesBenchOwner.Append(names, hashes, data, sequence, clock)
}
