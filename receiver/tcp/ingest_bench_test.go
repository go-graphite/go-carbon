package tcp

import (
	"bytes"
	"compress/gzip"
	"io"
	"math"
	"os"
	"strconv"
	"testing"

	"github.com/go-graphite/go-carbon/cache"
	"github.com/go-graphite/go-carbon/points"
	"github.com/go-graphite/go-carbon/receiver/parse"
	"github.com/go-graphite/go-carbon/receiver/stream/codec"
	"go.uber.org/zap"
)

// BenchmarkIngestCapture includes the real receiver and cache. A bounded capture
// prefix is replayed eight times, advancing timestamps by one minute per epoch.
// Private names remain in memory and are never printed or saved by this test.
// Run with STREAM_SAMPLE_FILE and -benchtime=1x to bound cache growth.
func BenchmarkIngestCapture(b *testing.B) {
	path := os.Getenv("STREAM_SAMPLE_FILE")
	if path == "" {
		b.Skip("set STREAM_SAMPLE_FILE to plaintext or gzip input")
	}
	f, err := os.Open(path)
	if err != nil {
		b.Fatal(err)
	}
	defer f.Close()
	var src io.Reader = f
	head := make([]byte, 2)
	{
		if _, err = io.ReadFull(f, head); err != nil {
			b.Fatal(err)
		}
		if _, err = f.Seek(0, 0); err != nil {
			b.Fatal(err)
		}
		if bytes.Equal(head, []byte{31, 139}) {
			z, e := gzip.NewReader(f)
			if e != nil {
				b.Fatal(e)
			}
			defer z.Close()
			src = z
		}
	}
	// Limit input memory independently of the complete capture's size.
	raw, err := io.ReadAll(io.LimitReader(src, 48<<20))
	if err != nil {
		b.Fatal(err)
	}
	var base []byte
	var names [][]byte
	for len(raw) > 0 && len(names) < 131072 {
		line, tail, ok := bytes.Cut(raw, []byte{'\n'})
		raw = tail
		if !ok {
			break
		}
		name, _, _, err := parse.PlainLine(line)
		if err != nil {
			b.Fatal("invalid capture point")
		}
		names = append(names, bytes.Clone(name))
		base = append(base, line...)
		base = append(base, '\n')
	}
	if len(names) == 0 {
		b.Fatal("empty capture")
	}
	var plain []byte
	expected := map[string][]points.Point{}
	for epoch := range 8 {
		for rest := base; len(rest) > 0; {
			line, tail, _ := bytes.Cut(rest, []byte{'\n'})
			rest = tail
			name, value, ts, _ := parse.PlainLine(line)
			ts += int64(epoch) * 60
			plain = append(plain, name...)
			plain = append(plain, ' ')
			plain = strconv.AppendFloat(plain, value, 'g', -1, 64)
			plain = append(plain, ' ')
			plain = strconv.AppendInt(plain, ts, 10)
			plain = append(plain, '\n')
			expected[string(name)] = append(expected[string(name)], points.Point{Value: value, Timestamp: ts})
		}
	}
	rows := len(names) * 8
	e, err := codec.NewEncoder(codec.Options{Flags: codec.FlagZstd})
	if err != nil {
		b.Fatal(err)
	}
	defer e.Close()
	stream, _ := codec.AppendPreamble(nil, codec.FlagZstd)
	for rest := plain; len(rest) > 0; {
		line, tail, _ := bytes.Cut(rest, []byte{'\n'})
		rest = tail
		name, v, ts, err := parse.PlainLine(line)
		if err != nil {
			b.Fatal(err)
		}
		if err = e.Add(name, v, ts); err != nil {
			b.Fatal(err)
		}
		if e.Ready() {
			stream, err = e.Flush(stream)
			if err != nil {
				b.Fatal(err)
			}
		}
	}
	stream, err = e.Flush(stream)
	if err != nil {
		b.Fatal(err)
	}
	compressed := encodeFrames(b, plain, 1<<20)
	for _, mode := range []string{"plaintext", "zstd-plaintext", "stream", "stream-ref"} {
		b.Run(mode, func(b *testing.B) {
			c := cache.New()
			c.SetMaxSize(0)
			// Resident series are common to all four cases; only stream-ref skips the
			// name hash/table probe after each ID has first resolved its slot.
			unique := map[string]bool{}
			for _, name := range names {
				if !unique[string(name)] {
					unique[string(name)] = true
					c.AddPoint(name, 0, 0)
				}
			}
			r := &TCP{logger: zap.NewNop(), decompressor: newDecompressor("")}
			r.SetPointOut(c.AddPoint)
			r.SetPointsOut(c.AddPoints)
			if mode == "stream-ref" {
				r.SetStreamPointOut(c.AddStreamPoint)
			}
			r.Start()
			defer r.Stop()
			input := plain
			if mode == "zstd-plaintext" {
				input = compressed
			}
			if mode == "stream" || mode == "stream-ref" {
				input = stream
			}
			conn := &benchmarkReadConn{}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				conn.Reader.Reset(input)
				r.HandleConnection(conn)
			}
			b.StopTimer()
			n := 0
			for name := range unique {
				data := c.Get(name)[1:]
				n += len(data)
				want := expected[name]
				for i, p := range data {
					w := want[i%len(want)]
					if p.Timestamp != w.Timestamp || math.Float64bits(p.Value) != math.Float64bits(w.Value) {
						b.Fatal("ingest changed a captured point")
					}
				}
			}
			if n != b.N*rows {
				b.Fatalf("%s lost points: %d", mode, n)
			}
			units := float64(b.N * rows)
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/units, "ns/point")
			b.ReportMetric(float64(len(input))/float64(rows), "wire-B/point")
			b.Logf("%d rows, %d resident series, capture prefix x8", rows, len(unique))
		})
	}
}
