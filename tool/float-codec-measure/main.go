// float-codec-measure compares offline candidates on captured render responses.
package main

import (
	"compress/gzip"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"

	"github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/floatresearch"
)

type row struct {
	Kind                                                           string
	Points, Groups, Bytes                                          int
	EncodeNS, DecodeNS                                             float64
	EncodeAllocBytes, EncodeAllocs, DecodeAllocBytes, DecodeAllocs float64
	Winners                                                        map[string]int
}
type report struct {
	File, Go, Arch        string
	Series, PresentPoints int
	Rows                  []row
}

var decodeScratch chunkstore.SealedPointBuffer
var codec = floatresearch.Codec{EncodeCurrent: chunkstore.EncodeSealedPoints, DecodeCurrent: decodeScratch.Decode}

func encode(kind string, g floatresearch.Group) ([]byte, string, error) {
	if kind == "production" || kind == "native-min" || strings.HasPrefix(kind, "production-prefix") {
		var b []byte
		var e error
		switch kind {
		case "production":
			b, e = chunkstore.EncodeSealedPointsFloat(g.Points, g.Step)
		case "native-min":
			b, e = chunkstore.EncodeSealedPointsFloatMin(g.Points, g.Step)
		default:
			n := 256
			if kind == "production-prefix64" {
				n = 64
			}
			b, e = chunkstore.EncodeSealedPointsFloatPrefix(g.Points, g.Step, n)
		}
		winner := "sealed"
		if len(b) > 0 && b[0] == 4 {
			winner = "native"
		}
		return b, winner, e
	}
	return codec.Variant(kind, g)
}

func decode(kind string, b []byte) ([]chunkstore.Point, error) {
	if kind == "native" {
		return decodeScratch.Decode(b)
	}
	return codec.DecodeVariant(kind, b)
}

func cpu() int64 {
	var r syscall.Rusage
	if e := syscall.Getrusage(syscall.RUSAGE_SELF, &r); e != nil {
		panic(e)
	}
	return (int64(r.Utime.Sec)+int64(r.Stime.Sec))*1e9 + (int64(r.Utime.Usec)+int64(r.Stime.Usec))*1000
}
func check(e error) {
	if e != nil {
		panic(e)
	}
}
func main() {
	input := flag.String("input", "", "capture json.gz glob")
	output := flag.String("out", "", "output directory")
	kinds := flag.String("kinds", strings.Join(floatresearch.VariantKinds, ","), "policies")
	repeat := flag.Int("repeat", 3, "timing repetitions")
	skip := flag.Bool("skip-existing", false, "skip completed reports")
	flag.Parse()
	paths, e := filepath.Glob(*input)
	check(e)
	if len(paths) == 0 {
		panic("no captures")
	}
	check(os.MkdirAll(*output, 0755))
	for _, path := range paths {
		if *skip {
			if _, e := os.Stat(filepath.Join(*output, filepath.Base(path)+".json")); e == nil {
				continue
			}
		}
		r, groups := load(path)
		for _, kind := range strings.Split(*kinds, ",") {
			rr := row{Kind: kind, Groups: len(groups), Winners: map[string]int{}}
			encoded := make([][]byte, len(groups))
			winners := make([]string, len(groups))
			for i, g := range groups {
				b, w, e := encode(kind, g)
				check(e)
				p, e := decode(w, b)
				check(e)
				if !floatresearch.IEEEEqual(g.Points, p) {
					panic(fmt.Sprintf("oracle %s %s group %d", path, kind, i))
				}
				encoded[i], winners[i] = b, w
				rr.Points += len(g.Points)
				rr.Bytes += len(b)
				rr.Winners[w]++
			}
			runtime.GC()
			var before, after runtime.MemStats
			runtime.ReadMemStats(&before)
			start := cpu()
			for n := 0; n < *repeat; n++ {
				for _, g := range groups {
					_, _, e := encode(kind, g)
					check(e)
				}
			}
			rr.EncodeNS = float64(cpu()-start) / float64(rr.Points**repeat)
			runtime.ReadMemStats(&after)
			rr.EncodeAllocBytes = float64(after.TotalAlloc-before.TotalAlloc) / float64(rr.Points**repeat)
			rr.EncodeAllocs = float64(after.Mallocs-before.Mallocs) / float64(rr.Points**repeat)
			runtime.GC()
			runtime.ReadMemStats(&before)
			start = cpu()
			for n := 0; n < *repeat; n++ {
				for i, b := range encoded {
					_, e := decode(winners[i], b)
					check(e)
				}
			}
			rr.DecodeNS = float64(cpu()-start) / float64(rr.Points**repeat)
			runtime.ReadMemStats(&after)
			rr.DecodeAllocBytes = float64(after.TotalAlloc-before.TotalAlloc) / float64(rr.Points**repeat)
			rr.DecodeAllocs = float64(after.Mallocs-before.Mallocs) / float64(rr.Points**repeat)
			r.Rows = append(r.Rows, rr)
			fmt.Fprintf(os.Stderr, "%s %-16s %d bytes %.2f/%.2f ns/pt\n", filepath.Base(path), kind, rr.Bytes, rr.EncodeNS, rr.DecodeNS)
		}
		b, e := json.MarshalIndent(r, "", "  ")
		check(e)
		check(os.WriteFile(filepath.Join(*output, filepath.Base(path)+".json"), b, 0644))
	}
}
func load(path string) (report, []floatresearch.Group) {
	r := report{File: path, Go: runtime.Version(), Arch: runtime.GOARCH}
	f, e := os.Open(path)
	check(e)
	defer f.Close()
	z, e := gzip.NewReader(f)
	check(e)
	defer z.Close()
	d := json.NewDecoder(z)
	_, e = d.Token()
	check(e)
	var groups []floatresearch.Group
	for d.More() {
		_, e = d.Token()
		check(e)
		var s struct {
			Start, Step int
			Values      []json.RawMessage
		}
		check(d.Decode(&s))
		r.Series++
		var p []chunkstore.Point
		for i, v := range s.Values {
			if string(v) != "null" {
				x, e := strconv.ParseFloat(strings.Trim(string(v), "\""), 64)
				check(e)
				p = append(p, chunkstore.Point{Timestamp: int64(s.Start + i*s.Step), Value: x})
			}
		}
		r.PresentPoints += len(p)
		for first := 0; first < len(p); first += 4096 {
			g := floatresearch.Group{Points: p[first:min(first+4096, len(p))], Step: s.Step}
			b, e := chunkstore.EncodeSealedPoints(g.Points, g.Step)
			check(e)
			got, e := chunkstore.DecodeSealedPoints(b)
			check(e)
			if !floatresearch.IEEEEqual(g.Points, got) {
				panic("current oracle")
			}
			if b[1]&(1|16) == 0 {
				groups = append(groups, g)
			}
		}
	}
	_, e = d.Token()
	check(e)
	if _, e = d.Token(); !errors.Is(e, io.EOF) {
		panic("trailing data")
	}
	return r, groups
}
