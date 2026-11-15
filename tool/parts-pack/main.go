// parts-pack inspects offline fixture payload and table overhead.
package main

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/cockroachdb/pebble"
	"github.com/go-graphite/go-carbon/internal/partsformat"
)

type report struct {
	Path                                                          string
	Tables, SST, Data, Index, Filter, RawKeys, RawValues, Entries uint64
	Payloads                                                      map[string]uint64
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: parts-pack DB... | -dictionary[-bundle] SRC NEWDIR | -repack SRC NEWDIR | -repartition SRC NEWDIR [HORIZON_DIVISOR] | -sample DIR")
		os.Exit(2)
	}
	if len(os.Args) == 4 && (os.Args[1] == "-dictionary" || os.Args[1] == "-dictionary-bundle") {
		if err := compactFixtureTables(os.Args[2], os.Args[3], os.Args[1] == "-dictionary-bundle"); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}
	if len(os.Args) == 3 && os.Args[1] == "-sample" {
		if err := processSample(os.Args[2]); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}
	if (len(os.Args) == 4 || len(os.Args) == 5) && (os.Args[1] == "-repack" || os.Args[1] == "-repack-zstd" || os.Args[1] == "-repartition") {
		divisor := uint64(8)
		if len(os.Args) == 5 {
			var err error
			divisor, err = strconv.ParseUint(os.Args[4], 10, 32)
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(1)
			}
		}
		if err := repack(os.Args[2], os.Args[3], os.Args[1] == "-repack-zstd", os.Args[1] == "-repartition", uint32(divisor)); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}
	for _, path := range os.Args[1:] {
		if err := inspect(path); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}
}
func inspect(path string) error {
	opts := &pebble.Options{ReadOnly: true}
	if strings.HasSuffix(path, "legacy") {
		merger := *pebble.DefaultMerger
		merger.Name = "go-carbon.chunks.exact.v1"
		merger.Merge = func(_ []byte, _ []byte) (pebble.ValueMerger, error) {
			return nil, errors.New("inspection requires a fully compacted fixture without merge operands")
		}
		opts.Merger = &merger
	}
	db, err := pebble.Open(path, opts)
	if err != nil {
		return err
	}
	defer db.Close()
	r := report{Path: path, Payloads: map[string]uint64{}}
	tables, err := db.SSTables(pebble.WithProperties())
	if err != nil {
		return err
	}
	for _, level := range tables {
		for _, t := range level {
			p := t.Properties
			r.Tables++
			r.SST += t.Size
			r.Data += p.DataSize
			r.Index += p.IndexSize
			r.Filter += p.FilterSize
			r.RawKeys += p.RawKeySize
			r.RawValues += p.RawValueSize
			r.Entries += p.NumEntries
		}
	}
	it, err := db.NewIter(nil)
	if err != nil {
		return err
	}
	defer it.Close()
	for ok := it.First(); ok; ok = it.Next() {
		k, v := it.Key(), it.Value()
		r.Payloads["visible_keys"] += uint64(len(k))
		if len(v) >= 28 && string(v[:4]) == "GCA2" {
			p, _, points, err := oldArchive(v)
			if err != nil {
				return err
			}
			key, err := partsformat.Decode(k)
			if err != nil {
				return err
			}
			h := 28 + 8*len(p.Archives)
			r.Payloads["archive_envelope"] += uint64(h)
			r.Payloads["archive_blocks"]++
			r.Payloads["archive_points"] += uint64(len(points))
			r.Payloads["canonical_name"] += uint64(len(key.Metric))
			if binary.BigEndian.Uint32(v[24:]) == 1 && len(v) >= h+16 {
				r.Payloads["sealed_header"] += 16
				r.Payloads["sealed_body"] += uint64(len(v) - h - 16)
				r.Payloads[fmt.Sprintf("sealed_flags_%02x", v[h+1])]++
			} else {
				r.Payloads["raw_body"] += uint64(len(v) - h)
			}
		} else if len(v) >= 16 && (v[0] == 3 || v[0] == 4) {
			r.Payloads["legacy_sealed_header"] += 16
			r.Payloads["legacy_sealed_body"] += uint64(len(v) - 16)
			r.Payloads["legacy_sealed_blocks"]++
			r.Payloads["legacy_sealed_points"] += uint64(binary.LittleEndian.Uint16(v[2:]))
			r.Payloads[fmt.Sprintf("legacy_flags_%02x", v[1])]++
		} else {
			r.Payloads["other_values"] += uint64(len(v))
			r.Payloads[fmt.Sprintf("other_key_%x", k[:min(len(k), 1)])]++
		}
	}
	if err := it.Error(); err != nil {
		return err
	}
	return json.NewEncoder(os.Stdout).Encode(r)
}
