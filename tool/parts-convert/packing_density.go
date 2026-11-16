//go:build linux || darwin

package main

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/go-graphite/go-carbon/internal/partsconvert"
	"github.com/go-graphite/go-carbon/internal/partsformat"
)

// Physical categories are disjoint and cover every byte of each live SST.
// Raw categories describe pre-compression bytes, not additive disk costs.
type packingDensity struct {
	SSTs, SSTBytes, DataBytes, IndexBytes, FilterBytes                     uint64
	PropertiesBytes, MetaIndexBytes, FooterBytes, OtherBytes               uint64
	Records, Points, RawKeyBytes, RawValueBytes, SchemaBytes, PayloadBytes uint64
	NameBytes, PartIDBytes, PartitionBytes                                 uint64
	UniquePartIDs, UniqueNames, PartitionIndexBytes, OtherTargetFileBytes  uint64
	AllTargetSSTBytes                                                      uint64
	MinTimestamp, MaxTimestamp                                             int64
	PointsPerRecord, Codecs, Compressions, ArchiveSteps                    map[string]uint64
	NonSSTBytesByDirectory                                                 map[string]uint64
}

func inspectPackingDensity(ctx context.Context, target string) (packingDensity, error) {
	r := packingDensity{PointsPerRecord: map[string]uint64{}, Codecs: map[string]uint64{}, Compressions: map[string]uint64{}, ArchiveSteps: map[string]uint64{}, NonSSTBytesByDirectory: map[string]uint64{}}
	dir := filepath.Join(target, "parts")
	db, err := pebble.Open(dir, packingOptions(true))
	if err != nil {
		return r, err
	}
	defer db.Close()
	levels, err := db.SSTables()
	if err != nil {
		return r, err
	}
	blockBytes := func(b sstable.BlockHandle) uint64 {
		if b.Length == 0 {
			return 0
		}
		return b.Length + 5
	}
	for _, level := range levels {
		for _, table := range level {
			if err = ctx.Err(); err != nil {
				return r, err
			}
			f, e := os.Open(filepath.Join(dir, fmt.Sprintf("%06d.sst", table.FileNum)))
			if e != nil {
				return r, e
			}
			readable, e := sstable.NewSimpleReadable(f)
			if e != nil {
				_ = f.Close()
				return r, e
			}
			reader, e := sstable.NewReader(readable, sstable.ReaderOptions{})
			if e != nil {
				_ = readable.Close()
				return r, e
			}
			layout, e := reader.Layout()
			if e != nil {
				_ = reader.Close()
				return r, e
			}
			var data, index uint64
			for _, b := range layout.Data {
				data += blockBytes(b.BlockHandle)
			}
			for _, b := range layout.Index {
				index += blockBytes(b)
			}
			index += blockBytes(layout.TopIndex)
			filter, props, meta, footer := blockBytes(layout.Filter), blockBytes(layout.Properties), blockBytes(layout.MetaIndex), layout.Footer.Length
			accounted := data + index + filter + props + meta + footer
			if accounted > table.Size {
				_ = reader.Close()
				return r, errors.New("SST layout exceeds file size")
			}
			r.SSTs++
			r.SSTBytes += table.Size
			r.DataBytes += data
			r.IndexBytes += index
			r.FilterBytes += filter
			r.PropertiesBytes += props
			r.MetaIndexBytes += meta
			r.FooterBytes += footer
			r.OtherBytes += table.Size - accounted
			r.Compressions[reader.Properties.CompressionName]++
			if e = reader.Close(); e != nil {
				return r, e
			}
		}
	}
	it, err := db.NewIter(nil)
	if err != nil {
		return r, err
	}
	defer it.Close()
	partIDs := map[partsformat.PartID]struct{}{}
	names := map[partsformat.ID]struct{}{}
	for ok := it.First(); ok; ok = it.Next() {
		if err = ctx.Err(); err != nil {
			return r, err
		}
		key, e := partsformat.Decode(it.Key())
		if e != nil {
			return r, e
		}
		m, archive, points, e := partsconvert.DecodeArchive(it.Value())
		if e != nil {
			return r, e
		}
		partIDs[key.Part] = struct{}{}
		names[key.Metric] = struct{}{}
		r.ArchiveSteps[fmt.Sprint(m.Archives[archive].Step)]++
		r.RawKeyBytes += uint64(len(it.Key()))
		r.RawValueBytes += uint64(len(it.Value()))
		header := uint64(28 + 8*len(m.Archives))
		r.SchemaBytes += header
		r.PayloadBytes += uint64(len(it.Value())) - header
		r.NameBytes += uint64(len(key.Metric))
		r.PartIDBytes += 16
		r.PartitionBytes += 8
		r.PointsPerRecord[fmt.Sprint(len(points))]++
		r.Codecs[fmt.Sprint(binary.BigEndian.Uint32(it.Value()[24:]))]++
		for _, p := range points {
			if r.Points == 0 || p.Timestamp < r.MinTimestamp {
				r.MinTimestamp = p.Timestamp
			}
			if r.Points == 0 || p.Timestamp > r.MaxTimestamp {
				r.MaxTimestamp = p.Timestamp
			}
			r.Points++
		}
		r.Records++
	}
	if err = it.Error(); err != nil {
		return r, err
	}
	r.UniquePartIDs, r.UniqueNames = uint64(len(partIDs)), uint64(len(names))
	err = filepath.WalkDir(target, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if entry.IsDir() || entry.Type()&os.ModeSymlink != 0 {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if path == filepath.Join(dir, "PARTITION-INDEX.json") {
			r.PartitionIndexBytes += uint64(info.Size())
		} else if filepath.Ext(path) != ".sst" {
			r.OtherTargetFileBytes += uint64(info.Size())
		}
		if filepath.Ext(path) == ".sst" {
			r.AllTargetSSTBytes += uint64(info.Size())
		} else {
			rel, err := filepath.Rel(target, path)
			if err != nil {
				return err
			}
			group, _, nested := strings.Cut(rel, string(filepath.Separator))
			if !nested {
				group = "."
			}
			r.NonSSTBytesByDirectory[group] += uint64(info.Size())
		}
		return nil
	})
	return r, err
}
