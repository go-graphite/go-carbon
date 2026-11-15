package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"math"
	"os"
	"path/filepath"
	"slices"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/partsformat"
	"github.com/go-graphite/go-carbon/internal/partspack"
)

// compactFixture is deliberately an offline stored-image diagnostic. Duplicate
// slots, mixed policy and empty images require a version/catalog protocol and
// fail closed here. In particular it is not an overlapping-part compactor.
func compactFixture(src, dst string) error {
	return compactFixtureTables(src, dst, false)
}

func compactFixtureTables(src, dst string, bundleTables bool) error {
	if err := os.Mkdir(dst, 0750); err != nil {
		return err
	}
	in, err := pebble.Open(src, &pebble.Options{ReadOnly: true})
	if err != nil {
		return err
	}
	defer in.Close()
	it, err := in.NewIter(nil)
	if err != nil {
		return err
	}
	defer it.Close()
	type group struct {
		prefix        []byte
		policy        partspack.Policy
		records       []partspack.Record
		source        hash.Hash
		bytes, points uint64
	}
	source := sha256.New()
	var points uint64
	partCount := 0
	var paths []string
	var writer *sstable.Writer
	var writerScope string
	closeTable := func() error {
		if writer == nil {
			return nil
		}
		err := writer.Close()
		writer = nil
		return err
	}
	defer func() { _ = closeTable() }()
	expected := sha256.New()
	var dictionaryBytes, encodeNS, decodeNS, blocks uint64
	writeGroup := func(g *group) error {
		id := string(g.prefix)
		identity := sha256.Sum256(append(g.source.Sum(nil), g.prefix...))
		// Reject ambiguous overlapping stored slots before discarding old PartIDs.
		type slot struct {
			name    partsformat.ID
			archive uint32
			time    int64
		}
		occupied := map[slot]bool{}
		type archiveID struct {
			name    partsformat.ID
			archive uint32
		}
		type image struct {
			key    partsformat.Key
			points []chunkstore.Point
		}
		images := map[archiveID]*image{}
		for _, r := range g.records {
			ps, err := partspack.DecodeBlock(r.Data)
			if err != nil {
				return err
			}
			for _, p := range ps {
				s := slot{r.Key.Metric, r.Key.Archive, p.Timestamp}
				if occupied[s] {
					return errors.New("duplicate stored slot requires version resolution")
				}
				occupied[s] = true
			}
			ai := archiveID{r.Key.Metric, r.Key.Archive}
			im := images[ai]
			if im == nil {
				im = &image{key: r.Key}
				images[ai] = im
			}
			im.points = append(im.points, ps...)
		}
		g.records = nil
		for _, im := range images {
			slices.SortFunc(im.points, func(a, b chunkstore.Point) int {
				if a.Timestamp < b.Timestamp {
					return -1
				}
				if a.Timestamp > b.Timestamp {
					return 1
				}
				return 0
			})
			for begin := 0; begin < len(im.points); begin += partspack.MaxPoints {
				end := min(begin+partspack.MaxPoints, len(im.points))
				key := im.key
				copy(key.Part[:], identity[:16])
				key.Ordinal = uint32(begin)
				key.BlockStart = im.points[begin].Timestamp
				data, err := partspack.EncodeBlock(im.points[begin:end], g.policy.Archives[key.Archive].Step)
				if err != nil {
					return err
				}
				g.records = append(g.records, partspack.Record{Key: key, Data: data})
			}
		}
		start := time.Now()
		packed, err := partspack.EncodePart(g.policy, g.records)
		encodeNS += uint64(time.Since(start).Nanoseconds())
		if err != nil {
			return err
		}
		start = time.Now()
		opened, err := partspack.OpenPart(packed)
		decodeNS += uint64(time.Since(start).Nanoseconds())
		if err != nil {
			return err
		}
		// Compare all restored key fields and every payload byte before publication.
		original := map[partsformat.Key][]byte{}
		for _, r := range g.records {
			original[r.Key] = r.Data
		}
		for _, r := range opened.Records {
			if !bytes.Equal(original[r.Key], r.Data) {
				return errors.New("dictionary changed portable identity or payload")
			}
		}
		key, err := partsformat.Encode(opened.Records[0].Key)
		if err != nil {
			return err
		}
		scope := id
		if bundleTables {
			first := opened.Records[0].Key
			lo, _, err := partsformat.VShardRangeForGroup(first.Group, first.VShard)
			if err != nil {
				return err
			}
			scope = string(lo)
		}
		if writer == nil || scope != writerScope || writer.EstimatedSize()+uint64(len(packed)) > 64<<20 {
			if err := closeTable(); err != nil {
				return err
			}
			path := filepath.Join(dst, fmt.Sprintf("packed-%06d.sst", len(paths)))
			f, err := vfs.Default.Create(path)
			if err != nil {
				return err
			}
			writer = sstable.NewWriter(objstorageprovider.NewFileWritable(f), sstable.WriterOptions{Compression: sstable.SnappyCompression})
			writerScope = scope
			paths = append(paths, path)
		}
		if err := writer.Set(key, packed); err != nil {
			return err
		}
		writeDigest(expected, key, packed)
		dictionaryBytes += uint64(len(packed))
		blocks += uint64(len(opened.Records))
		partCount++
		return nil
	}
	var g *group
	for ok := it.First(); ok; ok = it.Next() {
		key, value := it.Key(), it.Value()
		k, err := partsformat.Decode(key)
		if err != nil {
			return err
		}
		policy, archive, ps, err := oldArchive(value)
		if err != nil {
			return err
		}
		if archive != k.Archive || len(ps) == 0 {
			return errors.New("invalid archive identity or empty image")
		}
		prefix, err := partsformat.PartitionPrefixForGroup(k.Group, k.VShard, k.Partition)
		if err != nil {
			return err
		}
		if g == nil || !bytes.Equal(g.prefix, prefix) {
			if g != nil {
				if err := writeGroup(g); err != nil {
					return err
				}
			}
			g = &group{prefix: prefix, policy: policy, source: sha256.New()}
		}
		g.bytes += uint64(len(key) + len(value))
		g.points += uint64(len(ps))
		if g.bytes > partspack.MaxPartBytes || g.points > 1<<20 || len(g.records) >= partspack.MaxPartRecords {
			return errors.New("one source partition exceeds bounded diagnostic group; split the offline image first")
		}
		want, err := g.policy.MarshalBinary()
		if err != nil {
			return err
		}
		got, err := policy.MarshalBinary()
		if err != nil || !bytes.Equal(want, got) {
			return errors.New("mixed policies require separate publication scopes")
		}
		data, err := partspack.EncodeBlock(ps, policy.Archives[archive].Step)
		if err != nil {
			return err
		}
		decoded, err := partspack.DecodeBlock(data)
		if err != nil || len(decoded) != len(ps) {
			return errors.New("cold pack point count differs")
		}
		for i := range ps {
			if ps[i].Timestamp != decoded[i].Timestamp || math.Float64bits(ps[i].Value) != math.Float64bits(decoded[i].Value) {
				return errors.New("cold pack changed stored IEEE bits")
			}
		}
		writeDigest(source, key, value)
		writeDigest(g.source, key, value)
		g.records = append(g.records, partspack.Record{Key: k, Data: data})
		points += uint64(len(ps))
	}
	if err := it.Error(); err != nil {
		return err
	}
	if g != nil {
		if err := writeGroup(g); err != nil {
			return err
		}
	}
	if partCount == 0 {
		return errors.New("empty source")
	}
	sourceID := source.Sum(nil)
	if err := closeTable(); err != nil {
		return err
	}
	out, err := pebble.Open(filepath.Join(dst, "db"), &pebble.Options{})
	if err != nil {
		return err
	}
	defer func() {
		if out != nil {
			_ = out.Close()
		}
	}()
	if err := out.Ingest(paths); err != nil {
		return err
	}
	if err := out.Compact([]byte{0}, []byte{255}, true); err != nil {
		return err
	}
	if err := out.Close(); err != nil {
		return err
	}
	out = nil
	read, err := pebble.Open(filepath.Join(dst, "db"), &pebble.Options{ReadOnly: true})
	if err != nil {
		return err
	}
	defer read.Close()
	verify, err := read.NewIter(nil)
	if err != nil {
		return err
	}
	defer verify.Close()
	actual := sha256.New()
	var storedPoints, storedParts uint64
	for ok := verify.First(); ok; ok = verify.Next() {
		p, err := partspack.OpenPart(verify.Value())
		if err != nil {
			return err
		}
		for _, r := range p.Records {
			ps, err := partspack.DecodeBlock(r.Data)
			if err != nil {
				return err
			}
			storedPoints += uint64(len(ps))
		}
		storedParts++
		writeDigest(actual, verify.Key(), verify.Value())
	}
	if err := verify.Error(); err != nil {
		return err
	}
	if storedPoints != points || storedParts != uint64(partCount) || !bytes.Equal(expected.Sum(nil), actual.Sum(nil)) {
		return errors.New("persisted packed fixture differs")
	}
	layout, err := json.Marshal(map[string]any{"version": "offline-dictionary-v1", "production_ready": false, "partition_widths_changed": false, "sst_spans_partitions": bundleTables, "physical_expiry_requires_compaction": bundleTables})
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(dst, "layout.json"), layout, 0640); err != nil {
		return err
	}
	tables, err := read.SSTables()
	if err != nil {
		return err
	}
	size, count := uint64(len(layout)), 0
	for _, level := range tables {
		for _, table := range level {
			size += table.Size
			count++
		}
	}
	return json.NewEncoder(os.Stdout).Encode(map[string]any{
		"stored_points": points, "blocks": blocks, "parts": partCount, "ssts": count,
		"sst_and_metadata_bytes": size, "bytes_per_point": float64(size) / float64(points),
		"packed_value_bytes": dictionaryBytes, "bit_exact": true, "partition_widths_changed": false,
		"source_key_value_sha256": fmt.Sprintf("%x", sourceID), "persisted_key_value_sha256": fmt.Sprintf("%x", actual.Sum(nil)),
		"sst_spans_partitions": bundleTables, "physical_expiry_requires_compaction": bundleTables,
		"encode_wall_ns_per_point": float64(encodeNS) / float64(points),
		"open_wall_ns_per_point":   float64(decodeNS) / float64(points),
		"performance_qualified":    false,
	})
}
