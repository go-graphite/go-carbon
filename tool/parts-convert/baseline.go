//go:build linux || darwin

package main

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/partsconvert"
	"github.com/go-graphite/go-carbon/internal/partsformat"
	"github.com/go-graphite/go-carbon/internal/whisperio"
	whisper "github.com/go-graphite/go-whisper"
)

type comparison struct {
	Points, LegacySSTBytes, ConvertedSSTBytes          uint64
	LegacyBytesPerPoint, ConvertedBytesPerPoint, Ratio float64
	ColdPackPoints, ColdPackBlocks                     uint64
	BitExact                                           bool
}
type importCodec struct {
	whisperio.ContinuationCodec
	path string
}

func (c *importCodec) Create(ctx context.Context, kv chunkstore.ContinuationIO, m chunkstore.Metadata, now func() time.Time) error {
	return c.ContinuationCodec.Import(ctx, kv, c.path, m, now)
}

// compare builds actual current-engine native continuation/cold-pack SSTs and
// converted parts from the exact same stored archive image. It is deliberately
// small and sequential; it measures storage equality, not live writer CPU.
func compare(ctx context.Context, c config) (comparison, error) {
	var out comparison
	if err := os.Mkdir(c.dir, 0750); err != nil {
		return out, err
	}
	codec := &importCodec{}
	clock := func() time.Time { return time.Unix(1800000000, 0) }
	legacy, err := chunkstore.Open(filepath.Join(c.dir, "legacy"), chunkstore.Options{ExactBlocks: true, TimeBuckets: true, Continuation: codec, Now: clock, CacheSize: 16 << 20, MemTableSize: 8 << 20})
	if err != nil {
		return out, err
	}
	defer func() {
		if legacy != nil {
			_ = legacy.Close()
		}
	}()
	legacy.StopBackground()
	images := make(map[partsconvert.Unit][]partsconvert.StoredMetric)
	for shard := 0; shard < c.shards; shard++ {
		u := partsconvert.Unit{Group: "synthetic", VShard: uint16(shard)}
		source := synthetic(c, u, "comparison-stored-v2")
		err = source.ScanMetrics(ctx, u, func(m partsconvert.StoredMetric) error {
			path := filepath.Join(c.dir, "source.wsp")
			defs := make(whisper.Retentions, len(m.Archives))
			for i, a := range m.Archives {
				r := whisper.NewRetention(int(a.Step), int(a.Slots))
				defs[i] = &r
			}
			seedNow := time.Unix(1728000000, 0)
			w, err := whisper.CreateWithOptions(path, defs, whisper.Average, .5, &whisper.Options{Compressed: true, Now: func() time.Time { return seedNow }})
			if err != nil {
				return err
			}
			for _, a := range m.Archives {
				points := make([]whisper.TimeSeriesPoint, len(a.Points))
				ptr := make([]*whisper.TimeSeriesPoint, len(points))
				for i, p := range a.Points {
					points[i] = whisper.TimeSeriesPoint{Time: int(p.Timestamp), Value: math.Float64frombits(p.ValueBits)}
					ptr[i] = &points[i]
				}
				seedNow = time.Unix(a.Points[len(a.Points)-1].Timestamp+int64(a.Step), 0)
				if err = w.UpdateManyForArchive(ptr, int(a.Step)*int(a.Slots)); err != nil {
					_ = w.Close()
					return err
				}
			}
			if err = w.Close(); err != nil {
				return err
			}
			readOnly := os.O_RDONLY
			w, err = whisper.OpenWithOptions(path, &whisper.Options{OpenFileFlag: &readOnly, Now: clock})
			if err != nil {
				return err
			}
			cfg := chunkstore.MetricConfig{Name: string(m.Name), AggregationMethod: chunkstore.Average, XFilesFactor: .5}
			for ai, a := range m.Archives {
				stored, err := w.ArchivePoints(ai)
				if err != nil {
					_ = w.Close()
					return err
				}
				m.Archives[ai].Points = make([]partsconvert.StoredPoint, len(stored))
				for i, p := range stored {
					m.Archives[ai].Points[i] = partsconvert.StoredPoint{Timestamp: int64(p.Time), ValueBits: math.Float64bits(p.Value)}
				}
				cfg.Retentions = append(cfg.Retentions, chunkstore.Retention{Step: int(a.Step), Count: int(a.Slots)})
				out.Points += uint64(len(stored))
			}
			if err = w.Close(); err != nil {
				return err
			}
			codec.path = path
			if _, err = legacy.Create(ctx, cfg); err != nil {
				return err
			}
			images[u] = append(images[u], m)
			return os.Remove(path)
		})
		if err != nil {
			return out, err
		}
	}
	if err = legacy.ConfigureColdPack(chunkstore.ColdPackOptions{Mode: "exact", PackTails: true, Workers: 1, WriteWindow: time.Second, CPUFraction: 1, BytesPerSecond: 1 << 40, MinFreeBytes: 1, MaxCompactionDebt: 1 << 40}); err != nil {
		return out, err
	}
	if err = legacy.RunColdPack(ctx); err != nil {
		return out, err
	}
	st := legacy.ColdPackStats()
	out.ColdPackPoints = st.Points
	out.ColdPackBlocks = st.Blocks
	if st.Errors != 0 || st.Skipped != 0 || st.Conflicts != 0 {
		return out, fmt.Errorf("incomplete cold-pack baseline: %+v", st)
	}
	if err = legacy.Compact(); err != nil {
		return out, err
	}
	if err = legacy.Close(); err != nil {
		return out, err
	}
	legacy = nil
	out.LegacySSTBytes, err = sstBytes(filepath.Join(c.dir, "legacy"))
	if err != nil {
		return out, err
	}
	db, err := pebble.Open(filepath.Join(c.dir, "converted"), &pebble.Options{})
	if err != nil {
		return out, err
	}
	defer func() {
		if db != nil {
			_ = db.Close()
		}
	}()
	target := partsconvert.PebbleTarget{DB: db, DestinationID: "comparison-destination-v2"}
	for u, metrics := range images {
		source := synthetic(c, u, "comparison-stored-v2")
		source.ScanMetrics = func(_ context.Context, _ partsconvert.Unit, y func(partsconvert.StoredMetric) error) error {
			for _, m := range metrics {
				if err := y(m); err != nil {
					return err
				}
			}
			return nil
		}
		conv := partsconvert.Converter{Directory: filepath.Join(c.dir, "journal"), Source: source, Target: target, Space: &partsconvert.SpaceGuard{Path: c.dir}, ChunkBytes: 32 << 20, MaxTables: 100000}
		r, err := conv.Run(ctx, u)
		if err != nil {
			return out, err
		}
		// Compare every decoded stored point, independent from SST ordering.
		want := make(map[string]uint64)
		for _, m := range metrics {
			for ai, a := range m.Archives {
				for _, p := range a.Points {
					want[fmt.Sprintf("%s:%d:%d", m.Name, ai, p.Timestamp)] = p.ValueBits
				}
			}
		}
		for _, table := range r.Tables {
			it, err := db.NewIter(&pebble.IterOptions{LowerBound: table.Prefix, UpperBound: partsformat.PrefixEnd(table.Prefix)})
			if err != nil {
				return out, err
			}
			for ok := it.First(); ok; ok = it.Next() {
				key, err := partsformat.Decode(it.Key())
				if err != nil {
					_ = it.Close()
					return out, err
				}
				_, ai, points, err := partsconvert.DecodeArchive(it.Value())
				if err != nil {
					_ = it.Close()
					return out, err
				}
				for _, p := range points {
					id := fmt.Sprintf("%s:%d:%d", key.Metric, ai, p.Timestamp)
					bits, ok := want[id]
					if !ok || bits != p.ValueBits {
						_ = it.Close()
						return out, errors.New("baseline/converted archive bits differ")
					}
					delete(want, id)
				}
			}
			if err = errors.Join(it.Error(), it.Close()); err != nil {
				return out, err
			}
		}
		if len(want) != 0 {
			return out, errors.New("baseline/converted archive point missing")
		}
	}
	if err = db.Flush(); err != nil {
		return out, err
	}
	if err = db.Compact([]byte{0}, []byte{255}, true); err != nil {
		return out, err
	}
	if err = db.Close(); err != nil {
		return out, err
	}
	db = nil
	out.ConvertedSSTBytes, err = sstBytes(filepath.Join(c.dir, "converted"))
	if err != nil {
		return out, err
	}
	if out.Points == 0 {
		return out, errors.New("empty compression comparison")
	}
	out.BitExact = true
	out.LegacyBytesPerPoint = float64(out.LegacySSTBytes) / float64(out.Points)
	out.ConvertedBytesPerPoint = float64(out.ConvertedSSTBytes) / float64(out.Points)
	out.Ratio = float64(out.ConvertedSSTBytes) / float64(out.LegacySSTBytes)
	return out, nil
}
func sstBytes(root string) (uint64, error) {
	var total uint64
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() && strings.HasSuffix(path, ".sst") {
			total += uint64(info.Size())
		}
		return err
	})
	return total, err
}
