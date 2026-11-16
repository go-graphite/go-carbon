package v2daemon

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/partlog"
	"github.com/go-graphite/go-carbon/internal/v2core"
	"github.com/go-graphite/go-carbon/points"
)

func TestLayersPreserveExactCoreAndJournal(t *testing.T) {
	bits := []uint64{0, 1 << 63, 1, 0x7ff0000000000000, 0xfff0000000000000, 0x7ff8000000000042, 0x7ff0000000000001, 0x3ff0000000000000}
	for layer := 0; layer <= 2; layer++ {
		t.Run(fmt.Sprint(layer), func(t *testing.T) {
			root := t.TempDir()
			cfg := Config{Layer: layer, Producers: 2, Shards: 2, GroupInterval: time.Hour}
			s, err := Open(root, cfg)
			if err != nil {
				t.Fatal(err)
			}
			closed := false
			defer func() {
				if !closed {
					_ = s.Close()
				}
			}()
			want := map[uint64][]v2core.Row{}
			for round := 0; round < 8; round++ {
				for p, producer := range s.Producers {
					names := []string{"same.name", "other\x00name"}
					data := make([]points.Point, 2)
					seq := []uint64{uint64(round + 1), uint64(round + 1)}
					rows := make([]v2core.Row, 2)
					for i := range data {
						data[i] = points.Point{Timestamp: int64(round - 4), Value: math.Float64frombits(bits[(round+i)%len(bits)])}
						id := NumericID(uint64(i+1), p, 2)
						rows[i] = v2core.Row{ID: id, Time: data[i].Timestamp, Bits: math.Float64bits(data[i].Value), Seq: uint64(round + 1)}
						want[id] = append(want[id], rows[i])
					}
					if layer == 0 {
						err = producer.AppendNumeric(rows)
					} else {
						err = producer.Append(names, data, seq, int64(100+round))
					}
					if err != nil {
						t.Fatal(err)
					}
					// Borrowed caller buffers can be reused immediately.
					clear(data)
					clear(rows)
					clear(names)
					if err = producer.Seal(); err != nil {
						t.Fatal(err)
					}
				}
			}
			if layer == 2 {
				for _, p := range s.Producers {
					if p.SyncGroups != 0 {
						t.Fatal("barriers forced group sync")
					}
				}
			}
			if err = s.Close(); err != nil {
				t.Fatal(err)
			}
			closed = true
			if layer < 2 {
				if _, err = Open(root, cfg); err == nil {
					t.Fatal("non-durable reopen accepted")
				}
			} else {
				recovered, e := Open(root, cfg)
				if e != nil {
					t.Fatal(e)
				}
				if e = recovered.Close(); e != nil {
					t.Fatal(e)
				}
			}
			reader, err := v2core.Open(filepath.Join(root, "placement-group-00000001", "core"), coreConfig(cfg), v2core.Hooks{})
			if err != nil {
				t.Fatal(err)
			}
			defer reader.Close()
			for id, expected := range want {
				snap := reader.DBs()[v2core.Shard(id, cfg.Shards)].NewSnapshot()
				got, err := v2core.Read(snap, id)
				_ = snap.Close()
				if err != nil || !slices.Equal(got, expected) {
					t.Fatalf("id %d got %#v want %#v: %v", id, got, expected, err)
				}
			}
			if layer != 2 {
				return
			}
			for p := 0; p < 2; p++ {
				dir := filepath.Join(root, "placement-group-00000001", "journal", fmt.Sprintf("producer-%03d", p))
				catalog, err := partlog.OpenNameCatalog(filepath.Join(dir, "catalog"))
				if err != nil {
					t.Fatal(err)
				}
				decoder := partlog.CatalogDecoder{Catalog: catalog}
				file, err := os.Open(filepath.Join(dir, "segment.pdc4"))
				if err != nil {
					t.Fatal(err)
				}
				rounds := 0
				for {
					var h [4]byte
					_, err = io.ReadFull(file, h[:])
					if err == io.EOF {
						break
					}
					if err != nil {
						t.Fatal(err)
					}
					b := make([]byte, binary.LittleEndian.Uint32(h[:]))
					if _, err = io.ReadFull(file, b); err != nil {
						t.Fatal(err)
					}
					batches, cursor, err := DecodeApplications(b, &decoder, catalog, p, 2)
					if err != nil || len(batches) != 2 || cursor != uint64((rounds+1)*2) {
						t.Fatal("changed application packet", err)
					}
					for i, batch := range batches {
						if batch.Clock != int64(100+rounds) || len(batch.Points) != 1 {
							t.Fatal(batch)
						}
						point := batch.Points[0]
						if point.Time != int64(rounds-4) || point.Bits != bits[(rounds+i)%len(bits)] || point.Seq != uint64(rounds+1) {
							t.Fatal(point)
						}
					}
					rounds++
				}
				if rounds != 8 {
					t.Fatal("lost journal records", rounds)
				}
				_ = file.Close()
				decoder.Close()
				if err = catalog.Close(); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

func TestPendingQuotaAndStickyFailure(t *testing.T) {
	s, err := Open(t.TempDir(), Config{Layer: 2, Producers: 1, Shards: 1, MaxPendingRows: 1, GroupInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	p := s.Producers[0]
	if err = p.Append([]string{"a"}, []points.Point{{Timestamp: 1, Value: 1}}, []uint64{1}, 1); err != nil {
		t.Fatal(err)
	}
	if p.SyncGroups != 1 || p.ForcedGroups != 1 || len(p.packets) != 0 {
		t.Fatal("quota did not durably release rows")
	}
	if err = p.Append([]string{"a"}, nil, nil, 2); err == nil {
		t.Fatal("bad batch accepted")
	}
	if err = p.Seal(); err == nil {
		t.Fatal("failed stream kept publishing")
	}
}

func TestCatalogAdmissionAfterSeal(t *testing.T) {
	root := t.TempDir()
	cfg := Config{Layer: 1, Producers: 1, Shards: 1}
	s, err := Open(root, cfg)
	if err != nil {
		t.Fatal(err)
	}
	closed := false
	defer func() {
		if !closed {
			_ = s.Close()
		}
	}()
	p := s.Producers[0]
	appendPoint := func(name string, stamp int64) {
		t.Helper()
		if err := p.Append([]string{name}, []points.Point{{Timestamp: stamp, Value: float64(stamp)}}, []uint64{uint64(stamp)}, stamp); err != nil {
			t.Fatal(err)
		}
	}
	appendPoint("a", 1)
	if err = p.Seal(); err != nil {
		t.Fatal(err)
	}
	appendPoint("a", 2)
	appendPoint("b", 3)
	appendPoint("a", 4)
	if err = p.Seal(); err != nil {
		t.Fatal(err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	closed = true
	reader, err := v2core.Open(filepath.Join(root, "placement-group-00000001", "core"), coreConfig(cfg), v2core.Hooks{})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	snap := reader.DBs()[0].NewSnapshot()
	defer snap.Close()
	for id, want := range map[uint64][]int64{1: {1, 2, 4}, 2: {3}} {
		rows, err := v2core.Read(snap, id)
		if err != nil || len(rows) != len(want) {
			t.Fatal(id, rows, err)
		}
		for i, row := range rows {
			if row.Time != want[i] || row.Seq != uint64(want[i]) || row.Bits != math.Float64bits(float64(want[i])) {
				t.Fatal(row)
			}
		}
	}
}
