package main

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/whisperio"
)

type sortCorpusRow struct {
	Metric string `json:"metric"`
	Path   string `json:"path"`
	Files  map[string]struct {
		Bytes  int64  `json:"bytes"`
		SHA256 string `json:"sha256"`
	} `json:"files"`
}

type sortGoldenRow struct {
	Metric string
	Points uint64
	SHA256 string
}

func sortCorpus(tb testing.TB) (string, []sortCorpusRow, int64) {
	tb.Helper()
	root, manifest := os.Getenv("GRAPHITE_SORT_CORPUS"), os.Getenv("GRAPHITE_SORT_MANIFEST")
	if root == "" || manifest == "" {
		tb.Skip("requires explicit retained production corpus and pinned manifest")
	}
	data, err := os.ReadFile(manifest)
	if err != nil {
		tb.Fatal(err)
	}
	var input struct{ Rows []sortCorpusRow }
	if err := json.Unmarshal(data, &input); err != nil || len(input.Rows) == 0 {
		tb.Fatalf("invalid production corpus manifest: %v", err)
	}
	var bytes int64
	for _, row := range input.Rows {
		if !filepath.IsLocal(row.Path) || len(row.Files) != 1 {
			tb.Fatalf("expected immutable main-file fixture: %s", row.Path)
		}
		data, err := os.ReadFile(filepath.Join(root, row.Path))
		if err != nil {
			tb.Fatal(err)
		}
		pin := row.Files["main"]
		sum := sha256.Sum256(data)
		if int64(len(data)) != pin.Bytes || hex.EncodeToString(sum[:]) != pin.SHA256 {
			tb.Fatalf("production fixture identity changed: %s", row.Path)
		}
		bytes += int64(len(data))
	}
	return root, input.Rows, bytes
}

// Includes all archive timestamps and Float64 payload bits, including signed
// zero and NaN payloads. Generation/ID are storage identities, not transferred
// archive contents, so only the policy and archive data enter this fingerprint.
func sortSnapshotFingerprint(tb testing.TB, snapshot chunkstore.Snapshot) sortGoldenRow {
	tb.Helper()
	h := sha256.New()
	config, err := json.Marshal(snapshot.Metadata.MetricConfig)
	if err != nil {
		tb.Fatal(err)
	}
	h.Write(config)
	var value [8]byte
	add := func(v uint64) { binary.LittleEndian.PutUint64(value[:], v); h.Write(value[:]) }
	add(uint64(len(snapshot.Archives)))
	var count uint64
	for _, archive := range snapshot.Archives {
		add(uint64(archive.Retention.Step))
		add(uint64(archive.Retention.Count))
		add(uint64(len(archive.Points)))
		for _, point := range archive.Points {
			add(uint64(point.Timestamp))
			add(math.Float64bits(point.Value))
			count++
		}
	}
	return sortGoldenRow{snapshot.Metadata.Name, count, hex.EncodeToString(h.Sum(nil))}
}

func TestProductionSnapshotSortGolden(t *testing.T) {
	root, rows, _ := sortCorpus(t)
	golden := os.Getenv("GRAPHITE_SORT_GOLDEN")
	if golden == "" {
		t.Fatal("explicit original-implementation golden path is required")
	}
	ctx := context.Background()
	db, err := chunkstore.Open(t.TempDir(), chunkstore.Options{ExactBlocks: true, CacheSize: 32 << 20, MemTableSize: 8 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	}()
	var got []sortGoldenRow
	for _, row := range rows {
		snapshot, err := whisperio.ReadSnapshot(ctx, row.Metric, filepath.Join(root, row.Path))
		if err != nil {
			t.Fatalf("read %s: %v", row.Metric, err)
		}
		before := sortSnapshotFingerprint(t, snapshot)
		if _, err := db.CreateFromSnapshot(ctx, snapshot); err != nil {
			t.Fatal(err)
		}
		after, err := db.Snapshot(ctx, row.Metric)
		if err != nil {
			t.Fatal(err)
		}
		if before != sortSnapshotFingerprint(t, after) {
			t.Fatalf("exact-store import changed archive bits: %s", row.Metric)
		}
		got = append(got, before)
	}
	if os.Getenv("GRAPHITE_SORT_WRITE_GOLDEN") == "1" {
		data, err := json.MarshalIndent(got, "", "  ")
		if err != nil {
			t.Fatal(err)
		}
		f, err := os.OpenFile(golden, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := f.Write(append(data, '\n')); err != nil {
			t.Fatal(err)
		}
		if err := f.Close(); err != nil {
			t.Fatal(err)
		}
		return
	}
	data, err := os.ReadFile(golden)
	if err != nil {
		t.Fatal(err)
	}
	var want []sortGoldenRow
	if err := json.Unmarshal(data, &want); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatal("snapshot differs from original implementation on retained production corpus")
	}
}

func BenchmarkProductionSnapshotSort(b *testing.B) {
	root, rows, bytes := sortCorpus(b)
	ctx := context.Background()
	for _, stored := range []bool{false, true} {
		name := "source"
		if stored {
			name = "exact-store"
		}
		b.Run(name, func(b *testing.B) {
			var db *chunkstore.Store
			var points uint64
			if stored {
				var err error
				db, err = chunkstore.Open(b.TempDir(), chunkstore.Options{ExactBlocks: true, CacheSize: 64 << 20, MemTableSize: 8 << 20})
				if err != nil {
					b.Fatal(err)
				}
				defer db.Close()
			}
			for _, row := range rows {
				snapshot, err := whisperio.ReadSnapshot(ctx, row.Metric, filepath.Join(root, row.Path))
				if err != nil {
					b.Fatal(err)
				}
				for _, archive := range snapshot.Archives {
					points += uint64(len(archive.Points))
				}
				if stored {
					if _, err := db.CreateFromSnapshot(ctx, snapshot); err != nil {
						b.Fatal(err)
					}
				}
			}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				for _, row := range rows {
					var snapshot chunkstore.Snapshot
					var err error
					if stored {
						snapshot, err = db.Snapshot(ctx, row.Metric)
					} else {
						snapshot, err = whisperio.ReadSnapshot(ctx, row.Metric, filepath.Join(root, row.Path))
					}
					if err != nil || snapshot.Metadata.Name != row.Metric {
						b.Fatalf("snapshot %s: %v", row.Metric, err)
					}
				}
			}
			b.ReportMetric(float64(points), "points/op")
			b.ReportMetric(float64(bytes), "fixture-B/op")
		})
	}
}
