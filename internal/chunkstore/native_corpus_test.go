package chunkstore

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
)

type productionCorpusPoint struct {
	Timestamp int64
	Bits      uint64
}

type productionCorpusRow struct {
	Metric, Path         string
	Points               []productionCorpusPoint
	AllocatedBytes       int64
	InclusionProbability float64
	BaselineID           uint64
}

// Select a byte-weighted corpus from all canonical files in the captured
// ingress pool. Raw names and values remain in private lab artifacts.
func TestNativeProductionByteWeightedCorpus(t *testing.T) {
	dir, trace, source, output := os.Getenv("GRAPHITE_CORPUS_DB"), os.Getenv("GRAPHITE_CORPUS_TRACE"), os.Getenv("GRAPHITE_CORPUS_SOURCE"), os.Getenv("GRAPHITE_CORPUS_OUTPUT")
	if dir == "" || trace == "" || source == "" || output == "" {
		t.Skip("requires private full database, retained baseline and ingress")
	}
	if !strings.Contains(filepath.Clean(dir), "/production-replay-lab-v1/") || !strings.HasSuffix(filepath.Clean(source), "/full-baseline/cwhisper-copy") {
		t.Fatal("corpus must use private DB and retained baseline")
	}
	limit, err := strconv.ParseUint(os.Getenv("GRAPHITE_CORPUS_ORIGINAL_LIMIT"), 10, 64)
	if err != nil || limit == 0 {
		t.Fatal("requires verified original ID ceiling")
	}
	metrics, _, pointCount, err := loadProductionEngineTrace(trace)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.Mkdir(output, 0700); err != nil {
		t.Fatal(err)
	}
	weights := make([]int64, len(metrics))
	var absent atomic.Uint64
	jobs := make(chan int)
	errorsSeen := make(chan error, 16)
	var workers sync.WaitGroup
	for range 16 {
		workers.Go(func() {
			for i := range jobs {
				relative := strings.ReplaceAll(metrics[i].name, ".", "/") + ".wsp"
				if !filepath.IsLocal(relative) {
					absent.Add(1)
					continue
				}
				for _, suffix := range []string{"", ".ooo"} {
					st, e := os.Stat(filepath.Join(source, relative) + suffix)
					if e != nil {
						if os.IsNotExist(e) || errors.Is(e, syscall.ENAMETOOLONG) || errors.Is(e, syscall.ENOTDIR) {
							if suffix == "" {
								absent.Add(1)
							}
							break
						}
						select {
						case errorsSeen <- e:
						default:
						}
						break
					}
					if !st.Mode().IsRegular() {
						absent.Add(1)
						break
					}
					weights[i] += st.Sys().(*syscall.Stat_t).Blocks * 512
				}
			}
		})
	}
	for i := range metrics {
		jobs <- i
	}
	close(jobs)
	workers.Wait()
	close(errorsSeen)
	for e := range errorsSeen {
		t.Error(e)
	}
	if t.Failed() {
		t.FailNow()
	}
	var poolBytes int64
	var poolMetrics int
	for _, w := range weights {
		if w > 0 {
			poolBytes += w
			poolMetrics++
		}
	}
	if poolBytes <= 0 {
		t.Fatal("empty retained source pool")
	}
	db, err := Open(dir, Options{ExactBlocks: true, CacheSize: 512 << 20, MemTableSize: 64 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer closeTestStore(t, db)
	var rows []productionCorpusRow
	var changed, missing int
	var selectedBytes int64
	nameHash := sha256.New()
	policies := map[int]int{}
	for i, rec := range metrics {
		if weights[i] <= 0 {
			continue
		}
		probability := math.Min(1, 4096*float64(weights[i])/float64(poolBytes))
		h := sha256.Sum256([]byte("graphite-byte-corpus-20261020/" + rec.name))
		u := (float64(binary.BigEndian.Uint64(h[:8])>>11) + 0.5) / (1 << 53)
		if u >= probability {
			continue
		}
		m, e := db.Metadata(context.Background(), rec.name)
		if errors.Is(e, ErrNotFound) {
			missing++
			continue
		}
		if e != nil {
			t.Fatal(e)
		}
		if m.ID > limit || m.Generation != 1 || m.Revision != 1 {
			changed++
			continue
		}
		row := productionCorpusRow{Metric: rec.name, Path: strings.ReplaceAll(rec.name, ".", "/") + ".wsp", AllocatedBytes: weights[i], InclusionProbability: probability, BaselineID: m.ID}
		for _, point := range rec.points {
			row.Points = append(row.Points, productionCorpusPoint{Timestamp: point.Timestamp, Bits: math.Float64bits(point.Value)})
		}
		rows = append(rows, row)
		selectedBytes += weights[i]
		policies[m.Retentions[0].Step]++
		_, _ = nameHash.Write([]byte(rec.name))
		_, _ = nameHash.Write([]byte{0})
	}
	if len(rows) < 100 {
		t.Fatal("insufficient canonical byte-weighted source histories")
	}
	writeProductionReplayJSON(t, filepath.Join(output, "corpus.json"), map[string]any{"Rows": rows, "OriginalIDLimit": limit})
	writeProductionReplayJSON(t, filepath.Join(output, "RESULT.json"), map[string]any{"passed": true, "trace_metrics": len(metrics), "trace_points": pointCount, "source_pool_metrics": poolMetrics, "source_pool_allocated_bytes": poolBytes, "source_missing_or_invalid": absent.Load(), "selected_metrics": len(rows), "selected_allocated_bytes": selectedBytes, "selected_missing_catalog": missing, "selected_changed_since_baseline": changed, "primary_steps": policies, "name_fingerprint_sha256": hex.EncodeToString(nameHash.Sum(nil)), "sampling": "Independent deterministic Poisson inclusion, p=min(1,4096*allocated_source_bytes/total_pool_bytes), SHA256 uniform variate. Main plus OOO physical bytes. Canonical original-ID generation1/revision1 histories only.", "scope": "Byte-weighted corpus within the actual short ingress pool; not a fleet sample. Raw corpus remains private. Full-node census is separate; paired mirror replay still required."})
}
