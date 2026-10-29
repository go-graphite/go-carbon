package chunkstore

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"runtime/pprof"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/helper"
)

// Opt-in diagnosis of real metadata lookups on a private full-cardinality DB.
// It performs no data writes and emits no production qualification verdict.
func TestNativeProductionMetadataWorkingSet(t *testing.T) {
	dir, trace, output := os.Getenv("GRAPHITE_METADATA_DB"), os.Getenv("GRAPHITE_METADATA_TRACE"), os.Getenv("GRAPHITE_METADATA_OUTPUT")
	if dir == "" || trace == "" || output == "" {
		t.Skip("requires private full database and real ingress trace")
	}
	if !strings.Contains(filepath.Clean(dir), "/production-replay-lab-v1/") {
		t.Fatal("probe may only open its private lab copy")
	}
	cacheBytes := int64(128 << 20)
	if raw := os.Getenv("GRAPHITE_METADATA_CACHE_BYTES"); raw != "" {
		var err error
		cacheBytes, err = strconv.ParseInt(raw, 10, 64)
		if err != nil || cacheBytes <= 0 {
			t.Fatal("invalid cache budget")
		}
	}
	f, err := os.Open(trace)
	if err != nil {
		t.Fatal(err)
	}
	r := bufio.NewReaderSize(f, 1<<20)
	header := make([]byte, 36)
	if _, err = io.ReadFull(r, header); err != nil || string(header[:8]) != "GTREPL01" {
		t.Fatal("invalid replay trace header")
	}
	count := binary.LittleEndian.Uint32(header[8:12])
	if count == 0 || count > 10000000 {
		t.Fatal("invalid trace metric count")
	}
	names := make([]string, count)
	for i := range names {
		var n uint32
		if err = binary.Read(r, binary.LittleEndian, &n); err != nil || n == 0 || n > 1<<20 {
			t.Fatal("invalid trace metric")
		}
		b := make([]byte, n)
		if _, err = io.ReadFull(r, b); err != nil {
			t.Fatal(err)
		}
		names[i] = string(b)
	}
	if err = f.Close(); err != nil {
		t.Fatal(err)
	}
	// Choose a deterministic uniform hash sample before arranging it in the
	// actual noop queue's cache-shard groups. Selection is independent of size.
	sort.Slice(names, func(i, j int) bool {
		a, b := helper.HashString(names[i]), helper.HashString(names[j])
		if a != b {
			return a < b
		}
		return names[i] < names[j]
	})
	if len(names) > 250000 {
		clear(names[250000:])
		names = names[:250000]
	}
	sort.Slice(names, func(i, j int) bool {
		a, b := helper.HashString(names[i]), helper.HashString(names[j])
		if a&1023 != b&1023 {
			return a&1023 < b&1023
		}
		if a != b {
			return a < b
		}
		return names[i] < names[j]
	})
	oldProcs := runtime.GOMAXPROCS(192)
	defer runtime.GOMAXPROCS(oldProcs)
	db, err := Open(dir, Options{ExactBlocks: true, CacheSize: cacheBytes, MemTableSize: 64 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	shards := reflect.ValueOf(db.cache).Elem().FieldByName("shards").Len()
	if err = os.Mkdir(output, 0700); err != nil {
		t.Fatal(err)
	}
	prof, err := os.Create(filepath.Join(output, "cpu.pprof"))
	if err != nil {
		t.Fatal(err)
	}
	if err = pprof.StartCPUProfile(prof); err != nil {
		t.Fatal(err)
	}
	type fingerprint struct {
		ID, Generation, Revision, Policy uint64
		LastUpdate                       int64
	}
	fingerprints := make([]fingerprint, len(names))
	var found, missing atomic.Uint64
	errChan := make(chan error, 96)
	beforeStats := db.Stats()
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	var usageBefore, usageAfter syscall.Rusage
	if err = syscall.Getrusage(syscall.RUSAGE_SELF, &usageBefore); err != nil {
		t.Fatal(err)
	}
	began := time.Now()
	jobs := make(chan int)
	var workers sync.WaitGroup
	for range 96 {
		workers.Go(func() {
			for index := range jobs {
				for repeat := 0; repeat < 3; repeat++ {
					m, e := db.Metadata(context.Background(), names[index])
					if errors.Is(e, ErrNotFound) {
						if repeat == 0 {
							missing.Add(1)
						}
						continue
					}
					if e != nil {
						select {
						case errChan <- e:
						default:
						}
						continue
					}
					if m.Name != names[index] {
						select {
						case errChan <- errors.New("metadata name differs from requested key"):
						default:
						}
						continue
					}
					policy := uint64(m.AggregationMethod)*1099511628211 + uint64(math.Float32bits(m.XFilesFactor))
					for _, retention := range m.Retentions {
						policy = (policy ^ uint64(retention.Step)) * 1099511628211
						policy = (policy ^ uint64(retention.Count)) * 1099511628211
					}
					fp := fingerprint{m.ID, m.Generation, m.Revision, policy, m.LastUpdate.UnixNano()}
					if repeat == 0 {
						fingerprints[index] = fp
						found.Add(1)
					} else if fingerprints[index] != fp {
						select {
						case errChan <- errors.New("metadata changed during read-only probe"):
						default:
						}
					}
				}
			}
		})
	}
	for index := range names {
		jobs <- index
	}
	close(jobs)
	workers.Wait()
	elapsed := time.Since(began)
	if err = syscall.Getrusage(syscall.RUSAGE_SELF, &usageAfter); err != nil {
		t.Fatal(err)
	}
	runtime.ReadMemStats(&after)
	afterStats := db.Stats()
	pprof.StopCPUProfile()
	if err = prof.Close(); err != nil {
		t.Fatal(err)
	}
	close(errChan)
	for e := range errChan {
		if e != nil {
			t.Fatal(e)
		}
	}
	hash := sha256.New()
	for _, fp := range fingerprints {
		if err = binary.Write(hash, binary.LittleEndian, fp); err != nil {
			t.Fatal(err)
		}
	}
	cpu := func(r syscall.Rusage) float64 {
		return float64(r.Utime.Sec+r.Stime.Sec) + float64(r.Utime.Usec+r.Stime.Usec)/1e6
	}
	result := map[string]any{"passed": true, "metrics": len(names), "trace_metrics": count, "metadata_calls": 3 * len(names), "found": found.Load(), "missing": missing.Load(), "seconds": elapsed.Seconds(), "cpu_seconds": cpu(usageAfter) - cpu(usageBefore), "peak_rss_bytes_linux": usageAfter.Maxrss * 1024, "allocated_bytes": after.TotalAlloc - before.TotalAlloc, "allocations": after.Mallocs - before.Mallocs, "cache_bytes": cacheBytes, "cache_shards": shards, "gomaxprocs": runtime.GOMAXPROCS(0), "workers": 96, "cache_hits": afterStats.CacheHits - beforeStats.CacheHits, "cache_misses": afterStats.CacheMisses - beforeStats.CacheMisses, "database_disk_bytes": afterStats.DiskBytes, "metadata_fingerprint_sha256": hex.EncodeToString(hash.Sum(nil)), "scope": "Read-only metadata diagnostic on private full-cardinality database, actual ingress names, three lookups per name in noop shard groups. Uniform name sample; not byte-weighted fleet or steady read/write/compaction qualification."}
	b, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(output, "RESULT.json"), append(b, '\n'), 0600); err != nil {
		t.Fatal(err)
	}
}
