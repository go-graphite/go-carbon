package v2backend

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"

	"github.com/go-graphite/go-carbon/internal/v2core"
	"github.com/go-graphite/go-carbon/points"
)

// Fixed accepted input, including apply order within a metric and arrival clock.
// Core control intentionally excludes journal, catalogue, rollups and TCP.
func TestFixedAcceptedCorpus(t *testing.T) {
	if os.Getenv("V2_FIXED_CORPUS") != "1" {
		t.Skip("explicit fixed corpus only")
	}
	const names = 1 << 20
	const rounds = 256
	const producers = 48
	const dbs = 12
	const perSeal = 4
	runtime.GOMAXPROCS(96)
	root := t.TempDir()
	clock := int64(1794700800)
	coreOnly := os.Getenv("V2_FIXED_CORE") == "1"
	var s *Store
	var e *v2core.Engine
	var err error
	if coreOnly {
		e, err = v2core.Open(root, v2core.Config{JournalIdentity: "fixed-accepted-v1", Shards: dbs, Producers: producers, Owners: dbs, MaxRows: 2 << 20, PartitionSeconds: 86400, AppendParts: true, RawCodec: "run1"}, v2core.Hooks{})
	} else {
		opts := testOptions(root, simplePolicy())
		opts.Owners, opts.Shards, opts.WriteEngine = producers, dbs, "core"
		s, err = openAt(opts, func() time.Time { return time.Unix(clock, 0) })
	}
	if err != nil {
		t.Fatal(err)
	}
	closed := false
	defer func() {
		if !closed {
			if s != nil {
				_ = s.Close()
			} else {
				_ = e.Close()
			}
		}
	}()
	type named struct {
		name []byte
		id   int
	}
	namesByProducer := make([][]named, producers)
	if !coreOnly {
		for id := 0; id < names; id++ {
			name := fmt.Sprintf("bench.namespace%d.cluster1.host%07d.component.requests.value", id%4, id)
			p := s.route(name)
			namesByProducer[p] = append(namesByProducer[p], named{[]byte(name), id})
		}
	}
	var u0, u1 syscall.Rusage
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &u0)
	w0 := numericProcWriteBytes(t)
	start := time.Now()
	var cursors [producers]uint64
	for seal := 0; seal < rounds/perSeal; seal++ {
		clock = 1794700800 + int64((seal+1)*perSeal)
		var wg sync.WaitGroup
		for p := 0; p < producers; p++ {
			wg.Add(1)
			go func(p int) {
				defer wg.Done()
				if coreOnly {
					rows := make([]v2core.Row, 0, 32768)
					for r := seal * perSeal; r < (seal+1)*perSeal; r++ {
						for id := p; id < names; id += producers {
							rows = append(rows, v2core.Row{ID: uint64(id + 1), Time: 1794700799 + int64(r), Bits: math.Float64bits(float64(id%100003) + .125), Seq: uint64(r + 1)})
							if len(rows) == cap(rows) {
								cursors[p] += uint64(len(rows))
								if x := e.Append(p, rows, cursors[p], false); x != nil {
									t.Error(x)
									return
								}
								rows = rows[:0]
							}
						}
					}
					if len(rows) > 0 {
						cursors[p] += uint64(len(rows))
						if x := e.Append(p, rows, cursors[p], false); x != nil {
							t.Error(x)
						}
					}
				} else {
					batch := make([][]byte, 0, 1024)
					data := make([]points.Point, 1024)
					for r := seal * perSeal; r < (seal+1)*perSeal; r++ {
						for _, entry := range namesByProducer[p] {
							name, id := entry.name, entry.id
							i := len(batch)
							data[i] = points.Point{Timestamp: 1794700799 + int64(r), Value: float64(id%100003) + .125}
							batch = append(batch, name)
							if len(batch) == 1024 {
								if x := s.AppendBatch(batch, data[:len(batch)]); x != nil {
									t.Error(x)
									return
								}
								batch = batch[:0]
							}
						}
					}
					if len(batch) > 0 {
						if x := s.AppendBatch(batch, data[:len(batch)]); x != nil {
							t.Error(x)
						}
					}
				}
			}(p)
		}
		wg.Wait()
		if t.Failed() {
			return
		}
		if coreOnly {
			for p := 0; p < producers; p++ {
				if err = e.Append(p, nil, cursors[p], true); err != nil {
					t.Fatal(err)
				}
			}
		} else {
			if err = s.Flush(); err != nil {
				t.Fatal(err)
			}
		}
		if seal%8 == 7 {
			fmt.Printf("FIXED_PROGRESS seals=%d points=%d seconds=%.3f\n", seal+1, (seal+1)*perSeal*names, time.Since(start).Seconds())
		}
	}
	intake := time.Since(start).Seconds()
	if coreOnly {
		dumpFixedLevels(e.DBs(), "intake")
	} else {
		dumpFixedLevels(s.dbs, "intake")
	}
	settle := time.Now()
	quiet := 0
	for time.Since(settle) < 90*time.Second {
		active := 0
		var debt uint64
		if coreOnly {
			for _, db := range e.DBs() {
				m := db.Metrics()
				active += int(m.Compact.NumInProgress)
				debt += m.Compact.EstimatedDebt
			}
		} else {
			for _, db := range s.dbs {
				m := db.Metrics()
				active += int(m.Compact.NumInProgress)
				debt += m.Compact.EstimatedDebt
			}
		}
		fmt.Printf("FIXED_SETTLE active=%d debt=%d seconds=%.3f\n", active, debt, time.Since(settle).Seconds())
		if active == 0 && debt < 1<<20 {
			quiet++
		} else {
			quiet = 0
		}
		if quiet >= 5 {
			break
		}
		time.Sleep(time.Second)
	}
	if coreOnly {
		dumpFixedLevels(e.DBs(), "after90s")
	} else {
		dumpFixedLevels(s.dbs, "after90s")
		probeFixedReads(t, s, clock)
	}
	var rest, journal int64
	_ = filepath.Walk(root, func(p string, info os.FileInfo, x error) error {
		if x == nil && !info.IsDir() {
			rest += info.Size()
			if strings.Contains(p, "/journal/") || strings.Contains(p, "/global-catalog/") || strings.Contains(p, "/name-catalog/") {
				journal += info.Size()
			}
		}
		return x
	})
	if s != nil {
		if s.Stats().IngestedPoints != names*rounds {
			t.Fatal("count", s.Stats())
		}
		err = s.Close()
	} else {
		err = e.Close()
	}
	if err != nil {
		t.Fatal(err)
	}
	closed = true
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &u1)
	sec := func(x syscall.Timeval) float64 { return float64(x.Sec) + float64(x.Usec)/1e6 }
	cpu := sec(u1.Utime) + sec(u1.Stime) - sec(u0.Utime) - sec(u0.Stime)
	elapsed := time.Since(start).Seconds()
	count := float64(names * rounds)
	_ = json.NewEncoder(os.Stdout).Encode(map[string]any{"fixed_corpus": "v2", "core_only": coreOnly, "points": names * rounds, "names": names, "rounds": rounds, "seals": rounds / perSeal, "intake_seconds": intake, "intake_points_per_second": float64(names*rounds) / intake, "complete_seconds": elapsed, "points_per_second": count / elapsed, "cpu_ns_per_point": cpu * 1e9 / count, "written_bytes": numericProcWriteBytes(t) - w0, "written_B_per_point": float64(numericProcWriteBytes(t)-w0) / count, "at_rest_B_per_point": float64(rest) / count, "journal_B_per_point": float64(journal) / count, "settled": quiet >= 5})
}

func dumpFixedLevels(dbs []*pebble.DB, phase string) {
	for id, db := range dbs {
		m := db.Metrics()
		_ = json.NewEncoder(os.Stdout).Encode(map[string]any{"level_diagnostic": true, "phase": phase, "db": id, "levels": m.Levels})
		tables, err := db.SSTables()
		if err != nil {
			panic(err)
		}
		for level, ts := range tables {
			for _, table := range ts {
				_ = json.NewEncoder(os.Stdout).Encode(map[string]any{"table_diagnostic": true, "phase": phase, "db": id, "level": level, "size": table.Size, "smallest": fmt.Sprintf("%x", table.Smallest.UserKey), "largest": fmt.Sprintf("%x", table.Largest.UserKey)})
			}
		}
	}
}
func probeFixedReads(t *testing.T, s *Store, clock int64) {
	t.Helper()
	var latencies []int64
	hash := sha256.New()
	started := time.Now()
	for round := 0; round < 5; round++ {
		for i := 0; i < 64; i++ {
			id := (i * 7919) % (1 << 20)
			name := fmt.Sprintf("bench.namespace%d.cluster1.host%07d.component.requests.value", id%4, id)
			begin := time.Now()
			r, err := s.Fetch(context.Background(), name, int(clock-300), int(clock))
			if err != nil {
				t.Fatal(err)
			}
			if round > 0 {
				latencies = append(latencies, time.Since(begin).Nanoseconds())
			}
			var b [8]byte
			for _, v := range []int{r.From, r.Until, r.Step, len(r.Values)} {
				binary.LittleEndian.PutUint64(b[:], uint64(v))
				_, _ = hash.Write(b[:])
			}
			for _, v := range r.Values {
				binary.LittleEndian.PutUint64(b[:], math.Float64bits(v))
				_, _ = hash.Write(b[:])
			}
			r.Release()
		}
	}
	slices.Sort(latencies)
	_ = json.NewEncoder(os.Stdout).Encode(map[string]any{"read_probe": true, "queries": len(latencies), "p50_us": float64(latencies[len(latencies)/2]) / 1e3, "p99_us": float64(latencies[len(latencies)*99/100]) / 1e3, "render_sha256": fmt.Sprintf("%x", hash.Sum(nil)), "seconds": time.Since(started).Seconds()})
}
