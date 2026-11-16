package v2daemon

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/go-graphite/go-carbon/internal/partlog"
	"github.com/go-graphite/go-carbon/internal/partsformat"
	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2core"
	"github.com/go-graphite/go-carbon/points"
)

// Fixture reproduces the accepted input in v2backend/fixed_corpus_test.go.
// The formula, per-name order, stamps, values and four-update barriers are copied,
// not imported from the adapter. Geometry is identical in every layer.
type Fixture struct {
	CPUProfile                                               string
	BlockProfile                                             string
	Names, Rounds, Producers, Shards, BatchSize, SampleNames int
}

func DefaultFixture() Fixture {
	return Fixture{Names: 1 << 20, Rounds: 256, Producers: 48, Shards: 12, BatchSize: 32768, SampleNames: 3}
}
func fixtureName(id int) string {
	return fmt.Sprintf("bench.namespace%d.cluster1.host%07d.component.requests.value", id%4, id)
}
func fixturePoint(id, round int) points.Point {
	return points.Point{Timestamp: 1794700799 + int64(round), Value: float64(id%100003) + .125}
}
func procWritten() (uint64, error) {
	b, err := os.ReadFile("/proc/self/io")
	if err != nil {
		return 0, err
	}
	for _, line := range strings.Split(string(b), "\n") {
		if strings.HasPrefix(line, "write_bytes:") {
			return strconv.ParseUint(strings.TrimSpace(strings.TrimPrefix(line, "write_bytes:")), 10, 64)
		}
	}
	return 0, errors.New("missing process write counter")
}
func processCPU() (float64, error) {
	var u syscall.Rusage
	err := syscall.Getrusage(syscall.RUSAGE_SELF, &u)
	return float64(u.Utime.Sec+u.Stime.Sec) + float64(u.Utime.Usec+u.Stime.Usec)/1e6, err
}
func sizes(root string) (all, catalog, journal int64, err error) {
	err = filepath.Walk(root, func(path string, info os.FileInfo, e error) error {
		if e != nil {
			return e
		}
		if !info.IsDir() {
			all += info.Size()
			if strings.Contains(path, string(filepath.Separator)+"catalog"+string(filepath.Separator)) {
				catalog += info.Size()
			} else if strings.HasSuffix(path, "segment.pdc4") {
				journal += info.Size()
			}
		}
		return nil
	})
	return
}
func parallel(producers int, fn func(int) error) error {
	errs := make([]error, producers)
	var wg sync.WaitGroup
	for p := 0; p < producers; p++ {
		wg.Add(1)
		go func(p int) { defer wg.Done(); errs[p] = fn(p) }(p)
	}
	wg.Wait()
	return errors.Join(errs...)
}

// RunFixed writes one finite corpus, closes all resources, then verifies reopened
// snapshots. Read-back is outside timed intake/close and process CPU accounting.
func RunFixed(root string, layer int, f Fixture, out io.Writer) (result map[string]any, err error) {
	if f.Names < 1 || f.Rounds < 4 || f.Rounds%4 != 0 || f.Producers < 1 || f.Shards < 1 || f.BatchSize < 1 || f.BatchSize > 32768 || f.SampleNames < 1 {
		return nil, errors.New("invalid fixture")
	}
	// Name allocation and identical reusable generator buffers are outside timing.
	names := make([]string, f.Names)
	policies := make([][]byte, f.Names)
	for i := range names {
		names[i] = fixtureName(i)
		if layer == 3 {
			p := v2core.RollupPolicy{Name: partsformat.ID(names[i]), Group: "bench", Config: fixturePolicy(i)}
			policies[i], err = v2core.EncodeRollupPolicy(p)
			if err != nil {
				return nil, err
			}
		}
	}
	type input struct {
		names []string
		data  []points.Point
		seq   []uint64
		rows  []v2core.Row
		apps  []Application
		order uint64
	}
	inputs := make([]input, f.Producers)
	for p := range inputs {
		inputs[p] = input{names: make([]string, 0, f.BatchSize), data: make([]points.Point, 0, f.BatchSize), seq: make([]uint64, 0, f.BatchSize), rows: make([]v2core.Row, 0, f.BatchSize), apps: make([]Application, 0, f.BatchSize)}
	}
	cfg := Config{Layer: layer, Producers: f.Producers, Shards: f.Shards}
	var block *os.File
	stopBlock := func() error {
		if block == nil {
			return nil
		}
		runtime.SetBlockProfileRate(0)
		x := pprof.Lookup("block").WriteTo(block, 0)
		x = errors.Join(x, block.Close())
		block = nil
		return x
	}
	if f.BlockProfile != "" {
		block, err = os.Create(f.BlockProfile)
		if err != nil {
			return nil, err
		}
		runtime.SetBlockProfileRate(1000000)
		defer func() { err = errors.Join(err, stopBlock()) }()
	}
	var profile *os.File
	if f.CPUProfile != "" {
		var x error
		profile, x = os.Create(f.CPUProfile)
		if x != nil {
			return nil, x
		}
		if x = pprof.StartCPUProfile(profile); x != nil {
			_ = profile.Close()
			return nil, x
		}
		defer func() {
			if profile != nil {
				pprof.StopCPUProfile()
				err = errors.Join(err, profile.Close())
			}
		}()
	}
	startCPU, e := processCPU()
	if e != nil {
		return nil, e
	}
	startWrite, e := procWritten()
	if e != nil {
		return nil, e
	}
	start := time.Now()
	s, e := Open(root, cfg)
	if e != nil {
		return nil, e
	}
	defer func() {
		if s != nil {
			err = errors.Join(err, s.Close())
		}
	}()
	for seal := 0; seal < f.Rounds/4; seal++ {
		e = parallel(f.Producers, func(p int) error {
			in := &inputs[p]
			producer := s.Producers[p]
			flush := func() error {
				if len(in.rows) == 0 {
					return nil
				}
				var x error
				if layer == 0 {
					x = producer.AppendNumeric(in.rows)
				} else if layer == 3 {
					in.apps = in.apps[:0]
					for i, row := range in.rows {
						in.order++
						in.apps = append(in.apps, Application{Name: in.names[i], Policy: policies[int(row.ID-1)], Order: in.order, Clock: 1794700800 + int64((seal+1)*4), Points: in.data[i : i+1 : i+1], Sequence: in.seq[i : i+1 : i+1]})
					}
					x = producer.AppendApplications(in.apps)
					clear(in.apps)
				} else {
					x = producer.Append(in.names, in.data, in.seq, 1794700800+int64((seal+1)*4))
				}
				clear(in.names)
				in.names = in.names[:0]
				in.data = in.data[:0]
				in.seq = in.seq[:0]
				in.rows = in.rows[:0]
				return x
			}
			for round := seal * 4; round < (seal+1)*4; round++ {
				for id := p; id < f.Names; id += f.Producers {
					point := fixturePoint(id, round)
					in.names = append(in.names, names[id])
					in.data = append(in.data, point)
					in.seq = append(in.seq, uint64(round+1))
					in.rows = append(in.rows, v2core.Row{ID: uint64(id + 1), Time: point.Timestamp, Bits: math.Float64bits(point.Value), Seq: uint64(round + 1)})
					if len(in.rows) == f.BatchSize {
						if x := flush(); x != nil {
							return x
						}
					}
				}
			}
			if x := flush(); x != nil {
				return x
			}
			return producer.Seal()
		})
		if e != nil {
			return nil, e
		}
		if seal%8 == 7 {
			if e = json.NewEncoder(out).Encode(map[string]any{"progress_seals": seal + 1, "points": (seal + 1) * 4 * f.Names, "seconds": time.Since(start).Seconds()}); e != nil {
				return nil, e
			}
		}
	}
	intake := time.Since(start).Seconds()
	drainStart := time.Now()
	if e = parallel(f.Producers, func(p int) error { return s.Producers[p].Drain() }); e != nil {
		return nil, e
	}
	drainSeconds := time.Since(drainStart).Seconds()
	var ingest, compact, flushed, debt, groups, forced, encoded uint64
	var active int64
	var encodeTime, catalogSyncTime, journalSyncTime, coreSubmitTime time.Duration
	for _, db := range s.Core.DBs() {
		m := db.Metrics()
		for _, l := range m.Levels {
			ingest += l.BytesIngested
			compact += l.BytesCompacted
			flushed += l.BytesFlushed
		}
		debt += m.Compact.EstimatedDebt
		active += m.Compact.NumInProgress
	}
	for _, p := range s.Producers {
		groups += p.SyncGroups
		forced += p.ForcedGroups
		encoded += p.JournalBytes
		encodeTime += p.EncodeTime
		catalogSyncTime += p.CatalogSyncTime
		journalSyncTime += p.JournalSyncTime
		coreSubmitTime += p.CoreSubmitTime
	}
	closeStart := time.Now()
	if e = s.Close(); e != nil {
		return nil, e
	}
	closeSeconds := time.Since(closeStart).Seconds()
	s = nil
	complete := time.Since(start).Seconds()
	endCPU, e := processCPU()
	if e != nil {
		return nil, e
	}
	endWrite, e := procWritten()
	if e != nil {
		return nil, e
	}
	if profile != nil {
		pprof.StopCPUProfile()
		if e = profile.Close(); e != nil {
			return nil, e
		}
		profile = nil
	}
	if e = stopBlock(); e != nil {
		return nil, e
	}
	rest, catalog, journal, e := sizes(root)
	if e != nil {
		return nil, e
	}
	count := float64(f.Names) * float64(f.Rounds)
	result = map[string]any{"layer": layer, "names": f.Names, "rounds": f.Rounds, "producers": f.Producers, "shards": f.Shards, "barriers": f.Rounds / 4, "points": uint64(count), "intake_seconds": intake, "complete_seconds": complete, "intake_points_per_second": count / intake, "complete_points_per_second": count / complete, "cpu_ns_per_point": (endCPU - startCPU) * 1e9 / count, "written_B_per_point": float64(endWrite-startWrite) / count, "ingested_SST_before_close_B_per_point": float64(ingest) / count, "compaction_before_close_B_per_point": float64(compact) / count, "flush_before_close_B_per_point": float64(flushed) / count, "catalog_B_per_point": float64(catalog) / count, "journal_B_per_point": float64(journal) / count, "at_rest_B_per_point": float64(rest) / count, "estimated_debt_before_close": debt, "active_compactions_before_close": active, "journal_sync_groups": groups, "forced_groups": forced, "encoded_journal_bytes": encoded, "sync_groups_per_stream_second": float64(groups) / float64(f.Producers) / complete}
	result["final_drain_seconds"] = drainSeconds
	result["close_seconds"] = closeSeconds
	result["stream_encode_seconds"] = encodeTime.Seconds()
	result["stream_catalog_sync_seconds"] = catalogSyncTime.Seconds()
	result["stream_journal_sync_seconds"] = journalSyncTime.Seconds()
	result["stream_core_submit_seconds"] = coreSubmitTime.Seconds()
	// Reopen the same core API; no second reader or key translation is introduced.
	reader, e := v2core.Open(filepath.Join(root, "placement-group-00000001", "core"), coreConfig(cfg), v2core.Hooks{})
	if e != nil {
		return result, e
	}
	defer func() { err = errors.Join(err, reader.Close()) }()
	// Check every stored tuple with the core-owned decoder. Each metric routes
	// to one owner, so parallel owners write disjoint words of this bitmap.
	// The bitmap detects duplicates as well as missing tuples; a count alone
	// could hide one missing row behind one duplicate.
	words := (f.Rounds + 63) / 64
	seen := make([]uint64, f.Names*words)
	counts := make([]uint64, f.Shards)
	dbs := reader.DBs()
	if e = parallel(f.Shards, func(shard int) error {
		snapshot := dbs[shard].NewSnapshot()
		defer snapshot.Close()
		return v2core.ScanRaw(snapshot, func(r v2core.Row) error {
			if r.ID == 0 || r.ID > uint64(f.Names) || r.Seq == 0 || r.Seq > uint64(f.Rounds) || v2core.Shard(r.ID, f.Shards) != shard {
				return fmt.Errorf("invalid stored fixture row: %#v", r)
			}
			id, round := int(r.ID-1), int(r.Seq-1)
			point := fixturePoint(id, round)
			if r.Time != point.Timestamp || r.Bits != math.Float64bits(point.Value) {
				return fmt.Errorf("stored fixture tuple changed: %#v", r)
			}
			word, bit := id*words+round/64, uint64(1)<<uint(round%64)
			if seen[word]&bit != 0 {
				return fmt.Errorf("duplicate stored fixture tuple: %#v", r)
			}
			seen[word] |= bit
			counts[shard]++
			return nil
		})
	}); e != nil {
		return result, e
	}
	var checked uint64
	for _, n := range counts {
		checked += n
	}
	if checked != uint64(f.Names)*uint64(f.Rounds) {
		return result, fmt.Errorf("stored fixture count %d, want %d", checked, f.Names*f.Rounds)
	}
	result["exact_core_points"] = checked
	if layer > 0 {
		if e = parallel(f.Producers, func(p int) (err error) {
			catalog, err := partlog.OpenNameCatalog(filepath.Join(root, "placement-group-00000001", "journal", fmt.Sprintf("producer-%03d", p), "catalog"))
			if err != nil {
				return err
			}
			defer func() { err = errors.Join(err, catalog.Close()) }()
			for id := p; id < f.Names; id += f.Producers {
				local := uint64(id/f.Producers + 1)
				name, _, ok := catalog.Lookup(local)
				if !ok || name != names[id] || NumericID(local, p, f.Producers) != uint64(id+1) {
					return fmt.Errorf("persisted fixture name/ID mismatch %s", names[id])
				}
			}
			return nil
		}); e != nil {
			return result, e
		}
		result["exact_catalog_names"] = f.Names
	}
	for i := 0; i < f.SampleNames; i++ {
		id := i * (f.Names - 1) / max(1, f.SampleNames-1)
		if layer > 0 {
			p := id % f.Producers
			local := uint64(id/f.Producers + 1)
			catalog, x := partlog.OpenNameCatalog(filepath.Join(root, "placement-group-00000001", "journal", fmt.Sprintf("producer-%03d", p), "catalog"))
			if x != nil {
				return result, x
			}
			name, _, ok := catalog.Lookup(local)
			x = catalog.Close()
			if x != nil {
				return result, x
			}
			if !ok || name != names[id] || NumericID(local, p, f.Producers) != uint64(id+1) {
				return result, fmt.Errorf("persisted sample name/ID mismatch %s", names[id])
			}
		}
		snap := reader.DBs()[v2core.Shard(uint64(id+1), f.Shards)].NewSnapshot()
		rows, x := v2core.Read(snap, uint64(id+1))
		x = errors.Join(x, snap.Close())
		if x != nil {
			return result, x
		}
		if len(rows) != f.Rounds {
			return result, fmt.Errorf("name %s: %d points, want %d", names[id], len(rows), f.Rounds)
		}
		for round, row := range rows {
			point := fixturePoint(id, round)
			want := v2core.Row{ID: uint64(id + 1), Time: point.Timestamp, Bits: math.Float64bits(point.Value), Seq: uint64(round + 1)}
			if row != want {
				return result, fmt.Errorf("name %s round %d: got %#v want %#v", names[id], round, row, want)
			}
		}
	}
	if layer >= 2 {
		if e = verifyJournal(root, f, names, policies); e != nil {
			return result, e
		}
		result["exact_journal_points"] = f.Names * f.Rounds
	}
	if layer == 3 {
		checkedNames, checkedSlots, x := verifyFixedRollups(reader, f, names)
		if x != nil {
			return result, x
		}
		result["exact_archive_names"] = checkedNames
		result["exact_archive_slots"] = checkedSlots
	}
	result["exact_sample_names"] = f.SampleNames
	result["exact_sample_points"] = f.SampleNames * f.Rounds
	result["readback"] = "exact after close/reopen; outside measured interval"
	err = json.NewEncoder(out).Encode(result)
	return
}

// fixturePolicy assigns the published single/2/3/4 archive shapes by name,
// leaving every original input tuple, clock and singleton group unchanged.
func fixturePolicy(id int) sealrollup.Config {
	shapes := [][]sealrollup.Archive{
		{{Step: 1, Count: 86400}},
		{{Step: 1, Count: 86400}, {Step: 60, Count: 43200}, {Step: 3600, Count: 8760}, {Step: 86400, Count: 3650}},
		{{Step: 60, Count: 43200}, {Step: 3600, Count: 8760}, {Step: 86400, Count: 3650}},
		{{Step: 60, Count: 20160}, {Step: 1800, Count: 35040}},
	}
	return sealrollup.Config{Method: sealrollup.Average, XFilesFactor: .5, Archives: shapes[id%len(shapes)]}
}
