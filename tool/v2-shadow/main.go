// v2-shadow tails a classic input journal read-only into a fresh private L2 core.
package main

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"hash/fnv"
	"io"
	"math"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/go-graphite/go-carbon/internal/v2core"
	"github.com/go-graphite/go-carbon/internal/v2daemon"
	"github.com/go-graphite/go-carbon/points"
	"github.com/go-graphite/go-carbon/tags"
	pb "github.com/go-graphite/protocol/carbonapi_v2_pb"
	"google.golang.org/protobuf/proto"
)

var outputMu sync.Mutex

func emit(v any) { outputMu.Lock(); defer outputMu.Unlock(); _ = json.NewEncoder(os.Stdout).Encode(v) }

type tenantCPU struct {
	PID        int
	Command    string
	Ticks      uint64
	DeltaTicks uint64
}

var loadMu sync.Mutex
var previousCPU = map[int]uint64{}
var previousCPUTime time.Time

func contextLoad() map[string]any {
	loadMu.Lock()
	defer loadMu.Unlock()
	now := time.Now()
	load, _ := os.ReadFile("/proc/loadavg")
	stat, _ := os.ReadFile("/proc/self/status")
	ioBytes, _ := os.ReadFile("/proc/self/io")
	hostCPU, _ := os.ReadFile("/proc/stat")
	var tenants []tenantCPU
	paths, _ := filepath.Glob("/proc/[0-9]*/stat")
	for _, path := range paths {
		b, e := os.ReadFile(path)
		if e != nil {
			continue
		}
		text := string(b)
		a, z := strings.Index(text, "("), strings.LastIndex(text, ")")
		if a < 1 || z < a {
			continue
		}
		pid, e := strconv.Atoi(strings.TrimSpace(text[:a]))
		if e != nil || pid == os.Getpid() {
			continue
		}
		f := strings.Fields(text[z+1:])
		if len(f) < 13 {
			continue
		}
		u, _ := strconv.ParseUint(f[11], 10, 64)
		v, _ := strconv.ParseUint(f[12], 10, 64)
		ticks := u + v
		var delta uint64
		if old, ok := previousCPU[pid]; ok && ticks >= old {
			delta = ticks - old
		}
		previousCPU[pid] = ticks
		tenants = append(tenants, tenantCPU{pid, text[a+1 : z], ticks, delta})
	}
	sort.Slice(tenants, func(i, j int) bool {
		if tenants[i].DeltaTicks == tenants[j].DeltaTicks {
			return tenants[i].Ticks > tenants[j].Ticks
		}
		return tenants[i].DeltaTicks > tenants[j].DeltaTicks
	})
	if len(tenants) > 16 {
		tenants = tenants[:16]
	}
	elapsed := 0.0
	if !previousCPUTime.IsZero() {
		elapsed = now.Sub(previousCPUTime).Seconds()
	}
	previousCPUTime = now
	return map[string]any{"when_cet": now.In(time.FixedZone("CET", 3600)).Format(time.RFC3339), "loadavg": string(load), "process_status": string(stat), "process_io": string(ioBytes), "host_cpu_stat": strings.SplitN(string(hostCPU), "\n", 2)[0], "co_tenants": tenants, "co_tenant_interval_seconds": elapsed, "co_tenant_ticks_per_second": 100, "shared_host_provisional": true}
}

type candidate struct {
	ID   uint64
	Name string
}
type compareJob struct {
	Layer                int
	SourceLagSeconds     float64
	SourceSegmentsBehind int
	Names                []candidate
	Snapshots            []*pebble.Snapshot
	From, Until          int64
	SourcePath           string
	SourceOffset         int64
	SourceCaughtUp       bool
	Attempt              int
	FirstSnapshot        time.Time
	PostClose            bool
	SkipObservations     bool
}
type slot struct{ Bits, Seq uint64 }
type difference struct {
	Name                      string `json:"name"`
	Time                      int64  `json:"time"`
	V2Bits, ClassicBits       string
	V2Present, ClassicPresent bool
}

func fetch(client *http.Client, base, name string, from, until int64) (*pb.FetchResponse, error) {
	q := url.Values{"target": {name}, "from": {strconv.FormatInt(from, 10)}, "until": {strconv.FormatInt(until, 10)}, "format": {"protobuf"}}
	resp, err := client.Get(base + "/render/?" + q.Encode())
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(io.LimitReader(resp.Body, 16<<20))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("HTTP %d: %.200s", resp.StatusCode, b)
	}
	var reply pb.MultiFetchResponse
	if err = proto.Unmarshal(b, &reply); err != nil {
		return nil, err
	}
	if len(reply.Metrics) != 1 || reply.Metrics[0].Name != name {
		return nil, fmt.Errorf("expected one exact metric, got %d", len(reply.Metrics))
	}
	return reply.Metrics[0], nil
}

// compareRaw compares both occupied and missing slots. Coarse data is explicitly
// unqualified: classic archive policy/apply clocks are absent from this journal.
func compareRaw(name string, rows []v2core.Row, classic *pb.FetchResponse, from, until int64) (int, int, []difference, error) {
	step := int64(classic.StepTime)
	if step <= 0 || len(classic.IsAbsent) != len(classic.Values) {
		return 0, 0, nil, errors.New("invalid classic response geometry")
	}
	// Whisper fetch starts at the next slot after from. Compare the actual
	// returned slot coverage intersected with our frozen window; a raw point
	// exactly at the excluded API boundary is not a missing classic slot.
	from = max(from, int64(classic.StartTime))
	until = min(until, int64(classic.StartTime)+int64(len(classic.Values))*step)
	// Fine archives may have steps above one second. Restrict to exact timestamps;
	// unaligned input needs original classic slot/apply semantics, not a raw oracle.
	raw := map[int64]slot{}
	for _, r := range rows {
		if r.Time < from || r.Time >= until {
			continue
		}
		if r.Time%step != 0 {
			return 0, 0, nil, fmt.Errorf("unaligned raw timestamp %d for step %d", r.Time, step)
		}
		if old, ok := raw[r.Time]; !ok || r.Seq > old.Seq {
			raw[r.Time] = slot{r.Bits, r.Seq}
		}
	}
	compared, mismatches := 0, 0
	var examples []difference
	for i, v := range classic.Values {
		ts := int64(classic.StartTime) + int64(i)*step
		if ts < from || ts >= until {
			continue
		}
		r, present := raw[ts]
		occupied := !classic.IsAbsent[i]
		if !present && !occupied {
			continue
		}
		compared++
		if present != occupied || (present && r.Bits != math.Float64bits(v)) {
			mismatches++
			if true {
				examples = append(examples, difference{name, ts, fmt.Sprintf("%016x", r.Bits), fmt.Sprintf("%016x", math.Float64bits(v)), present, occupied})
			}
		}
		delete(raw, ts)
	}
	for ts, r := range raw {
		compared++
		mismatches++
		if true {
			examples = append(examples, difference{Name: name, Time: ts, V2Bits: fmt.Sprintf("%016x", r.Bits), V2Present: true})
		}
	}
	return compared, mismatches, examples, nil
}

type totals struct{ Names, Points, Mismatches, Failures uint64 }

func compare(job compareJob, base string, sums *totals) bool {
	if job.Layer == 3 {
		return compareL3(job, base, sums)
	}
	start := time.Now()
	defer func() {
		for _, s := range job.Snapshots {
			_ = s.Close()
		}
	}()
	selected := map[uint64][]v2core.Row{}
	for _, n := range job.Names {
		selected[n.ID] = nil
	}
	var scanErr error
	for _, s := range job.Snapshots {
		if err := v2core.ScanRaw(s, func(r v2core.Row) error {
			if _, ok := selected[r.ID]; ok && r.Time >= job.From && r.Time < job.Until {
				selected[r.ID] = append(selected[r.ID], r)
			}
			return nil
		}); err != nil {
			scanErr = err
			break
		}
	}
	result := map[string]any{"stage": "comparison", "context": contextLoad(), "from": job.From, "until": job.Until, "source_path": job.SourcePath, "source_offset": job.SourceOffset, "source_caught_up_at_snapshot": job.SourceCaughtUp, "requested_names": len(job.Names), "names": job.Names, "attempt": job.Attempt, "since_first_snapshot_seconds": time.Since(job.FirstSnapshot).Seconds(), "scan_seconds": time.Since(start).Seconds()}
	result["post_close_recheck"] = job.PostClose
	result["source_lag_seconds"] = job.SourceLagSeconds
	result["source_segments_behind"] = job.SourceSegmentsBehind
	if scanErr != nil {
		sums.Failures++
		result["error"] = scanErr.Error()
		emit(result)
		return true
	}
	client := &http.Client{Timeout: 5 * time.Second}
	pairs, total, bad := 0, 0, 0
	var examples []difference
	var failures []string
	for _, n := range job.Names {
		classic, err := fetch(client, base, n.Name, job.From, job.Until)
		if err != nil {
			failures = append(failures, n.Name+": "+err.Error())
			continue
		}
		count, mismatch, ex, err := compareRaw(n.Name, selected[n.ID], classic, job.From, job.Until)
		if err != nil {
			failures = append(failures, n.Name+": "+err.Error())
			continue
		}
		pairs++
		total += count
		bad += mismatch
		examples = append(examples, ex...)
	}
	sums.Names += uint64(pairs)
	sums.Points += uint64(total)
	sums.Mismatches += uint64(bad)
	sums.Failures += uint64(len(failures))
	result["compared_names"] = pairs
	result["compared_points"] = total
	result["mismatches"] = bad
	result["examples"] = examples
	result["failures"] = failures
	result["seconds"] = time.Since(start).Seconds()
	result["rollups_qualified"] = false
	emit(result)
	return bad > 0 || len(failures) > 0
}

func run() (retErr error) {
	source := flag.String("source", "", "read-only classic input journal directory")
	out := flag.String("out", "", "fresh private output directory")
	base := flag.String("classic", "http://127.0.0.1:8080", "classic read API")
	duration := flag.Duration("duration", 20*time.Minute, "wall duration")
	shards := flag.Int("shards", 4, "core owners")
	interval := flag.Duration("compare-interval", time.Minute, "comparison cadence")
	sample := flag.Int("sample", 16, "rotating names per comparison")
	lag := flag.Duration("lag", 120*time.Second, "fixed measured/exposed exclusion, not a parity waiver")
	sampleEvery := flag.Uint64("sample-every", 1, "admit all points only for names whose FNV64a hash modulo this value is zero; 1 admits all")
	policyFile := flag.String("policies", "", "fixed JSON name-to-policy map; L3 admits only this population and performs no policy lookups")
	capacityOnly := flag.Bool("capacity-only", false, "measure admission without expensive parity scans")
	layer := flag.Int("layer", 3, "3 rollup (v2 has no raw journal layer); L3 reconstructs receiver applications, not original classic applications")
	fullL3 := flag.Bool("full-l3", false, "admit every name with classic info or paired classic policy files; fail on missing policy")
	workers := flag.Int("workers", 0, "partition-parallel decoder/catalog workers (1..16); requires capacity-only or fixed L3 policies")
	receipts := flag.String("classic-receipts", "", "JSON array of frozen differences to reobserve through classic read API")
	recheckQuiet := flag.Bool("recheck-quiet", false, "retain all differences but omit equal-slot observations during a frozen recheck")
	recheck := flag.String("recheck", "", "one saved comparison JSON object to check against a stopped existing out root, opened read-only")
	flag.Parse()
	if (*classicSchemasPath != "" || *classicAggregationPath != "") && (*workers == 0 || !*fullL3 || *layer != 3) {
		return errors.New("classic policy files require parallel full-l3")
	}
	if *receipts != "" {
		return reobserveClassic(*receipts, *base)
	}
	if *workers > 0 {
		return runParallel(*source, *out, *base, *policyFile, *duration, *interval, *lag, *workers, *shards, *sample, *layer, *capacityOnly, *fullL3)
	}
	if *recheck != "" {
		return recheckFrozen(*out, *recheck, *base, *shards, *recheckQuiet)
	}
	if *source == "" || *out == "" || *duration <= 0 || *sample < 1 || *interval <= 0 || *lag < 0 || *sampleEvery < 1 {
		return errors.New("source, fresh out and positive limits required")
	}
	sourceAbs, err := filepath.Abs(*source)
	if err != nil {
		return err
	}
	outAbs, err := filepath.Abs(*out)
	if err != nil {
		return err
	}
	sourceAbs, err = filepath.EvalSymlinks(sourceAbs)
	if err != nil {
		return err
	}
	// Resolve the existing parent too, so symlink aliases cannot put output in
	// the journal directory while appearing to use a different path.
	parent, err := filepath.EvalSymlinks(filepath.Dir(outAbs))
	if err != nil {
		return err
	}
	outAbs = filepath.Join(parent, filepath.Base(outAbs))
	if outAbs == sourceAbs || strings.HasPrefix(outAbs, sourceAbs+string(os.PathSeparator)) || strings.HasPrefix(sourceAbs, outAbs+string(os.PathSeparator)) {
		return errors.New("source and output directories must not overlap")
	}
	paths, err := filepath.Glob(filepath.Join(*source, "*.log"))
	if err != nil {
		return err
	}
	if len(paths) == 0 {
		return errors.New("no source journal")
	}
	path := paths[len(paths)-1]
	tail, err := readyShadowTail(path)
	if err != nil {
		return err
	}
	defer func() { tail.close() }()
	// Decode the newest segment dictionary to its live edge before admitting.
	// Historical frames are needed for decoder state, never submitted to the core.
	warmup := time.Now()
	var skipped uint64
	for {
		groups, _, e := tail.next()
		if errors.Is(e, io.EOF) {
			seq := binary.LittleEndian.Uint64(tail.header[8:])
			nextPath := filepath.Join(*source, fmt.Sprintf("%016x.log", seq+1))
			if _, e = os.Stat(nextPath); e == nil {
				next, e := readyShadowTail(nextPath)
				if e != nil {
					return e
				}
				tail.close()
				tail = next
				path = nextPath
				continue
			}
			if !errors.Is(e, os.ErrNotExist) {
				return e
			}
			break
		}
		if errors.Is(e, io.ErrUnexpectedEOF) && time.Since(warmup) < time.Minute {
			time.Sleep(25 * time.Millisecond)
			continue
		}
		if e != nil {
			return fmt.Errorf("live-tail warmup: %w", e)
		}
		for _, g := range groups {
			skipped += uint64(len(g.Data))
		}
		if time.Since(warmup) > time.Minute {
			return errors.New("cannot reach live tail within one minute")
		}
	}
	emit(map[string]any{"stage": "live_tail", "context": contextLoad(), "source_path": path, "source_offset": tail.offset, "skipped_history_points": skipped, "warmup_seconds": time.Since(warmup).Seconds()})
	if err = os.Mkdir(*out, 0700); err != nil {
		return fmt.Errorf("fresh output directory required: %w", err)
	}
	store, err := v2daemon.Open(*out, v2daemon.Config{Layer: *layer, Producers: 1, Shards: *shards, MaxPendingRows: 2 << 20, GroupInterval: time.Second})
	if err != nil {
		return err
	}
	defer func() {
		if e := store.Close(); retErr == nil {
			retErr = e
		}
	}()
	producer := store.Producers[0]
	start := time.Now()
	deadline := start.Add(*duration)
	sealAt := start.Add(time.Second)
	sampleAt := start.Add(*interval)
	from := start.Unix() + 60
	sums := &totals{}
	jobs := make(chan compareJob, 1)
	retries := make(chan compareJob, 2)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for job := range jobs {
			if compare(job, *base, sums) && job.Attempt < 2 {
				job.Snapshots = nil
				job.Attempt++
				select {
				case retries <- job:
				default:
				}
			}
		}
	}()
	defer func() {
		close(jobs)
		<-done
		emit(map[string]any{"stage": "comparison_totals", "context": contextLoad(), "totals": sums})
		if retErr == nil && ((!*capacityOnly && sums.Points == 0) || sums.Mismatches > 0 || sums.Failures > 0) {
			retErr = fmt.Errorf("comparison incomplete or differing: %+v", *sums)
		}
	}()
	names := map[string]uint64{}
	policies := map[string][]byte{}
	if *policyFile != "" {
		b, e := os.ReadFile(*policyFile)
		if e != nil {
			return e
		}
		var raw map[string]v2core.RollupPolicy
		if e = json.Unmarshal(b, &raw); e != nil {
			return e
		}
		for n, p := range raw {
			encoded, e := v2core.EncodeRollupPolicy(p)
			if e != nil {
				return e
			}
			policies[n] = encoded
		}
	}
	policyClient := &http.Client{Timeout: 5 * time.Second}
	var applicationOrder uint64
	var candidates []candidate
	rotation := 0
	var sequence, decodedBytes uint64
	var partialSince time.Time
	var caughtUp bool
	lastCaughtUp := time.Now()
	var decodedPoints uint64
	batchNames := make([]string, 0, 32768)
	batchData := make([]points.Point, 0, 32768)
	batchSeq := make([]uint64, 0, 32768)
	flush := func() error {
		if len(batchData) == 0 {
			return nil
		}
		e := producer.Append(batchNames, nil, batchData, batchSeq, time.Now().Unix())
		batchNames = batchNames[:0]
		batchData = batchData[:0]
		batchSeq = batchSeq[:0]
		return e
	}
	emit(map[string]any{"stage": "start", "context": contextLoad(), "source_path": path, "duration_seconds": duration.Seconds(), "layer": *layer, "capacity_only": *capacityOnly, "raw_only": *layer == 2, "sample_every": *sampleEvery, "fresh_history_from": from})
	for time.Now().Before(deadline) {
		now := time.Now()
		if !now.Before(sealAt) {
			if err = flush(); err != nil {
				return err
			}
			if err = producer.Seal(); err != nil {
				return err
			}
			if err = producer.Drain(); err != nil {
				return err
			}
			sealAt = now.Add(time.Second)
		}
		if !now.Before(sampleAt) {
			stats := v2core.Sample(store.Core, "sample", start, sequence)
			stats["context"] = contextLoad()
			stats["source_path"] = path
			stats["source_offset"] = tail.offset
			stats["source_caught_up"] = caughtUp
			stats["source_bytes"] = decodedBytes
			stats["names"] = len(names)
			stats["decoded_points"] = decodedPoints
			stats["sample_every"] = *sampleEvery
			stats["source_lag_seconds"] = time.Since(lastCaughtUp).Seconds()
			stats["source_backlog"] = sourceBacklog(*source, path, tail.offset)
			selfCPU, _ := os.ReadFile("/proc/self/stat")
			stats["self_cpu_stat"] = string(selfCPU)
			emit(stats)
			until := lastCaughtUp.Add(-*lag).Unix()
			if !*capacityOnly && until > from && len(candidates) > 0 && len(jobs) == 0 {
				job := compareJob{Layer: *layer, From: from, Until: until, FirstSnapshot: now}
				select {
				case job = <-retries:
				default:
					for i := 0; i < *sample && i < len(candidates); i++ {
						job.Names = append(job.Names, candidates[rotation%len(candidates)])
						rotation++
					}
				}
				// Fence this producer before pinning; retries keep the exact names
				// and numeric window while observing a new immutable publication.
				if err = flush(); err != nil {
					return err
				}
				if err = producer.Seal(); err != nil {
					return err
				}
				if err = producer.Drain(); err != nil {
					return err
				}
				if err = store.Core.Append(0, nil, sequence, true); err != nil {
					return err
				}
				job.SourcePath, job.SourceOffset, job.SourceCaughtUp = path, tail.offset, caughtUp
				job.SourceLagSeconds = time.Since(lastCaughtUp).Seconds()
				available, e := filepath.Glob(filepath.Join(*source, "*.log"))
				if e != nil {
					return e
				}
				if len(available) > 0 {
					newest, e := strconv.ParseUint(strings.TrimSuffix(filepath.Base(available[len(available)-1]), ".log"), 16, 64)
					if e != nil {
						return e
					}
					current := binary.LittleEndian.Uint64(tail.header[8:])
					if newest < current {
						return errors.New("source epoch regressed")
					}
					job.SourceSegmentsBehind = int(newest - current)
				}
				if job.SourceLagSeconds > 10 || job.SourceSegmentsBehind != 0 {
					return errors.New("comparison source backlog exceeds bound")
				}
				for _, db := range store.Core.DBs() {
					job.Snapshots = append(job.Snapshots, db.NewSnapshot())
				}
				jobs <- job
			}
			sampleAt = now.Add(*interval)
		}
		groups, n, e := tail.next()
		if errors.Is(e, io.EOF) || errors.Is(e, io.ErrUnexpectedEOF) {
			if errors.Is(e, io.ErrUnexpectedEOF) {
				if partialSince.IsZero() {
					partialSince = time.Now()
				}
				if time.Since(partialSince) > 5*time.Second {
					return errors.New("partial/corrupt frame stalled 5s")
				}
			}
			if errors.Is(e, io.EOF) {
				seq := binary.LittleEndian.Uint64(tail.header[8:])
				nextPath := filepath.Join(*source, fmt.Sprintf("%016x.log", seq+1))
				if _, e = os.Stat(nextPath); e == nil {
					next, e := readyShadowTail(nextPath)
					if e != nil {
						return e
					}
					tail.close()
					tail = next
					path = nextPath
					caughtUp = false
					continue
				}
				if !errors.Is(e, os.ErrNotExist) {
					return e
				}
				available, e := filepath.Glob(filepath.Join(*source, "*.log"))
				if e != nil {
					return e
				}
				for _, later := range available {
					if later > nextPath {
						return fmt.Errorf("source gap before %s", later)
					}
				}
				caughtUp = true
				lastCaughtUp = time.Now()
			}
			time.Sleep(25 * time.Millisecond)
			continue
		}
		if e != nil {
			return e
		}
		caughtUp = false
		partialSince = time.Time{}
		decodedBytes += uint64(n)
		for _, group := range groups {
			decodedPoints += uint64(len(group.Data))
			if !selectedName(group.Metric, *sampleEvery) {
				continue
			}
			name, e := tags.Normalize(group.Metric)
			if e != nil {
				return e
			}
			if *layer == 3 && *policyFile != "" && policies[name] == nil {
				continue
			}
			_, ok := names[name]
			if !ok {
				id := uint64(len(names) + 1)
				names[name] = id
				if (*layer == 3 || *sampleEvery > 1 || id%8192 == 1) && len(candidates) < 4096 {
					candidates = append(candidates, candidate{id, name})
				}
			}

			if *layer == 3 {
				policy := policies[name]
				if policy == nil {
					policy, e = fetchPolicy(policyClient, *base, name)
					if e != nil {
						return e
					}
					policies[name] = policy
					emit(map[string]any{"stage": "policy", "name": name, "policy": json.RawMessage(policy), "context": contextLoad()})
				}
				for at := 0; at < len(group.Data); {
					end := min(at+32768, len(group.Data))
					seqs := make([]uint64, end-at)
					for i := range seqs {
						sequence++
						seqs[i] = sequence
					}
					applicationOrder++
					if e = producer.AppendApplications([]v2daemon.Application{{Name: name, Policy: policy, Order: applicationOrder, Clock: time.Now().Unix(), Points: group.Data[at:end], Sequence: seqs}}); e != nil {
						return e
					}
					at = end
				}
				continue
			}
			for _, point := range group.Data {
				sequence++
				batchNames = append(batchNames, name)
				batchData = append(batchData, point)
				batchSeq = append(batchSeq, sequence)
				if len(batchData) == cap(batchData) {
					if err = flush(); err != nil {
						return err
					}
				}
			}
		}
	}
	if err = flush(); err != nil {
		return err
	}
	if err = producer.Seal(); err != nil {
		return err
	}
	if err = producer.Drain(); err != nil {
		return err
	}
	emit(map[string]any{"stage": "intake_complete", "context": contextLoad(), "points": sequence, "seconds": time.Since(start).Seconds(), "frontiers": store.Core.DurableFrontiers(), "source_path": path, "source_offset": tail.offset})
	return nil
}
func main() {
	if err := run(); err != nil {
		emit(map[string]any{"stage": "fatal", "context": contextLoad(), "error": err.Error()})
		os.Exit(1)
	}
	emit(map[string]any{"stage": "closed", "context": contextLoad()})
}

// Stable name selection retains every group/point for an admitted identity.
func selectedName(name string, every uint64) bool {
	if every == 1 {
		return true
	}
	h := fnv.New64a()
	_, _ = h.Write([]byte(name))
	return h.Sum64()%every == 0
}
