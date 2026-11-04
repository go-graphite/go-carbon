// sealed-inventory reads immutable SSTs without opening a Pebble database.
// Rewritten tables contain private experimental frames: never ingest them.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"golang.org/x/time/rate"
)

type config struct {
	asOf                                  string
	writeWindow                           time.Duration
	mode, dir, tmp, out, policies, levels string
	metadataDir                           string
	files, procs, level                   int
	readMB                                float64
	keep                                  bool
}
type tally struct {
	Entries, Points, LogicalBytes       uint64
	PhysicalBytes                       float64
	Full4096, Partial, Larger           uint64
	TombstonedEntries, TombstonedPoints uint64
}
type policyReport struct {
	Bytes                                        uint64
	SavedBytes                                   int64
	WholeFileSavingPct, FloatBytesSavingPct      float64
	EncodeDeltaNSPerPoint, DecodeDeltaNSPerPoint float64
	EncodeCPU, DecodeCPU                         int64
	Points, ChangedGroups, OracleFailures        uint64
	EncodeNSPerPoint, DecodeNSPerPoint           float64
	Winners                                      map[string]uint64
}
type fileReport struct {
	Ordinary                                                       map[string]*ordinaryRow
	OrdinaryByReason                                               map[string]*tally
	Name                                                           string
	Bytes, DataBytes, IndexBytes, FilterBytes, OverheadBytes       uint64
	FloatDataBytes, SealedDataBytes, FloatSharePct, SealedSharePct float64
	RangeDelSpans, RangeDelKeys                                    uint64
	Classes                                                        map[string]*tally
	Policies                                                       map[string]*policyReport
	BaselineBytes                                                  uint64
	BaselineDeltaPct                                               float64
	BaselineWithin3Pct                                             bool
	Level                                                          int
	LevelSource, WriterOptions                                     string
}
type report struct {
	OrdinaryCutoffUTC                                                string
	Ordinary                                                         map[string]*ordinaryRow
	OrdinaryByReason                                                 map[string]*tally
	SchemaVersion                                                    int
	Mode, Architecture, GoVersion, StartedUTC                        string
	Procs                                                            int
	ReadMBPerSecond                                                  float64
	ReadBytes                                                        int64
	WallSeconds                                                      float64
	FileBytes                                                        uint64
	Classes                                                          map[string]*tally
	DataBytes, IndexBytes, FilterBytes, OverheadBytes, BaselineBytes uint64
	FloatDataBytes, SealedDataBytes, FloatSharePct, SealedSharePct   float64
	Policies                                                         map[string]*policyReport
	Files                                                            []*fileReport
	Notes                                                            []string
}
type runner struct {
	ordinary                 *ordinaryIndex
	ctx                      context.Context
	cfg                      config
	limiter                  *rate.Limiter
	readBytes                atomic.Int64
	done                     atomic.Int64
	levels                   map[string]int
	outputBytes, outputLimit uint64
	tempDirs                 []string
}

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "sealed-inventory:", err)
		os.Exit(1)
	}
}
func run(args []string) error {
	if len(args) == 0 || (args[0] != "inventory" && args[0] != "transcode") {
		return errors.New("usage: sealed-inventory inventory|transcode -sst-dir DIR -out report.json [-tmp DIR] [-policy current,prefix256,minfour,alp,chimp]")
	}
	c := config{mode: args[0]}
	fs := flag.NewFlagSet(c.mode, flag.ContinueOnError)
	fs.StringVar(&c.asOf, "as-of", "", "ordinary reason model clock (RFC3339; default scan start)")
	fs.DurationVar(&c.writeWindow, "write-window", 24*time.Hour, "ordinary reason model: native range cold age")
	fs.StringVar(&c.metadataDir, "metadata-dir", "", "all immutable store SSTs for descriptor/manifest index seeks (default sst-dir)")
	fs.StringVar(&c.dir, "sst-dir", "", "directory of immutable SST files")
	fs.StringVar(&c.out, "out", "", "JSON report (outside the SST directory)")
	fs.StringVar(&c.tmp, "tmp", "", "temporary output directory, outside SST directory")
	fs.StringVar(&c.policies, "policy", "current,prefix256,minfour,alp,chimp", "comma-separated policies")
	fs.StringVar(&c.levels, "levels-json", "", "optional JSON filename-to-level mapping, copied from a known snapshot")
	fs.IntVar(&c.level, "level", 6, "fallback writer level (0..6)")
	fs.IntVar(&c.files, "files", 0, "first N files in sorted filename order; 0=all (sample before linking)")
	fs.IntVar(&c.procs, "procs", 4, "GOMAXPROCS cap; codec CPU measurements run serially")
	fs.Float64Var(&c.readMB, "read-mb-s", 200, "read token bucket MiB/s; 0=unlimited")
	fs.BoolVar(&c.keep, "keep", false, "retain private experimental tables")
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}
	if fs.NArg() != 0 || c.dir == "" || c.out == "" || c.procs < 1 || c.files < 0 || c.level < 0 || c.level > 6 || c.readMB < 0 || math.IsNaN(c.readMB) || math.IsInf(c.readMB, 0) {
		return errors.New("invalid/missing flags")
	}
	if c.mode == "transcode" && c.tmp == "" {
		return errors.New("transcode requires -tmp")
	}
	for _, p := range strings.Split(c.policies, ",") {
		switch p {
		case "current", "prefix256", "minfour", "alp", "chimp":
		default:
			return fmt.Errorf("unknown policy %q", p)
		}
	}
	var err error
	c.dir, err = filepath.EvalSymlinks(c.dir)
	if err != nil {
		return err
	}
	c.dir, err = filepath.Abs(c.dir)
	if err != nil {
		return err
	}
	if c.metadataDir == "" {
		c.metadataDir = c.dir
	} else {
		c.metadataDir, err = filepath.EvalSymlinks(c.metadataDir)
		if err != nil {
			return err
		}
		c.metadataDir, err = filepath.Abs(c.metadataDir)
		if err != nil {
			return err
		}
	}
	for _, input := range []string{c.dir, c.metadataDir} {
		if err = outside(input, c.out); err != nil {
			return err
		}
		if c.mode == "transcode" {
			if err = outside(input, c.tmp); err != nil {
				return err
			}
		}
	}
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	previous := runtime.GOMAXPROCS(c.procs)
	defer runtime.GOMAXPROCS(previous)
	r := runner{ctx: ctx, cfg: c, levels: map[string]int{}}
	defer func() {
		if ctx.Err() != nil {
			for _, dir := range r.tempDirs {
				_ = os.RemoveAll(dir)
			}
		}
	}()
	if c.readMB > 0 {
		r.limiter = rate.NewLimiter(rate.Limit(c.readMB*(1<<20)), 1<<20)
	}
	if c.levels != "" {
		b, e := os.ReadFile(c.levels)
		if e != nil {
			return e
		}
		if e = json.Unmarshal(b, &r.levels); e != nil {
			return e
		}
		for _, v := range r.levels {
			if v < 0 || v > 6 {
				return errors.New("invalid level mapping")
			}
		}
	}
	files, total, err := listFiles(c.dir, c.files)
	if err != nil {
		return err
	}
	if c.mode == "transcode" {
		if err = spaceCheck(c.tmp, total); err != nil {
			return err
		}
	}
	r.outputLimit = 2 * total
	start := time.Now()
	progressDone := make(chan struct{})
	defer close(progressDone)
	go func() {
		tick := time.NewTicker(10 * time.Second)
		defer tick.Stop()
		for {
			select {
			case <-progressDone:
				return
			case <-tick.C:
				fmt.Fprintf(os.Stderr, "elapsed=%s files=%d/%d read_MiB=%.1f\n", time.Since(start).Round(time.Second), r.done.Load(), len(files), float64(r.readBytes.Load())/(1<<20))
			}
		}
	}()
	if c.writeWindow < 0 {
		return errors.New("write-window must be nonnegative")
	}
	metadataFiles, _, err := listFiles(c.metadataDir, 0)
	if err != nil {
		return err
	}
	index, err := r.openOrdinaryIndex(files)
	if err == nil {
		index.metadataPaths = metadataFiles
		index.runner = &r
	}
	if err != nil {
		return err
	}
	analysisNow := start
	if c.asOf != "" {
		analysisNow, err = time.Parse(time.RFC3339, c.asOf)
		if err != nil {
			index.close()
			return err
		}
	}
	index.cutoff = analysisNow.Add(-c.writeWindow).Unix()
	defer index.close()
	r.ordinary = index
	result := report{OrdinaryCutoffUTC: analysisNow.Add(-c.writeWindow).UTC().Format(time.RFC3339), Ordinary: map[string]*ordinaryRow{}, OrdinaryByReason: map[string]*tally{}, SchemaVersion: 3, Mode: c.mode, Architecture: runtime.GOOS + "/" + runtime.GOARCH, GoVersion: runtime.Version(), StartedUTC: start.UTC().Format(time.RFC3339), Procs: c.procs, ReadMBPerSecond: c.readMB, Classes: map[string]*tally{}, Notes: []string{
		"Ordinary rows partition ordinary bytes. Schema comes from store-wide manifests, else only archive ordinal is known. Newest-observed/open-candidate is relative to sampled tail groups, not proof of the live writer head. Missing metadata, obsolete versions and descriptor mismatch remain explicit. Descriptors/manifests are joined across every SST in metadata-dir using bounded index seeks, with a 32-reader LRU, 128-file/metric LRU and 2 MiB/8192-entry per-file/metric bound. Tail temperature/position still uses selected SSTs only; unseen tails remain unknown. Cross-file range deletions are not resolved.",
		"PhysicalBytes is compressed data-block bytes including 5-byte trailers apportioned by user-key+value logical bytes; it is an exact accounting convention, not independent compressibility.",
		"Bloom bits/key are inferred from serialized filter size; confirm BaselineWithin3Pct before extrapolating paired rewrite savings.",
		"Every internal point entry/version is counted, including obsolete entries and point deletions. Tombstoned counts reflect only newer range deletions in the same file; cross-file liveness is not established by this offline inventory.",
		"Files are scanned serially; process CPU (getrusage) brackets codec work only, excludes SST I/O/writer work and the bit oracle. GC and runtime background work can contribute.",
		"Whole-file savings compare paired unchanged and candidate rewrites; original-file delta and baseline agreement are reported separately. These are per-file rewrites, not a cross-file compaction.",
		"Production uses pure-Go Snappy outer blocks; inner research Zstd uses klauspost pure Go. No cgo required.",
	}}
	result.Policies = map[string]*policyReport{}
	for _, path := range files {
		if err = ctx.Err(); err != nil {
			return err
		}
		f, e := r.scan(path)
		if e != nil {
			return fmt.Errorf("%s: %w", filepath.Base(path), e)
		}
		result.Files = append(result.Files, f)
		for key, row := range f.Ordinary {
			dst := result.Ordinary[key]
			if dst == nil {
				copyRow := *row
				copyRow.tally = tally{}
				dst = &copyRow
				result.Ordinary[key] = dst
			}
			addTally(&dst.tally, &row.tally)
		}
		for key, t := range f.OrdinaryByReason {
			if result.OrdinaryByReason[key] == nil {
				result.OrdinaryByReason[key] = &tally{}
			}
			addTally(result.OrdinaryByReason[key], t)
		}
		result.FileBytes += f.Bytes
		result.DataBytes += f.DataBytes
		result.IndexBytes += f.IndexBytes
		result.FilterBytes += f.FilterBytes
		result.OverheadBytes += f.OverheadBytes
		result.BaselineBytes += f.BaselineBytes
		result.FloatDataBytes += f.FloatDataBytes
		result.SealedDataBytes += f.SealedDataBytes
		for name, p := range f.Policies {
			dst := result.Policies[name]
			if dst == nil {
				dst = &policyReport{Winners: map[string]uint64{}}
				result.Policies[name] = dst
			}
			dst.Bytes += p.Bytes
			dst.EncodeCPU += p.EncodeCPU
			dst.DecodeCPU += p.DecodeCPU
			dst.Points += p.Points
			dst.ChangedGroups += p.ChangedGroups
			dst.OracleFailures += p.OracleFailures
			for winner, n := range p.Winners {
				dst.Winners[winner] += n
			}
		}

		for k, v := range f.Classes {
			dst := result.Classes[k]
			if dst == nil {
				dst = &tally{}
				result.Classes[k] = dst
			}
			addTally(dst, v)
		}
		r.done.Add(1)
	}
	if result.FileBytes > 0 {
		result.FloatSharePct = 100 * result.FloatDataBytes / float64(result.FileBytes)
		result.SealedSharePct = 100 * result.SealedDataBytes / float64(result.FileBytes)
	}
	finishPolicies(result.Policies, result.BaselineBytes, result.FloatDataBytes)
	result.WallSeconds = time.Since(start).Seconds()
	result.ReadBytes = r.readBytes.Load()
	if err = ctx.Err(); err != nil {
		return err
	}
	b, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return err
	}
	// Exclusive creation avoids overwriting SST hard links or a previous report.
	out, err := os.OpenFile(c.out, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	_, err = out.Write(append(b, '\n'))
	return errors.Join(err, out.Close())
}
func addTally(a, b *tally) {
	a.Entries += b.Entries
	a.Points += b.Points
	a.LogicalBytes += b.LogicalBytes
	a.PhysicalBytes += b.PhysicalBytes
	a.Full4096 += b.Full4096
	a.Partial += b.Partial
	a.Larger += b.Larger
	a.TombstonedEntries += b.TombstonedEntries
	a.TombstonedPoints += b.TombstonedPoints
}
func outside(input, path string) error {
	abs, err := filepath.Abs(path)
	if err != nil {
		return err
	}
	// Resolve existing ancestors, including a non-existent output's parent.
	tail := ""
	cur := abs
	for {
		resolved, e := filepath.EvalSymlinks(cur)
		if e == nil {
			abs = filepath.Join(resolved, tail)
			break
		}
		if !os.IsNotExist(e) {
			return e
		}
		parent := filepath.Dir(cur)
		if parent == cur {
			return e
		}
		tail = filepath.Join(filepath.Base(cur), tail)
		cur = parent
	}
	rel, err := filepath.Rel(input, abs)
	if err != nil {
		return err
	}
	if rel == "." || (rel != ".." && !strings.HasPrefix(rel, ".."+string(os.PathSeparator))) {
		return fmt.Errorf("output %s is inside SST directory", path)
	}
	return nil
}
func listFiles(dir string, n int) ([]string, uint64, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, 0, err
	}
	var paths []string
	var total uint64
	for _, e := range entries {
		if !strings.HasSuffix(e.Name(), ".sst") {
			continue
		}
		if e.Type()&os.ModeSymlink != 0 {
			return nil, 0, fmt.Errorf("refusing SST symlink: %s", e.Name())
		}
		info, e2 := e.Info()
		if e2 != nil {
			return nil, 0, e2
		}
		if !info.Mode().IsRegular() {
			return nil, 0, errors.New("SST is not regular")
		}
		paths = append(paths, filepath.Join(dir, e.Name()))
	}
	sort.Strings(paths)
	if n > 0 && len(paths) > n {
		paths = paths[:n]
	}
	if len(paths) == 0 {
		return nil, 0, errors.New("no .sst files")
	}
	for _, p := range paths {
		st, e := os.Stat(p)
		if e != nil {
			return nil, 0, e
		}
		total += uint64(st.Size())
	}
	return paths, total, nil
}
func spaceCheck(dir string, input uint64) error {
	var st syscall.Statfs_t
	if err := syscall.Statfs(dir, &st); err != nil {
		return err
	}
	available := st.Bavail * uint64(st.Bsize)
	if available < 50<<30 {
		return errors.New("temporary filesystem has less than 50 GiB free")
	}
	if input > available/2 {
		return errors.New("2x sampled input exceeds free space")
	}
	return nil
}
