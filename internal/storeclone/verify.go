package storeclone

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/sstable"
)

// Report is the result of Verify.
type Report struct {
	Tables  int   `json:"tables"`
	Bytes   int64 `json:"bytes"`
	Missing int   `json:"missing"`
	Seconds int64 `json:"seconds"`
}

// Verify opens dir read-only, checks that every table its manifest names
// exists with the recorded size, and validates every block checksum of every
// live table with workers parallel readers.
func Verify(dir string, workers int) (Report, error) {
	started := time.Now()
	db, err := openReadOnly(dir, "")
	if err != nil {
		return Report{}, err
	}
	levels, err := db.SSTables()
	if cerr := db.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return Report{}, err
	}
	var report Report
	var names []string
	seen := map[string]bool{}
	for _, level := range levels {
		for _, t := range level {
			name := fmt.Sprintf("%06d.sst", uint64(t.BackingSSTNum))
			if seen[name] {
				continue
			}
			seen[name] = true
			info, err := os.Stat(filepath.Join(dir, name))
			if err != nil || !t.Virtual && uint64(info.Size()) != t.Size {
				report.Missing++
				continue
			}
			names = append(names, name)
			report.Bytes += info.Size()
		}
	}
	report.Tables = len(names)
	readerOpts := sstable.ReaderOptions{}
	if merger, err := recordedMerger(dir); err != nil {
		return report, err
	} else if merger != "" {
		readerOpts.MergerName, readerOpts.Merge = merger, pebble.DefaultMerger.Merge
	}
	var next atomic.Int64
	var mu sync.Mutex
	var errs []error
	var wg sync.WaitGroup
	for i := 0; i < max(1, workers); i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				n := next.Add(1) - 1
				if n >= int64(len(names)) {
					return
				}
				if err := validateTable(filepath.Join(dir, names[n]), readerOpts); err != nil {
					mu.Lock()
					errs = append(errs, fmt.Errorf("%s: %w", names[n], err))
					mu.Unlock()
				}
			}
		}()
	}
	wg.Wait()
	report.Seconds = int64(time.Since(started).Seconds())
	if report.Missing > 0 {
		errs = append(errs, fmt.Errorf("%d live tables missing or truncated", report.Missing))
	}
	return report, errors.Join(errs...)
}

func validateTable(path string, opts sstable.ReaderOptions) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	readable, err := sstable.NewSimpleReadable(f)
	if err != nil {
		_ = f.Close()
		return err
	}
	r, err := sstable.NewReader(readable, opts)
	if err != nil {
		_ = readable.Close()
		return err
	}
	return errors.Join(r.ValidateBlockChecksums(), r.Close())
}

// CacheGate pauses sending while the source daemon's write cache is at or
// above PauseAt of its limit, or its overflow-drop counter rose since the
// previous poll; it resumes below ResumeAt. Unreadable metrics pause too.
type CacheGate struct {
	URL               string
	PauseAt, ResumeAt float64
	Every             time.Duration
	// MinFree aborts (Err) when the staging filesystem's free share drops
	// below it, because the links keep compacted tables alive.
	StagingDir string
	MinFree    float64
	Log        io.Writer

	mu     sync.Mutex
	cond   *sync.Cond
	paused bool
	err    error
	ratio  float64
	drops  float64
}

// Run polls until ctx ends.
func (g *CacheGate) Run(ctx context.Context) {
	g.mu.Lock()
	g.cond = sync.NewCond(&g.mu)
	g.mu.Unlock()
	context.AfterFunc(ctx, func() { g.mu.Lock(); g.cond.Broadcast(); g.mu.Unlock() })
	g.poll(true)
	t := time.NewTicker(g.Every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			g.poll(false)
		}
	}
}

func (g *CacheGate) poll(first bool) {
	ratio, drops, err := cacheState(g.URL)
	var free float64 = 1
	if g.StagingDir != "" {
		var st syscall.Statfs_t
		if e := syscall.Statfs(g.StagingDir, &st); e == nil && st.Blocks > 0 {
			free = float64(st.Bavail) / float64(st.Blocks)
		}
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	was := g.paused
	switch {
	case free < g.MinFree:
		g.err = fmt.Errorf("staging filesystem free %.1f%% below %.0f%%", free*100, g.MinFree*100)
	case err != nil:
		g.paused = true
	case ratio >= g.PauseAt || !first && drops > g.drops:
		g.paused = true
	case ratio < g.ResumeAt:
		g.paused = false
	}
	if err == nil {
		g.ratio, g.drops = ratio, drops
	}
	if g.Log != nil && (was != g.paused || err != nil || g.err != nil) {
		fmt.Fprintf(g.Log, "%s gate paused=%v cache=%.1f%% drops=%.0f free=%.1f%% err=%v abort=%v\n",
			time.Now().Format(time.RFC3339), g.paused, ratio*100, drops, free*100, err, g.err)
	}
	g.cond.Broadcast()
}

// Wait blocks while paused.
func (g *CacheGate) Wait(ctx context.Context) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	for g.paused && g.err == nil && ctx.Err() == nil {
		g.cond.Wait()
	}
	if g.err != nil {
		return g.err
	}
	return ctx.Err()
}

// State returns the last cache ratio and whether sending is paused.
func (g *CacheGate) State() (float64, bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.ratio, g.paused
}

// cacheState reads the cache fill (the larger of points and bytes against
// their limits) and the overflow-drop counter from Prometheus text.
func cacheState(url string) (float64, float64, error) {
	client := http.Client{Timeout: 10 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		return 0, 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 0, 0, fmt.Errorf("metrics: %s", resp.Status)
	}
	v := map[string]float64{}
	s := bufio.NewScanner(resp.Body)
	s.Buffer(make([]byte, 1<<20), 1<<20)
	for s.Scan() {
		line := s.Text()
		if strings.HasPrefix(line, "#") {
			continue
		}
		name, rest, _ := strings.Cut(line, "{")
		if i := strings.LastIndexByte(rest, '}'); i >= 0 {
			rest = rest[i+1:]
		} else {
			name, rest, _ = strings.Cut(line, " ")
		}
		fields := strings.Fields(rest)
		if len(fields) == 0 {
			continue
		}
		if f, err := strconv.ParseFloat(fields[0], 64); err == nil {
			v[name] += f
		}
	}
	if err := s.Err(); err != nil {
		return 0, 0, err
	}
	points, ok := v["cache_points"]
	if !ok {
		return 0, 0, errors.New("metrics: no cache_points")
	}
	ratio := 0.0
	if limit := v["cache_limit_points"]; limit > 0 {
		ratio = points / limit
	}
	byteLimit := v["cache_limit_bytes"]
	if byteLimit == 0 && v["cache_limit_memory_share"] > 0 {
		byteLimit = v["cache_limit_memory_share"] * memTotal()
	}
	if byteLimit > 0 {
		ratio = math.Max(ratio, v["cache_estimated_bytes"]/byteLimit)
	}
	return ratio, v["cache_overflow_dropped_points_total"], nil
}

func memTotal() float64 {
	data, err := os.ReadFile("/proc/meminfo")
	if err != nil {
		return 0
	}
	for _, line := range strings.Split(string(data), "\n") {
		if f := strings.Fields(line); len(f) >= 2 && f[0] == "MemTotal:" {
			kb, _ := strconv.ParseFloat(f[1], 64)
			return kb * 1024
		}
	}
	return 0
}

// WriteJSON writes v as indented JSON.
func WriteJSON(w io.Writer, v any) error {
	e := json.NewEncoder(w)
	e.SetIndent("", " ")
	return e.Encode(v)
}
