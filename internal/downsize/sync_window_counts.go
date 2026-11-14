package downsize

import (
	"context"
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// A baseline is durable before the native import starts. Retrying the same
// transfer must not replace it with the already repaired target's count.
type windowPointCount struct {
	Source     string    `json:"source"`
	Window     syncRange `json:"window"`
	Before     *int64    `json:"before"`
	After      *int64    `json:"after"`
	Superseded bool      `json:"superseded_by_ranges,omitempty"`
}

type windowRepairCount struct {
	Unit      string `json:"unit"`
	Added     *int64 `json:"filled_points"`
	Transfers int    `json:"measured_transfers"`
	Unknown   int    `json:"unmeasured_transfers"`
}

func countWindowPoints(d map[string][]syncDigest) int64 {
	var n int64
	for _, archives := range d {
		for _, a := range archives {
			n += a.Count
		}
	}
	return n
}

func readWindowPointCount(path string, initial windowPointCount) (windowPointCount, error) {
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return initial, atomicStateJSON(path, initial)
	}
	if err != nil {
		return initial, err
	}
	var saved windowPointCount
	err = json.Unmarshal(data, &saved)
	// An interrupted measurement may overlap repairs subsequently made from
	// another source. Preserve uncertainty rather than count those points twice.
	if err == nil && saved.After == nil {
		saved.Before = nil
		err = atomicStateJSON(path, saved)
	}
	return saved, err
}

func collectWindowPointCounts(state string) (windowRepairCount, error) {
	var total int64
	out := windowRepairCount{Unit: "finite serving-archive points added in selected windows; includes missing-name copies; not raw input events"}
	err := filepath.WalkDir(state, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".window-points.json") {
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		var c windowPointCount
		if err := json.Unmarshal(data, &c); err != nil {
			return err
		}
		if c.Superseded {
			return nil
		}
		if c.Before == nil || c.After == nil || *c.After < *c.Before {
			out.Unknown++
			return nil
		}
		out.Transfers++
		total += *c.After - *c.Before
		return nil
	})
	if out.Unknown == 0 && out.Transfers > 0 {
		out.Added = &total
	}
	return out, err
}

// Keep completed legacy totals exactly once. An incomplete legacy baseline
// cannot be apportioned across ranges after a crash; retain it as a marker and
// let each bounded range record its own measurement instead.
func prepareWindowRangeCounts(path string, initial windowPointCount) (bool, error) {
	data, err := os.ReadFile(path)
	if err == nil {
		if err = json.Unmarshal(data, &initial); err != nil {
			return false, err
		}
		if !initial.Superseded && initial.After != nil {
			return false, nil
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return false, err
	}
	if initial.Superseded {
		return true, nil
	}
	initial.Superseded = true
	return true, atomicStateJSON(path, initial)
}

func (b *syncLiveBackend) beginWindowRangeCount(ctx context.Context, target string, names []string, w syncRange, path string, imported bool, source string) (windowPointCount, error) {
	initial := windowPointCount{Source: source, Window: w}
	if _, err := os.Stat(path); err == nil {
		return readWindowPointCount(path, initial)
	} else if !errors.Is(err, os.ErrNotExist) {
		return initial, err
	}
	// An existing receipt without a range baseline was imported by an older
	// client. Its current count cannot establish how many points it added.
	if !imported {
		started := time.Now()
		d, err := b.Digest(ctx, target, names, w.From, w.Until)
		event("sync_window_digest", map[string]any{"stage": "before", "source": source, "names": len(names), "seconds": time.Since(started).Seconds()})
		if err == nil {
			n := countWindowPoints(d)
			initial.Before = &n
		}
		if ctx.Err() != nil {
			return initial, ctx.Err()
		}
	}
	return readWindowPointCount(path, initial)
}

func (b *syncLiveBackend) finishWindowRangeCount(ctx context.Context, target string, names []string, w syncRange, path string, measurement windowPointCount) error {
	started := time.Now()
	d, err := b.Digest(ctx, target, names, w.From, w.Until)
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if err != nil {
		// Point accounting remains explicitly unknown.
		return nil
	}
	n := countWindowPoints(d)
	measurement.After = &n
	event("sync_window_digest", map[string]any{"stage": "after", "source": measurement.Source, "names": len(names), "seconds": time.Since(started).Seconds()})
	return atomicStateJSON(path, measurement)
}
