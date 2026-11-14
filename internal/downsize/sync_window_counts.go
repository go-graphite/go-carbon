package downsize

import (
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

// A baseline is durable before the native import starts. Retrying the same
// transfer must not replace it with the already repaired target's count.
type windowPointCount struct {
	Source string    `json:"source"`
	Window syncRange `json:"window"`
	Before *int64    `json:"before"`
	After  *int64    `json:"after"`
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
