package main

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"
)

// The plan and operations are private artifacts. Operations carry actual
// per-metric write batches and read windows, rather than inventing values or
// turning a whole ingress trace into one large write per metric.
type replayPlan struct {
	Version                       int
	SourceRoot                    string
	Sampling                      string
	PopulationBytes               int64
	StartNS, WarmupUntilNS, EndNS int64
	CacheBytes                    int64
	MemtableBytes                 uint64
	Rows                          []replayRow
}

type replayRow struct {
	Metric, Path          string
	AllocatedBytes        int64
	InclusionProbability  float64
	CapturedAtNS          int64
	MainSHA256, OOOSHA256 string
}

type replayPoint struct {
	Timestamp int64
	Bits      uint64
}
type replayOperation struct {
	AtNS        int64
	Kind        string
	Metric      int
	Points      []replayPoint
	From, Until int
}

func loadPlan(path string) (replayPlan, error) {
	f, err := os.Open(path)
	if err != nil {
		return replayPlan{}, err
	}
	defer f.Close()
	var p replayPlan
	d := json.NewDecoder(io.LimitReader(f, 64<<20))
	d.DisallowUnknownFields()
	if err := d.Decode(&p); err != nil {
		return p, err
	}
	if err := d.Decode(new(any)); !errors.Is(err, io.EOF) {
		return p, errors.New("trailing plan data")
	}
	if p.Version != 1 || p.StartNS <= 0 || p.WarmupUntilNS < p.StartNS || p.EndNS <= p.WarmupUntilNS || len(p.Rows) == 0 || len(p.Rows) > 100000 {
		return p, errors.New("invalid replay interval or corpus")
	}
	if !filepath.IsAbs(p.SourceRoot) || p.Sampling == "" || p.PopulationBytes <= 0 || p.CacheBytes <= 0 || p.MemtableBytes < 64<<10 {
		return p, errors.New("explicit source, sampling scope and engine budgets are required")
	}
	seen := make(map[string]bool)
	var selectedBytes int64
	for _, row := range p.Rows {
		if row.Metric == "" || strings.ContainsAny(row.Metric, "/\\\x00") || seen[row.Metric] || !filepath.IsLocal(row.Path) || filepath.Clean(row.Path) != row.Path || row.Path != strings.ReplaceAll(row.Metric, ".", "/")+".wsp" || row.AllocatedBytes <= 0 || math.IsNaN(row.InclusionProbability) || row.InclusionProbability <= 0 || row.InclusionProbability > 1 {
			return p, errors.New("invalid, repeated or unweighted corpus row")
		}
		seen[row.Metric] = true
		if row.CapturedAtNS <= 0 || row.CapturedAtNS > p.StartNS || len(row.MainSHA256) != 64 || row.OOOSHA256 != "absent" && len(row.OOOSHA256) != 64 {
			return p, errors.New("a hashed capture completed before replay start is required for every row")
		}
		if row.AllocatedBytes > p.PopulationBytes-selectedBytes {
			return p, errors.New("selected allocation exceeds the declared population")
		}
		selectedBytes += row.AllocatedBytes
	}
	return p, nil
}

func eachOperation(path string, plan replayPlan, visit func(int, replayOperation) error) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 64<<10), 4<<20)
	previous, ordinal := plan.StartNS, 0
	for scanner.Scan() {
		if ordinal >= 10000000 {
			return errors.New("operation count exceeds bound")
		}
		var op replayOperation
		d := json.NewDecoder(strings.NewReader(scanner.Text()))
		d.DisallowUnknownFields()
		if err := d.Decode(&op); err != nil {
			return fmt.Errorf("operation %d: %w", ordinal, err)
		}
		if err := d.Decode(new(any)); !errors.Is(err, io.EOF) {
			return errors.New("trailing operation data")
		}
		if op.AtNS < previous || op.AtNS > plan.EndNS || op.Metric < 0 || op.Metric >= len(plan.Rows) {
			return fmt.Errorf("operation %d is out of order or outside the corpus", ordinal)
		}
		previous = op.AtNS
		switch op.Kind {
		case "write":
			if len(op.Points) == 0 || len(op.Points) > 65536 {
				return errors.New("invalid captured write batch")
			}
			for _, p := range op.Points {
				if p.Timestamp <= 0 || p.Timestamp > math.MaxUint32 {
					return errors.New("invalid write timestamp")
				}
			}
		case "read":
			if len(op.Points) != 0 || op.From > op.Until {
				return errors.New("invalid captured read")
			}
		default:
			return errors.New("operation must be read or write")
		}
		if err := visit(ordinal, op); err != nil {
			return err
		}
		ordinal++
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	if ordinal == 0 {
		return errors.New("empty operation trace")
	}
	return nil
}

func fileSHA(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func writeJSON(path string, value any) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	return errors.Join(json.NewEncoder(f).Encode(value), f.Sync(), f.Close())
}
