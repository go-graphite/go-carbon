package carbon

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/go-graphite/go-carbon/cache"
	"github.com/go-graphite/go-carbon/internal/v2core"
	"github.com/go-graphite/go-carbon/points"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	v2Shadow      = "shadow"
	v2ServingDual = "v2-serving-with-classic-dual-write"
	v2Only        = "v2-only"
)

// Existing enabled=true configurations retain their v2-only behavior. New
// canaries should explicitly choose shadow, then serving-dual for the hold.
func v2StorageMode(c v2Config) (string, error) {
	switch c.Mode {
	case "", v2Only:
		return v2Only, nil
	case v2Shadow, v2ServingDual:
		return c.Mode, nil
	default:
		return "", fmt.Errorf("unknown v2.mode %q", c.Mode)
	}
}

type v2FallbackState struct {
	Version                        int
	ClassicPath, DataPath, Backend string
	Finalized                      bool
}

// This receipt binds the hold to one classic target. Finalization is sticky:
// after v2-only has started, changing a flag cannot declare stale classic data
// authoritative. It is deliberately a guard, not a cross-engine epoch system.
func (app *App) prepareV2Fallback(mode string) error {
	c := app.Config
	root := c.V2.Path
	classic, err := filepath.Abs(sharedStorePath(c))
	if err != nil {
		return err
	}
	data, err := filepath.Abs(c.Whisper.DataDir)
	if err != nil {
		return err
	}
	want := v2FallbackState{Version: 1, ClassicPath: classic, DataPath: data, Backend: c.Whisper.StorageBackend, Finalized: mode == v2Only}
	path := filepath.Join(root, "classic-fallback.json")
	b, err := os.ReadFile(path)
	switch {
	case err == nil:
		var got v2FallbackState
		if json.Unmarshal(b, &got) != nil || got.Version != 1 {
			return errors.New("invalid v2 classic fallback receipt")
		}
		if mode != v2Only && (got.Finalized || got.ClassicPath != classic || got.DataPath != data || got.Backend != want.Backend) {
			return errors.New("classic rollback unavailable: v2-only was started or classic target changed; restore and verify classic before a new canary")
		}
		if !want.Finalized || got.Finalized {
			return nil
		}
	case !os.IsNotExist(err):
		return err
	case mode != v2Only:
		entries, err := os.ReadDir(root)
		if err != nil && !os.IsNotExist(err) {
			return err
		}
		for _, entry := range entries {
			if entry.Name() != "restart.lock" && entry.Name() != "restart.json" {
				return errors.New("cannot enable dual write on untracked v2 history; begin with an empty v2 root")
			}
		}
	}
	if err := os.MkdirAll(root, 0700); err != nil {
		return err
	}
	b, err = json.Marshal(want)
	if err != nil {
		return err
	}
	f, err := os.CreateTemp(root, ".classic-fallback-*")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	if _, err = f.Write(b); err != nil {
		_ = f.Close()
		return err
	}
	if err = errors.Join(f.Sync(), f.Close()); err != nil {
		return err
	}
	if err = os.Rename(f.Name(), path); err != nil {
		return err
	}
	d, err := os.Open(root)
	if err != nil {
		return err
	}
	return errors.Join(d.Sync(), d.Close())
}

func (v *v2Runtime) mirrorClassic(core *cache.Cache) func(int, []v2core.Batch) error {
	return func(_ int, batches []v2core.Batch) error {
		for _, b := range batches {
			var policy v2core.RollupPolicy
			if err := json.Unmarshal(b.Policy, &policy); err != nil {
				return err
			}
			if len(policy.Name) == 0 {
				return errors.New("classic mirror requires named policy")
			}
			ps := &points.Points{Metric: string(policy.Name), Data: make([]points.Point, len(b.Points))}
			for i, p := range b.Points {
				ps.Data[i] = points.Point{Value: math.Float64frombits(p.Bits), Timestamp: p.Time}
			}
			// Accepted v2 input waits for classic capacity instead of a live
			// drop, and the classic input log records it like any input.
			for !core.HasRecoveryCapacityFor(ps.Metric, len(ps.Data)) {
				time.Sleep(time.Millisecond)
			}
			core.Add(ps)
			v.metrics.mirrored.Add(uint64(len(ps.Data)))
		}
		return nil
	}
}

type v2ModeMetrics struct {
	mode          string
	mirrored      atomic.Uint64
	rejectedNames atomic.Uint64
	unregister    func()
}

var v2ModeDesc = prometheus.NewDesc("go_carbon_storage_mode", "Active storage mode (one hot). Changes require restart.", []string{"mode"}, nil)
var v2MirroredDesc = prometheus.NewDesc("go_carbon_classic_mirrored_points_total", "Points v2 intake submitted to classic.", nil, nil)
var v2RejectedNamesDesc = prometheus.NewDesc("v2_name_rejected_points_total", "Points refused at v2 intake because their name has no valid policy (for example no schema or a classic policy mismatch). Over-long names are in receiver_name_too_long_rejected_points_total.", nil, nil)

func (m *v2ModeMetrics) Describe(ch chan<- *prometheus.Desc) {
	ch <- v2ModeDesc
	ch <- v2MirroredDesc
	ch <- v2RejectedNamesDesc
}
func (m *v2ModeMetrics) Collect(ch chan<- prometheus.Metric) {
	for _, mode := range []string{v2Shadow, v2ServingDual, v2Only} {
		value := 0.0
		if mode == m.mode {
			value = 1
		}
		ch <- prometheus.MustNewConstMetric(v2ModeDesc, prometheus.GaugeValue, value, mode)
	}
	ch <- prometheus.MustNewConstMetric(v2MirroredDesc, prometheus.CounterValue, float64(m.mirrored.Load()))
	ch <- prometheus.MustNewConstMetric(v2RejectedNamesDesc, prometheus.CounterValue, float64(m.rejectedNames.Load()))
}
