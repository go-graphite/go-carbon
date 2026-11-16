package v2core

import (
	"errors"
	"os"
	"path/filepath"
	"slices"

	"github.com/cockroachdb/pebble"
)

// SettlementProbe reads each open DB's persisted options once. It observes
// Pebble's automatic-work thresholds without requesting any compaction.
type SettlementProbe struct {
	engine *Engine
	l0     []int
	target uint64
}

func NewSettlementProbe(e *Engine) (*SettlementProbe, error) {
	p := &SettlementProbe{engine: e}
	for _, o := range e.owners {
		paths, err := filepath.Glob(filepath.Join(o.dir, "OPTIONS-*"))
		if err != nil {
			return nil, err
		}
		if len(paths) == 0 {
			return nil, errors.New("missing persisted Pebble options")
		}
		slices.Sort(paths)
		b, err := os.ReadFile(paths[len(paths)-1])
		if err != nil {
			return nil, err
		}
		var opts pebble.Options
		if err = opts.Parse(string(b), nil); err != nil {
			return nil, err
		}
		if opts.L0CompactionThreshold <= 0 || len(opts.Levels) == 0 {
			return nil, errors.New("missing Pebble settlement thresholds")
		}
		p.l0 = append(p.l0, opts.L0CompactionThreshold)
		for _, l := range opts.Levels {
			if l.TargetFileSize <= 0 {
				return nil, errors.New("invalid Pebble target file size")
			}
			if p.target == 0 || uint64(l.TargetFileSize) < p.target {
				p.target = uint64(l.TargetFileSize)
			}
		}
	}
	return p, nil
}

func (p *SettlementProbe) Sample() map[string]any {
	var debt uint64
	var active, flushes, maxL0 int64
	below := true
	for i, o := range p.engine.owners {
		m := o.db.Metrics()
		debt += m.Compact.EstimatedDebt
		active += m.Compact.NumInProgress
		flushes += m.Flush.NumInProgress
		maxL0 = max(maxL0, int64(m.Levels[0].Sublevels))
		below = below && int(m.Levels[0].Sublevels) < p.l0[i]
	}
	return map[string]any{"estimated_debt_bytes": debt, "compactions_in_progress": active, "flushes_in_progress": flushes, "max_l0_sublevels": maxL0, "l0_compaction_thresholds": slices.Clone(p.l0), "l0_below_compaction_threshold": below, "settlement_target_file_bytes": p.target}
}
