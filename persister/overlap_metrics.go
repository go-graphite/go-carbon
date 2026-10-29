package persister

import (
	whisper "github.com/go-graphite/go-whisper"
	"github.com/prometheus/client_golang/prometheus"
)

// overlapUpdatesCollector exposes go-whisper's process-wide count of
// compressed updates that took the classic replay path, by the rule that sent
// them there, how the corrections that can avoid a whole-file rewrite reached
// storage, and the blocks rewrites read through a fallback.
type overlapUpdatesCollector struct {
	desc, rollups, sidecars, folds *prometheus.Desc
	late, latePoints, directFolds  *prometheus.Desc
	fallbacks                      *prometheus.Desc
}

func newOverlapUpdatesCollector() *overlapUpdatesCollector {
	return &overlapUpdatesCollector{
		desc: prometheus.NewDesc(
			"persister_overlap_updates_total",
			"Compressed updates rewritten through the classic replay path, by overlap kind",
			[]string{"kind"}, nil,
		),
		rollups: prometheus.NewDesc(
			"persister_rollup_corrections_total",
			"Buffered rollup corrections, patched in place or rewriting the whole file, by result",
			[]string{"result"}, nil,
		),
		sidecars: prometheus.NewDesc(
			"persister_sidecar_slot_merges_total",
			"Sidecar slot collisions whose sidecar was folded in place, or replayed with the batch as before, by result",
			[]string{"result"}, nil,
		),
		folds: prometheus.NewDesc(
			"persister_sidecar_folds_total",
			"Due out-of-order sidecars folded in place by maintenance, or left for the ordinary merge rules, by result",
			[]string{"result"}, nil,
		),
		late: prometheus.NewDesc(
			"persister_ooo_direct_writes_total",
			"Updates whose late points were written into their blocks, or diverted to the out-of-order sidecar and why, by result",
			[]string{"result"}, nil,
		),
		latePoints: prometheus.NewDesc(
			"persister_ooo_direct_points_total",
			"Late points written into their blocks, or diverted to the out-of-order sidecar and why, by result",
			[]string{"result"}, nil,
		),
		directFolds: prometheus.NewDesc(
			"persister_ooo_direct_sidecar_folds_total",
			"Existing out-of-order sidecars an update with direct late writes folded in place first, or could not, by result",
			[]string{"result"}, nil,
		),
		fallbacks: prometheus.NewDesc(
			"persister_rewrite_block_fallbacks_total",
			"Compressed blocks a rewrite read through a fallback: exact (typed storage served points the file could not encode) or aligned (points off the archive grid, stored on it), by kind",
			[]string{"kind"}, nil,
		),
	}
}

func (c *overlapUpdatesCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.desc
	ch <- c.rollups
	ch <- c.sidecars
	ch <- c.folds
	ch <- c.late
	ch <- c.latePoints
	ch <- c.directFolds
	ch <- c.fallbacks
}

func (c *overlapUpdatesCollector) Collect(ch chan<- prometheus.Metric) {
	for _, kind := range whisper.OverlapKinds() {
		ch <- prometheus.MustNewConstMetric(c.desc, prometheus.CounterValue, float64(whisper.OverlapUpdates(kind)), kind.String())
	}
	for _, result := range whisper.InPlaceResults() {
		ch <- prometheus.MustNewConstMetric(c.rollups, prometheus.CounterValue, float64(whisper.RollupCorrections(result)), result.String())
		ch <- prometheus.MustNewConstMetric(c.sidecars, prometheus.CounterValue, float64(whisper.SidecarSlotMerges(result)), result.String())
		ch <- prometheus.MustNewConstMetric(c.folds, prometheus.CounterValue, float64(whisper.SidecarFolds(result)), result.String())
		ch <- prometheus.MustNewConstMetric(c.directFolds, prometheus.CounterValue, float64(whisper.DirectSidecarFolds(result)), result.String())
	}
	for _, result := range whisper.DirectLateResults() {
		ch <- prometheus.MustNewConstMetric(c.late, prometheus.CounterValue, float64(whisper.DirectLateWrites(result)), result.String())
		ch <- prometheus.MustNewConstMetric(c.latePoints, prometheus.CounterValue, float64(whisper.DirectLatePoints(result)), result.String())
	}
	for _, kind := range whisper.RewriteBlockFallbacks() {
		ch <- prometheus.MustNewConstMetric(c.fallbacks, prometheus.CounterValue, float64(whisper.RewriteBlockFallbackCount(kind)), kind.String())
	}
}

// overlapUpdateCounts snapshots the process-wide counts, indexed by kind.
func overlapUpdateCounts() map[whisper.OverlapKind]uint64 {
	counts := make(map[whisper.OverlapKind]uint64)
	for _, kind := range whisper.OverlapKinds() {
		counts[kind] = whisper.OverlapUpdates(kind)
	}
	return counts
}

// sendOverlapUpdates reports the replays since the previous call, one stat
// per kind, as persister.overlapUpdates.<kind>.
func (p *Whisper) sendOverlapUpdates(send func(string, float64)) {
	p.overlapMu.Lock()
	defer p.overlapMu.Unlock()
	if p.overlapSeen == nil {
		p.overlapSeen = make(map[whisper.OverlapKind]uint64)
	}
	for kind, n := range overlapUpdateCounts() {
		send("overlapUpdates."+kind.String(), float64(n-p.overlapSeen[kind]))
		p.overlapSeen[kind] = n
	}
}
