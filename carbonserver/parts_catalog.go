package carbonserver

import (
	"errors"
	"math"
	"time"

	"github.com/dgryski/go-trigram"
	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/persister"
	"github.com/go-graphite/go-carbon/points"
	protov3 "github.com/go-graphite/protocol/carbonapi_v3_pb"
	"go.uber.org/zap"
)

// PartsCatalogIndex is an experimental catalog adapter using the SAME glob and
// quota implementations as the serving listener. Construct off the writer path
// from a pinned catalog generation. Find and Throttle never lock a parts owner.
// ReloadQuotas/RefreshUsage have one control-queue caller, just as today's index
// updater. This does not register a second server or change receiver wiring.
type PartsCatalogIndex struct {
	listener *CarbonserverListener
	opts     PartsIndexOptions
}

type PartsIndexOptions struct {
	Trie            bool
	Quotas          []*Quota // already translated, exactly as Config.getCarbonserverQuotas
	ReportFrequency time.Duration
	EstimateSize    func(string) (logical, physical, points int64)
}

func NewPartsCatalogIndex(metadata []store.Metadata, opts PartsIndexOptions) (*PartsCatalogIndex, error) {
	return newPartsCatalogIndex(metadata, opts, nil)
}

func newPartsCatalogIndex(metadata []store.Metadata, opts PartsIndexOptions, usage *throughputQuotaManager) (*PartsCatalogIndex, error) {
	if opts.ReportFrequency <= 0 {
		return nil, errors.New("positive quota report frequency required")
	}
	if len(opts.Quotas) > 0 && opts.EstimateSize == nil {
		return nil, errors.New("quota size estimator required")
	}
	estimate := opts.EstimateSize
	if estimate == nil {
		estimate = func(string) (int64, int64, int64) { return 0, 0, 0 }
	}
	l := &CarbonserverListener{logger: zap.NewNop(), accessLogger: zap.NewNop(), trieIndex: opts.Trie, trigramIndex: !opts.Trie, maxGlobs: math.MaxInt, maxMetricsGlobbed: math.MaxInt, failOnMaxGlobs: true, quotaUsageReportFrequency: opts.ReportFrequency}
	ti := newTrie(".wsp", 0, estimate)
	if usage != nil {
		ti.throughputs = usage
	}
	details := make(map[string]*protov3.MetricDetails)
	seen := make(map[string]struct{})
	var files []string
	// Always build quota trie; trigram remains an independent serving option.
	for _, m := range metadata {
		path := metricStoreVirtualPath(m.Name)
		logical, dp := metricStoreLogicalSize(m)
		if _, err := ti.insert(path, logical, 0, dp, 0); err != nil {
			return nil, err
		}
		if !opts.Trie {
			var err error
			files, err = l.addMetricStoreCatalogEntry(m, files, details, nil, seen)
			if err != nil {
				return nil, err
			}
		}
	}
	if _, err := ti.applyQuotas(opts.ReportFrequency, opts.Quotas...); err != nil {
		return nil, err
	}
	// The normal report refresh also rotates throughput windows. A catalog
	// publication must refresh storage counts without granting a new window.
	// Admission is serialized with this control operation by the caller.
	recorders := make(map[*throughputUsagePerNamespace]*throughputUsageDataPointsRecoder)
	ti.throughputs.entries.Range(func(_, value any) bool {
		q := value.(*throughputUsagePerNamespace)
		recorders[q] = q.dpRecorder()
		return true
	})
	ti.refreshUsage(ti.throughputs)
	for q, recorder := range recorders {
		q.dpRecorderv.Store(recorder)
	}
	fidx := &fileIndex{trieIdx: ti, files: files, details: details}
	if !opts.Trie {
		fidx.idx = trigram.NewIndex(files)
		fidx.idx.Prune(0.95)
	}
	l.UpdateFileIndex(fidx)
	return &PartsCatalogIndex{listener: l, opts: opts}, nil
}

// Next builds the next immutable find generation while retaining consumed
// throughput for unchanged namespaces. Call from the admission/control owner
// and publish it with the data/catalog generation. In-flight old find readers
// can finish using the previous object. Do not concurrently reload quotas.
func (p *PartsCatalogIndex) Next(metadata []store.Metadata) (*PartsCatalogIndex, error) {
	return newPartsCatalogIndex(metadata, p.opts, p.listener.CurrentFileIndex().trieIdx.throughputs)
}

// Find retains the current backend's behavior, including brace expansion and
// branch/leaf collisions. It does not claim that trie and trigram agree on
// every historically unsupported pattern. Results are names plus leaf flags.
func (p *PartsCatalogIndex) Find(query string) ([]string, []bool, error) {
	l := p.listener
	if l.trieIndex {
		files, leafs, _, _, err := l.expandGlobsTrie(query)
		return files, leafs, err
	}
	globs, err := l.globPatterns(query, l.logger)
	if err != nil {
		return nil, nil, err
	}
	files := l.matchGlobIndex(l.CurrentFileIndex(), globs, false)
	leafs := make([]bool, len(files))
	for i, path := range files {
		files[i], leafs[i] = l.normalizeGlobFile(path, nil)
	}
	return files, leafs, nil
}

func (p *PartsCatalogIndex) Throttle(ps *points.Points, inCache bool) bool {
	return p.listener.CurrentFileIndex().trieIdx.throttle(ps, inCache)
}

func (p *PartsCatalogIndex) ReloadQuotas(quotas []*Quota) error {
	_, err := p.listener.CurrentFileIndex().trieIdx.applyQuotas(p.listener.quotaUsageReportFrequency, quotas...)
	if err == nil {
		p.opts.Quotas = quotas
	}
	return err
}

// RefreshUsage advances throughput accounting using today's report-window
// reset behavior. Do not call it merely because a catalog snapshot is rebuilt.
func (p *PartsCatalogIndex) RefreshUsage() {
	ti := p.listener.CurrentFileIndex().trieIdx
	ti.refreshUsage(ti.throughputs)
}

// PartsQuotaRules translates the existing storage-quotas.conf parser output
// with today's report interval and shared-storage physical-quota semantics.
// In particular the arithmetic intentionally matches getCarbonserverQuotas.
func PartsQuotaRules(rules persister.WhisperQuotas, frequency time.Duration, ignorePhysical bool) ([]*Quota, error) {
	var out []*Quota
	for _, q := range rules {
		if q.PhysicalSize > 0 && q.PhysicalSize != math.MaxInt64 && !ignorePhysical {
			return nil, errors.New("physical-size quota unsupported for shared parts")
		}
		physical := q.PhysicalSize
		if ignorePhysical {
			physical = 0
		}
		out = append(out, &Quota{Pattern: q.Pattern, Namespaces: q.Namespaces, Metrics: q.Metrics, DataPoints: q.DataPoints, LogicalSize: q.LogicalSize, PhysicalSize: physical, Throughput: q.Throughput * int64(frequency/time.Minute), DroppingPolicy: ParseQuotaDroppingPolicy(q.DroppingPolicy), StatMetricPrefix: q.StatMetricPrefix})
	}
	return out, nil
}
