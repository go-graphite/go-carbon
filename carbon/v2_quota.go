package carbon

import (
	"context"
	"errors"
	"math"
	"path/filepath"
	"strings"
	"time"

	"github.com/go-graphite/go-carbon/carbonserver"
	"github.com/go-graphite/go-carbon/internal/v2read"
	"github.com/go-graphite/go-carbon/persister"
	"github.com/go-graphite/go-whisper"
)

// v2QuotaRules translates the storage-quotas.conf parser output with today's
// report interval and shared-storage physical-quota semantics. The arithmetic
// intentionally matches getCarbonserverQuotas.
func v2QuotaRules(rules persister.WhisperQuotas, frequency time.Duration, ignorePhysical bool) ([]*carbonserver.Quota, error) {
	var out []*carbonserver.Quota
	for _, q := range rules {
		if q.PhysicalSize > 0 && q.PhysicalSize != math.MaxInt64 && !ignorePhysical {
			return nil, errors.New("physical-size quota unsupported for shared parts")
		}
		physical := q.PhysicalSize
		if ignorePhysical {
			physical = 0
		}
		out = append(out, &carbonserver.Quota{Pattern: q.Pattern, Namespaces: q.Namespaces, Metrics: q.Metrics, DataPoints: q.DataPoints, LogicalSize: q.LogicalSize, PhysicalSize: physical, Throughput: q.Throughput * int64(frequency/time.Minute), DroppingPolicy: carbonserver.ParseQuotaDroppingPolicy(q.DroppingPolicy), StatMetricPrefix: q.StatMetricPrefix})
	}
	return out, nil
}

func v2QuotaMetric(name string, archives int, count int64) carbonserver.V2QuotaMetric {
	return carbonserver.V2QuotaMetric{Name: name, DataPoints: count, LogicalSize: 16 + 12*int64(archives) + 12*count}
}

// newV2Quotas captures historical policy metadata once, before intake. The live
// refresh obtains v2 policies from immutable publications, never writer state.
func (app *App) newV2Quotas(classicNames []string) (*carbonserver.V2Quotas, map[string]carbonserver.V2QuotaMetric, error) {
	c := app.Config
	if len(c.Whisper.Quotas) == 0 {
		return nil, nil, nil
	}
	rules, err := v2QuotaRules(c.Whisper.Quotas, c.Carbonserver.QuotaUsageReportFrequency.Value(), c.Whisper.PebbleChunkIgnorePhysicalQuotas)
	if err != nil {
		return nil, nil, err
	}
	estimate := func(name string) (int64, int64, int64) {
		schema, ok := c.Whisper.Schemas.Match(name)
		if !ok {
			return 4096 + 172800*12, 0, 172800
		}
		var count int64
		for _, a := range schema.Retentions {
			count += int64(a.NumberOfPoints())
		}
		m := v2QuotaMetric(name, len(schema.Retentions), count)
		return m.LogicalSize, 0, m.DataPoints
	}
	q, err := carbonserver.NewV2Quotas(rules, c.Carbonserver.QuotaUsageReportFrequency.Value(), estimate)
	if err != nil {
		return nil, nil, err
	}
	historic := make(map[string]carbonserver.V2QuotaMetric, len(classicNames))
	for _, name := range classicNames {
		var count int64
		var archives int
		if app.MetricStore != nil {
			m, err := app.MetricStore.Metadata(context.Background(), name)
			if err == nil {
				for _, a := range m.Retentions {
					count += int64(a.Count)
				}
				archives = len(m.Retentions)
			} else if !c.Whisper.StoreIncrementalMigration {
				return nil, nil, err
			}
		}
		if archives == 0 {
			w, err := whisper.Open(filepath.Join(c.Whisper.DataDir, strings.ReplaceAll(name, ".", string(filepath.Separator))+".wsp"))
			if err != nil {
				return nil, nil, err
			}
			for _, a := range w.Retentions() {
				count += int64(a.NumberOfPoints())
			}
			archives = len(w.Retentions())
			if err := w.Close(); err != nil {
				return nil, nil, err
			}
		}
		historic[name] = v2QuotaMetric(name, archives, count)
	}
	return q, historic, nil
}

func publishV2QuotasContext(ctx context.Context, q *carbonserver.V2Quotas, fence uint64, r *v2read.Reader, names []string, historic map[string]carbonserver.V2QuotaMetric) error {
	if q == nil {
		return nil
	}
	metrics := make([]carbonserver.V2QuotaMetric, 0, len(names))
	for _, name := range names {
		if err := ctx.Err(); err != nil {
			return err
		}
		// Historical and v2 policies are checked equal at first overlap admission.
		if m, ok := historic[name]; ok {
			metrics = append(metrics, m)
			continue
		}
		m, err := r.Metadata(ctx, name)
		if errors.Is(err, v2read.ErrNotFound) {
			continue
		} // concurrent expiry; next refresh removes it
		if err != nil {
			return err
		}
		var count int64
		for _, a := range m.Archives {
			count += a.Points
		}
		metrics = append(metrics, v2QuotaMetric(name, len(m.Archives), count))
	}
	return q.Publish(metrics, fence)
}
