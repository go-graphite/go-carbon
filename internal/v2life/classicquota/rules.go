// Package classicquota translates the classic policy without coupling the
// lifecycle owner to carbonserver's serving implementation.
package classicquota

import (
	"errors"
	"math"
	"time"

	"github.com/go-graphite/go-carbon/carbonserver"
	"github.com/go-graphite/go-carbon/persister"
)

// Rules translates the existing storage-quotas.conf parser output
// with today's report interval and shared-storage physical-quota semantics.
// In particular the arithmetic intentionally matches getCarbonserverQuotas.
func Rules(rules persister.WhisperQuotas, frequency time.Duration, ignorePhysical bool) ([]*carbonserver.Quota, error) {
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
