package carbonserver

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"strings"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/v2read"
)

// SetV2UnionReader keeps the classic engine as immutable historical authority.
// The runtime owns its read-only handle and admits new writes only through v2.
func (l *CarbonserverListener) SetV2UnionReader(reader *v2read.Reader) {
	l.SetV2Reader(reader)
	l.v2Union = reader != nil
}

func classicMissing(err error) bool {
	return errors.Is(err, os.ErrNotExist) || errors.Is(err, store.ErrNotFound)
}

func (l *CarbonserverListener) fetchV2Union(ctx context.Context, name string, from, until int32) (*metricFromDisk, error) {
	started := time.Now()
	newer, masked, err := l.v2Reader.FetchWithMask(ctx, name, int(from), int(until))
	if err != nil && !errors.Is(err, v2read.ErrNotFound) {
		return nil, err
	}
	if !l.v2Union {
		if err != nil {
			return nil, err
		}
		if newer == nil {
			return nil, errors.New("time range not found")
		}
		return v2DiskResult(started, newer), nil
	}
	if masked {
		if newer == nil {
			return nil, v2read.ErrNotFound
		}
		return v2DiskResult(started, newer), nil
	}
	classic, classicErr := l.fetchClassicFromDisk(ctx, name, from, until)
	if classicErr != nil && !classicMissing(classicErr) {
		return nil, classicErr
	}
	if newer == nil {
		if classicErr != nil {
			return nil, classicErr
		}
		return classic, nil
	}
	if classicErr != nil {
		return v2DiskResult(started, newer), nil
	}
	prior := classic.Timeseries
	if prior.FromTime() != newer.FromTime() || prior.UntilTime() != newer.UntilTime() || prior.Step() != newer.Step() || len(prior.Values()) != len(newer.Values()) {
		return nil, errors.New("classic/v2 union archive shape mismatch")
	}
	info, err := l.getClassicMetricInfo(name)
	if err != nil {
		return nil, err
	}
	if err = validateUnionPolicy(info, newer.Metric); err != nil {
		return nil, err
	}
	values := prior.Values()
	for i, occupied := range newer.Occupied {
		if occupied {
			values[i] = newer.Values()[i]
		}
	}
	// The classic result owns its detached values. Its metadata is identical to
	// the validated v2 policy, and no live writer owns the classic source.
	classic.CacheData = nil
	return classic, nil
}

func v2DiskResult(started time.Time, s *v2read.FetchSeries) *metricFromDisk {
	return &metricFromDisk{DiskStartTime: started, Timeseries: s, Metadata: Metadata{ConsolidationFunc: titleizeAggrMethod(s.Metric.AggregationMethod), XFilesFactor: s.Metric.XFilesFactor}}
}

func validateUnionPolicy(classic metricInfo, newer v2read.Metric) error {
	if !strings.EqualFold(classic.aggregationMethod, newer.AggregationMethod) || math.Float32bits(classic.xFilesFactor) != math.Float32bits(newer.XFilesFactor) || len(classic.retentions) != len(newer.Archives) {
		return errors.New("classic/v2 union policy mismatch")
	}
	for i, a := range newer.Archives {
		if int64(classic.retentions[i].secondsPerPoint) != a.Step || int64(classic.retentions[i].numberOfPoints) != a.Points {
			return errors.New("classic/v2 union retention mismatch")
		}
	}
	return nil
}

// ValidateV2UnionPolicy is called once when admitting an existing classic name.
// Multiarchive admission requires an installed, immutable classic baseline.
func (l *CarbonserverListener) ValidateV2UnionPolicy(name string, newer v2read.Metric) error {
	classic, err := l.getClassicMetricInfo(name)
	if classicMissing(err) {
		return nil
	}
	if err != nil {
		return err
	}
	if err = validateUnionPolicy(classic, newer); err != nil {
		return err
	}
	if len(newer.Archives) > 1 {
		if l.v2Reader != nil {
			baseline, err := l.v2Reader.BaselineMetadata(context.Background(), name)
			if err == nil {
				return validateUnionPolicy(classic, baseline)
			}
			if !errors.Is(err, v2read.ErrNotFound) {
				return err
			}
		}
		return fmt.Errorf("classic/v2 cutover for %q requires seeded rollup archives", name)
	}
	return nil
}
