package carbonserver

import (
	"cmp"
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"github.com/go-graphite/go-carbon/points"
	"github.com/go-graphite/go-whisper"
)

// https://github.com/golang/go/issues/448
func mod(a, b int) int {
	m := a % b
	if m < 0 {
		m += b
	}
	return m
}

// from go-graphite/go-whisper
func interval(time, secondsPerPoint int) int {
	return time - mod(time, secondsPerPoint) + secondsPerPoint
}

var errCacheTimeRangeNotFound = errors.New("time range not found")

// storageNow is the clock the stored read path uses to choose an archive:
// the shared store's when one serves reads, otherwise go-whisper's.
func (listener *CarbonserverListener) storageNow() time.Time {
	if db := listener.getMetricStore(); db != nil {
		return db.Now()
	}
	return whisper.Now()
}

// fetchFromCache answers a metric that has no stored series yet. The answer is
// what the series created from the cached points would return for the same
// request: the archive is chosen and the points are rolled up into it as
// go-whisper and the shared store do, so the step does not depend on whether
// the cache has been written out yet.
func (listener *CarbonserverListener) fetchFromCache(metric string, fromTime, untilTime int32, resp *response) error {
	path := listener.whisperData + "/" + strings.ReplaceAll(metric, ".", "/") + ".wsp"

	logger := listener.logger.With(
		zap.String("path", path),
		zap.Int("fromTime", int(fromTime)),
		zap.Int("untilTime", int(untilTime)),
	)

	// query cache
	cacheStartTime := time.Now()
	cacheData := listener.cacheGet(metric)
	waitTime := time.Since(cacheStartTime)
	atomic.AddUint64(&listener.metrics.CacheWaitTimeFetchNS, uint64(waitTime.Nanoseconds()))
	listener.prometheus.cacheDuration("wait", waitTime)

	if cacheData == nil {
		// we really can't find this metric
		atomic.AddUint64(&listener.metrics.NotFound, 1)
		listener.logger.Error("No data (wsp file nor cache) exists for the metric", zap.String("path", path))
		return errors.New("No data (wsp file nor cache) exists for the metric")
	}

	logger.Debug("fetching cache only metric")

	// retentions, aggMethod, xFilesFactor from matched schema/agg
	retentions, ok := listener.whisperGetConfig.MetricRetentions(metric)
	if !ok || len(retentions) == 0 {
		return errors.New("no retention schemas defined")
	}
	aggrMethod, aggrXFilesFact, ok := listener.whisperGetConfig.MetricAggrConf(metric)
	if !ok {
		return errors.New("no storage aggregation defined")
	}

	atomic.AddUint64(&listener.metrics.CacheRequestsTotal, 1)
	workStartTime := time.Now()
	series, err := cacheOnlySeries(cacheData, retentions, aggrMethod, float32(aggrXFilesFact),
		int(fromTime), int(untilTime), int(listener.storageNow().Unix()))
	if err != nil {
		if errors.Is(err, errCacheTimeRangeNotFound) {
			logger.Warn("metric time range not found")
		} else {
			logger.Warn("failed to fetch points", zap.Error(err))
		}
		return err
	}
	workTime := time.Since(workStartTime)
	atomic.AddUint64(&listener.metrics.CacheWorkTimeNS, uint64(workTime.Nanoseconds()))
	listener.prometheus.cacheDuration("work", workTime)
	listener.prometheus.cacheRequest("metric", series.cached > 0)
	if series.cached > 0 {
		atomic.AddUint64(&listener.metrics.CacheHit, 1)
	} else {
		atomic.AddUint64(&listener.metrics.CacheMiss, 1)
	}

	resp.StartTime = int64(series.from)
	resp.StopTime = int64(series.until)
	resp.StepTime = int64(series.step)
	resp.Values = series.values
	resp.ConsolidationFunc = titleizeAggrMethod(aggrMethod.String())
	resp.XFilesFactor = float32(aggrXFilesFact)

	atomic.AddUint64(&listener.metrics.MetricsReturned, 1)
	listener.prometheus.returnedMetric()

	atomic.AddUint64(&listener.metrics.PointsReturned, uint64(len(series.values)))
	listener.prometheus.returnedPoint(len(series.values))

	return nil
}

// cachedSeries is a series built from cached points alone.
type cachedSeries struct {
	from, until, step int
	values            []float64
	cached            int // cached points inside [from, until)
}

// archiveSlot is one occupied slot of an archive.
type archiveSlot struct {
	interval int
	value    float64
}

// cacheOnlySeries returns what a new series with the given retentions,
// aggregation method and xFilesFactor would answer for [fromTime, untilTime]
// at now after storing pts, as go-whisper (plain and compressed) and the
// shared store do:
//   - the range is clamped to [now-maxRetention, now];
//   - the archive is the first whose retention covers now-from;
//   - pts are written like UpdateMany: sorted by time (stable, so a later
//     duplicate wins), each to the finest archive whose retention still holds
//     it, the last point of a slot winning;
//   - every coarser archive holds the aggregate of the occupied slots of the
//     archive above it, in time order, when their share of the slots reaches
//     xFilesFactor.
func cacheOnlySeries(pts []points.Point, retentions whisper.Retentions, method whisper.AggregationMethod, xff float32, fromTime, untilTime, now int) (cachedSeries, error) {
	for i, r := range retentions {
		if r.SecondsPerPoint() <= 0 || r.NumberOfPoints() <= 0 {
			return cachedSeries{}, fmt.Errorf("invalid retention %d", i)
		}
	}
	if fromTime > untilTime {
		return cachedSeries{}, fmt.Errorf("invalid time interval: from time '%d' is after until time '%d'", fromTime, untilTime)
	}
	oldest := now - retentions[len(retentions)-1].MaxRetention()
	if fromTime > now || untilTime < oldest {
		return cachedSeries{}, errCacheTimeRangeNotFound
	}
	fromTime = max(fromTime, oldest)
	untilTime = min(untilTime, now)

	target := len(retentions) - 1
	for i, r := range retentions {
		if r.MaxRetention() >= now-fromTime {
			target = i
			break
		}
	}
	step := retentions[target].SecondsPerPoint()
	from, until := interval(fromTime, step), interval(untilTime, step)

	slots := archiveSlots(pts, retentions[:target+1], method, xff, now)
	if from == until && len(slots) > 0 {
		// Zero-length range on a populated archive: include the next point.
		until += step
	}
	series := cachedSeries{from: from, until: until, step: step, values: make([]float64, (until-from)/step)}
	for i := range series.values {
		series.values[i] = math.NaN()
	}
	for _, s := range slots {
		if s.interval >= from && s.interval < until {
			series.values[(s.interval-from)/step] = s.value
		}
	}
	for _, p := range pts {
		if ts := int(p.Timestamp); ts >= from && ts < until {
			series.cached++
		}
	}
	return series, nil
}

// archiveSlots returns the occupied slots of the last of retentions, ordered
// by time, after writing pts to a new series at now.
func archiveSlots(pts []points.Point, retentions whisper.Retentions, method whisper.AggregationMethod, xff float32, now int) []archiveSlot {
	sorted := slices.Clone(pts)
	slices.SortStableFunc(sorted, func(a, b points.Point) int { return cmp.Compare(a.Timestamp, b.Timestamp) })

	var slots []archiveSlot
	newer := len(sorted) // sorted[newer:] went to finer archives
	for i, r := range retentions {
		if i > 0 {
			slots = rollupSlots(slots, retentions[i-1].SecondsPerPoint(), r.SecondsPerPoint(), method, xff)
		}
		// Points too old for the finer archives are written to this one
		// directly and replace its rolled-up value.
		oldest := now - r.MaxRetention()
		first, _ := slices.BinarySearchFunc(sorted[:newer], oldest, func(p points.Point, t int) int { return cmp.Compare(int(p.Timestamp), t) })
		if first < newer {
			slots = overlaySlots(slots, alignSlots(sorted[first:newer], r.SecondsPerPoint()))
		}
		newer = first
	}
	return slots
}

// alignSlots aligns time-ordered points to an archive, the last point of a
// slot winning.
func alignSlots(pts []points.Point, step int) []archiveSlot {
	slots := make([]archiveSlot, 0, len(pts))
	for _, p := range pts {
		ts := int(p.Timestamp)
		s := archiveSlot{ts - mod(ts, step), p.Value}
		if n := len(slots); n > 0 && slots[n-1].interval == s.interval {
			slots[n-1] = s
			continue
		}
		slots = append(slots, s)
	}
	return slots
}

// overlaySlots merges two time-ordered slot lists; top wins on equal slots.
func overlaySlots(base, top []archiveSlot) []archiveSlot {
	if len(base) == 0 {
		return top
	}
	merged := make([]archiveSlot, 0, len(base)+len(top))
	i, j := 0, 0
	for i < len(base) && j < len(top) {
		switch {
		case base[i].interval < top[j].interval:
			merged = append(merged, base[i])
			i++
		case base[i].interval > top[j].interval:
			merged = append(merged, top[j])
			j++
		default:
			merged = append(merged, top[j])
			i++
			j++
		}
	}
	merged = append(merged, base[i:]...)
	return append(merged, top[j:]...)
}

// rollupSlots aggregates the time-ordered slots of a finer archive into a
// coarser one, keeping slots whose known share reaches xff.
func rollupSlots(finer []archiveSlot, finerStep, step int, method whisper.AggregationMethod, xff float32) []archiveSlot {
	need := step / finerStep
	if need <= 0 {
		return nil
	}
	var coarser []archiveSlot
	known := make([]float64, 0, need)
	for i := 0; i < len(finer); {
		start := finer[i].interval - mod(finer[i].interval, step)
		known = known[:0]
		for ; i < len(finer) && finer[i].interval-mod(finer[i].interval, step) == start; i++ {
			known = append(known, finer[i].value)
		}
		if float32(len(known))/float32(need) < xff {
			continue
		}
		coarser = append(coarser, archiveSlot{start, aggregateSlots(method, known)})
	}
	return coarser
}

// aggregateSlots matches go-whisper's aggregate, including NaN handling and
// summation order. storage-aggregation.conf allows only these methods.
func aggregateSlots(method whisper.AggregationMethod, known []float64) float64 {
	switch method {
	case whisper.Sum:
		return sumSlots(known)
	case whisper.First:
		return known[0]
	case whisper.Last:
		return known[len(known)-1]
	case whisper.Max:
		x := known[0]
		for _, v := range known {
			if v > x {
				x = v
			}
		}
		return x
	case whisper.Min:
		x := known[0]
		for _, v := range known {
			if v < x {
				x = v
			}
		}
		return x
	default:
		return sumSlots(known) / float64(len(known))
	}
}

func sumSlots(values []float64) float64 {
	sum := 0.0
	for _, v := range values {
		sum += v
	}
	return sum
}
