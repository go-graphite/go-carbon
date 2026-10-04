package chunkstore

import (
	"errors"
	"fmt"
	"math"
)

func validate(c MetricConfig) error {
	if c.Name == "" {
		return errors.New("metric name is empty")
	}
	if len(c.Retentions) == 0 {
		return errors.New("metric has no retentions")
	}
	for i, r := range c.Retentions {
		if r.SecondsPerPoint() <= 0 || r.NumberOfPoints() <= 0 || r.NumberOfPoints() > math.MaxInt/r.SecondsPerPoint() {
			return fmt.Errorf("invalid retention %d", i)
		}
		// Buckyd's classic Whisper header stores maximum retention as uint32.
		// This also keeps chunk indexes and query end keys representable.
		if uint64(r.MaxRetention()) > math.MaxUint32 {
			return fmt.Errorf("retention %d exceeds the transfer format", i)
		}
		if i > 0 && r.SecondsPerPoint() <= c.Retentions[i-1].SecondsPerPoint() {
			return errors.New("retentions must be increasing")
		}
		if i > 0 {
			higher := c.Retentions[i-1]
			if r.SecondsPerPoint()%higher.SecondsPerPoint() != 0 {
				return errors.New("higher precision must evenly divide lower precision")
			}
			if higher.MaxRetention() >= r.MaxRetention() {
				return errors.New("lower precision must cover a larger interval")
			}
			if higher.NumberOfPoints() < r.SecondsPerPoint()/higher.SecondsPerPoint() {
				return errors.New("higher precision has too few points to consolidate")
			}
		}
	}
	if c.AggregationMethod < Average || c.AggregationMethod > First {
		return errors.New("unsupported aggregation method")
	}
	if math.IsNaN(float64(c.XFilesFactor)) || c.XFilesFactor < 0 || c.XFilesFactor > 1 {
		return errors.New("invalid xFilesFactor")
	}
	return nil
}

func cloneConfig(c MetricConfig) MetricConfig {
	c.Retentions = append([]Retention(nil), c.Retentions...)
	return c
}
func maxRetention(rs []Retention) int { return rs[len(rs)-1].MaxRetention() }
func align(t, step int) int           { return t - t%step }
func interval(t, step int) int        { return align(t, step) + step }
func targetArchive(rs []Retention, diff, target int) int {
	for i, r := range rs {
		if target >= 0 && r.MaxRetention() != target {
			continue
		}
		if target >= 0 || diff <= r.MaxRetention() {
			return i
		}
	}
	return -1
}
func aggregate(method AggregationMethod, values []float64) float64 {
	switch method {
	case Average:
		var x float64
		for _, v := range values {
			x += v
		}
		return x / float64(len(values))
	case Sum:
		var x float64
		for _, v := range values {
			x += v
		}
		return x
	case First:
		return values[0]
	case Last:
		return values[len(values)-1]
	case Max:
		x := values[0]
		for _, v := range values[1:] {
			if v > x {
				x = v
			}
		}
		return x
	case Min:
		x := values[0]
		for _, v := range values[1:] {
			if v < x {
				x = v
			}
		}
		return x
	}
	panic("unsupported aggregation")
}
