// Package chunkstore implements the experimental compressed archive-chunk store.
// Its persistence and policy types are independent of the Whisper file library.
package chunkstore

import (
	"strconv"

	"github.com/go-graphite/go-carbon/points"
)

type Point = points.Point

type Retention struct {
	Step  int
	Count int
}

func (r Retention) SecondsPerPoint() int { return r.Step }
func (r Retention) NumberOfPoints() int  { return r.Count }
func (r Retention) MaxRetention() int    { return r.Step * r.Count }

type AggregationMethod uint32

const (
	Average AggregationMethod = iota + 1
	Sum
	Last
	Max
	Min
	First
)

func (m AggregationMethod) String() string {
	switch m {
	case Average:
		return "average"
	case Sum:
		return "sum"
	case Last:
		return "last"
	case Max:
		return "max"
	case Min:
		return "min"
	case First:
		return "first"
	default:
		return strconv.FormatUint(uint64(m), 10)
	}
}

type MetricConfig struct {
	Name              string
	Retentions        []Retention
	AggregationMethod AggregationMethod
	XFilesFactor      float32
}

type Metadata struct {
	MetricConfig
	ID         uint64
	Generation uint64
	Revision   uint64 `json:"-"`
}

type Series struct {
	Metadata  Metadata
	FromTime  int
	UntilTime int
	Step      int
	Values    []float64
}
