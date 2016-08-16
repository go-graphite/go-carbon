package cache

import "github.com/lomik/go-carbon/points"

// Query request from carbonlink
type Query struct {
	Metric    string
	Wait      chan bool      // close after finish collect reply
	CacheData *points.Points // from cache
}

// NewQuery create Query instance
func NewQuery(metric string) *Query {
	return &Query{
		Metric: metric,
		Wait:   make(chan bool),
	}
}
