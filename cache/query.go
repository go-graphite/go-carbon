package cache

import "github.com/lomik/go-carbon/points"

// Query request from carbonlink
type Query struct {
	Metric    string
	ReplyChan chan *Reply
}

// NewQuery create Query instance
func NewQuery(metric string) *Query {
	return &Query{
		Metric:    metric,
		ReplyChan: make(chan *Reply, 1),
	}
}

// Reply response to carbonlink
type Reply struct {
	Points *points.Points
}

// NewReply create Reply instance
func NewReply() *Reply {
	return &Reply{}
}
