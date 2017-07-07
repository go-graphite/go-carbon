package parse

import (
	"github.com/lomik/go-carbon/points"
	pickle "github.com/lomik/graphite-pickle"
)

func Pickle(body []byte) ([]*points.Points, error) {
	result := []*points.Points{}

	err := pickle.ParseMessage(body, func(name string, value float64, timestamp int64) {
		if len(result) == 0 || result[len(result)-1].Metric != name {
			result = append(result, points.OnePoint(name, value, timestamp))
		} else {
			result[len(result)-1].Add(value, timestamp)
		}
	})

	return result, err
}
