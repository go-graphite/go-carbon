package parse

import (
	"errors"

	"github.com/go-graphite/go-carbon/points"
	"github.com/vmihailenco/msgpack/v5"
)

// Datapoint loads a msgpack data
type Datapoint struct {
	Name  string  `json:"Name"`
	Value float64 `json:"Value"`
	Time  int64   `json:"Time"`
}

// Msgpack is used to unpack metrics produced by
// carbon-relay-ng
func Msgpack(body []byte) ([]*points.Points, error) {
	result := make([]*points.Points, 0)

	var d Datapoint
	err := msgpack.Unmarshal(body, &d)
	if err != nil {
		return result, err
	}

	if d.Name == "" {
		err = errors.New("Empty metric name")
		return result, err
	}

	result = append(result, points.OnePoint(d.Name, d.Value, d.Time))
	return result, err
}
