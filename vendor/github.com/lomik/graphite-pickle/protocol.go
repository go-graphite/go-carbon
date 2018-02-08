package pickle

import (
	"bytes"
	"errors"
	"math"

	"github.com/lomik/og-rek"
)

var BadPickleError = errors.New("bad pickle message")

// ParsePickle ...
func ParseMessage(pkt []byte, callback func(string, float64, int64)) error {
	d := ogórek.NewDecoder(bytes.NewReader(pkt))

	v, err := d.Decode()
	if err != nil {
		return err
	}

	series, ok := ToList(v)
	if !ok {
		return BadPickleError
	}

	for _, s := range series {
		metric, ok := ToList(s)
		if !ok {
			return BadPickleError
		}

		if len(metric) < 2 {
			return BadPickleError
		}

		name, ok := metric[0].(string)
		if !ok {
			return BadPickleError
		}

		for _, p := range metric[1:] {
			point, ok := ToList(p)
			if !ok {
				return BadPickleError
			}

			if len(point) != 2 {
				return BadPickleError
			}

			timestamp, ok := ToInt64(point[0])
			if !ok {
				return BadPickleError
			}

			if timestamp < 0 || timestamp > math.MaxUint32 {
				return BadPickleError
			}

			value, ok := ToFloat64(point[1])
			if !ok {
				return BadPickleError
			}

			if math.IsNaN(value) || math.IsInf(value, 0) {
				// silent skip NaN
				continue
			}

			callback(name, value, timestamp)
		}
	}

	return nil
}

// Message has a metric name and multiple data points.
type Message struct {
	Name   string
	Points []DataPoint
}

// DataPoint represents a data point which has a timestamp and a value.
type DataPoint struct {
	Timestamp int64
	Value     float64
}

// MarshalMessages marshals multiple messages to bytes.
func MarshalMessages(msgs []Message) ([]byte, error) {
	series := make([]interface{}, len(msgs))
	for i, msg := range msgs {
		pts := make([]interface{}, 1+len(msg.Points))
		pts[0] = msg.Name
		for j, p := range msg.Points {
			pts[j+1] = []interface{}{p.Timestamp, p.Value}
		}
		series[i] = pts
	}

	var buf bytes.Buffer
	enc := ogórek.NewEncoder(&buf)
	err := enc.Encode(series)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
