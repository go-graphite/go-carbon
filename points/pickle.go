package points

import (
	"bytes"
	"errors"
	"math"

	"github.com/lomik/og-rek"
)

func asList(value interface{}) ([]interface{}, bool) {
	if l, ok := value.([]interface{}); ok {
		return l, ok
	}

	if l, ok := value.(ogórek.Tuple); ok {
		return []interface{}(l), ok
	}

	// pp.Println("v", v)
	// pp.Println("Fail", value, v)

	return nil, false
}

func asInt64(value interface{}) (int64, bool) {
	switch value := value.(type) {
	case float32:
		return int64(value), true
	case float64:
		return int64(value), true
	case int:
		return int64(value), true
	case int16:
		return int64(value), true
	case int32:
		return int64(value), true
	case int64:
		return int64(value), true
	case int8:
		return int64(value), true
	case uint:
		return int64(value), true
	case uint16:
		return int64(value), true
	case uint32:
		return int64(value), true
	case uint64:
		return int64(value), true
	case uint8:
		return int64(value), true
	default:
		return 0, false
	}
	return 0, false
}

func asFloat64(value interface{}) (float64, bool) {
	switch value := value.(type) {
	case float32:
		return float64(value), true
	case float64:
		return float64(value), true
	case int:
		return float64(value), true
	case int16:
		return float64(value), true
	case int32:
		return float64(value), true
	case int64:
		return float64(value), true
	case int8:
		return float64(value), true
	case uint:
		return float64(value), true
	case uint16:
		return float64(value), true
	case uint32:
		return float64(value), true
	case uint64:
		return float64(value), true
	case uint8:
		return float64(value), true
	default:
		return 0, false
	}
	return 0, false
}

var BadPickleError = errors.New("bad pickle message")

// ParsePickle ...
func ParsePickle(pkt []byte) ([]*Points, error) {
	d := ogórek.NewDecoder(bytes.NewReader(pkt))

	v, err := d.Decode()
	if err != nil {
		return nil, err
	}

	series, ok := asList(v)
	if !ok {
		return nil, BadPickleError
	}

	msgs := []*Points{}
	for _, s := range series {
		metric, ok := asList(s)
		if !ok {
			return nil, BadPickleError
		}

		if len(metric) < 2 {
			return nil, BadPickleError
		}

		name, ok := metric[0].(string)
		if !ok {
			return nil, BadPickleError
		}

		msg := New()
		msg.Metric = name

		for _, p := range metric[1:] {
			point, ok := asList(p)
			if !ok {
				return nil, BadPickleError
			}

			if len(point) != 2 {
				return nil, BadPickleError
			}

			timestamp, ok := asInt64(point[0])
			if !ok {
				return nil, BadPickleError
			}

			if timestamp < 0 || timestamp > math.MaxUint32 {
				return nil, BadPickleError
			}

			value, ok := asFloat64(point[1])
			if !ok {
				return nil, BadPickleError
			}

			msg.Add(value, timestamp)
		}
		msgs = append(msgs, msg)
	}

	return msgs, nil
}
