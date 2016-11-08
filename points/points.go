package points

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/hydrogen18/stalecucumber"
)

// Point value/time pair
type Point struct {
	Value     float64
	Timestamp int64
}

// Points from carbon clients
type Points struct {
	Metric string
	Data   []Point
}

// New creates new instance of Points
func New() *Points {
	return &Points{}
}

// OnePoint create Points instance with single point
func OnePoint(metric string, value float64, timestamp int64) *Points {
	return &Points{
		Metric: metric,
		Data: []Point{
			Point{
				Value:     value,
				Timestamp: timestamp,
			},
		},
	}
}

// NowPoint create OnePoint with now timestamp
func NowPoint(metric string, value float64) *Points {
	return OnePoint(metric, value, time.Now().Unix())
}

// Copy returns copy of object
func (p *Points) Copy() *Points {
	return &Points{
		Metric: p.Metric,
		Data:   p.Data,
	}
}

func (p *Points) WriteTo(w io.Writer) (n int64, err error) {
	var c int
	for _, d := range p.Data { // every metric point
		c, err = w.Write([]byte(fmt.Sprintf("%s %v %v\n", p.Metric, d.Value, d.Timestamp)))
		n += int64(c)
		if err != nil {
			return
		}
	}
	return
}

// ParseText parse text protocol Point
//  host.Point.value 42 1422641531\n
func ParseText(line string) (*Points, error) {

	row := strings.Split(strings.Trim(line, "\n \t\r"), " ")
	if len(row) != 3 {
		return nil, fmt.Errorf("bad message: %#v", line)
	}

	// 0x2e == ".". Or use split? @TODO: benchmark
	// if strings.Contains(row[0], "..") || row[0][0] == 0x2e || row[0][len(row)-1] == 0x2e {
	// 	return nil, fmt.Errorf("bad message: %#v", line)
	// }

	value, err := strconv.ParseFloat(row[1], 64)

	if err != nil || math.IsNaN(value) {
		return nil, fmt.Errorf("bad message: %#v", line)
	}

	tsf, err := strconv.ParseFloat(row[2], 64)

	if err != nil || math.IsNaN(tsf) {
		return nil, fmt.Errorf("bad message: %#v", line)
	}

	// 315522000 == "1980-01-01 00:00:00"
	// if tsf < 315532800 {
	// 	return nil, fmt.Errorf("bad message: %#v", line)
	// }

	// 4102444800 = "2100-01-01 00:00:00"
	// Hello people from the future
	// if tsf > 4102444800 {
	// 	return nil, fmt.Errorf("bad message: %#v", line)
	// }

	return OnePoint(row[0], value, int64(tsf)), nil
}

// ParsePickle ...
func ParsePickle(pkt []byte) ([]*Points, error) {
	result, err := stalecucumber.Unpickle(bytes.NewReader(pkt))

	list, err := stalecucumber.ListOrTuple(result, err)
	if err != nil {
		return nil, err
	}

	msgs := []*Points{}
	for i := 0; i < len(list); i++ {
		metric, err := stalecucumber.ListOrTuple(list[i], nil)
		if err != nil {
			return nil, err
		}

		if len(metric) < 2 {
			return nil, errors.New("Unexpected array length while unpickling metric")
		}

		name, err := stalecucumber.String(metric[0], nil)
		if err != nil {
			return nil, err
		}

		msg := New()
		msg.Metric = name

		for j := 1; j < len(metric); j++ {
			v, err := stalecucumber.ListOrTuple(metric[j], nil)
			if err != nil {
				return nil, err
			}
			if len(v) != 2 {
				return nil, errors.New("Unexpected array length while unpickling data point")
			}
			timestamp, err := stalecucumber.Int(v[0], nil)
			if err != nil {
				timestampFloat, err := stalecucumber.Float(v[0], nil)
				if err != nil {
					return nil, err
				}
				timestamp = int64(timestampFloat)
			}
			if timestamp > math.MaxUint32 || timestamp < 0 {
				err = errors.New("Unexpected value for timestamp, cannot be cast to uint32")
				return nil, err
			}

			value, err := stalecucumber.Float(v[1], nil)
			if err != nil {
				valueInt, err := stalecucumber.Int(v[1], nil)
				if err != nil {
					return nil, err
				}
				value = float64(valueInt)
			}

			msg.Add(value, timestamp)
		}
		msgs = append(msgs, msg)
	}
	return msgs, nil
}

// Append point
func (p *Points) Append(onePoint Point) *Points {
	p.Data = append(p.Data, onePoint)
	return p
}

// Add value/timestamp pair to points
func (p *Points) Add(value float64, timestamp int64) *Points {
	p.Data = append(p.Data, Point{
		Value:     value,
		Timestamp: timestamp,
	})
	return p
}

// Eq points check
func (p *Points) Eq(other *Points) bool {
	if other == nil {
		return false
	}
	if p.Metric != other.Metric {
		return false
	}
	if p.Data == nil && other.Data == nil {
		return true
	}
	if (p.Data == nil || other.Data == nil) && (p.Data != nil || other.Data != nil) {
		return false
	}
	if len(p.Data) != len(other.Data) {
		return false
	}
	for i := 0; i < len(p.Data); i++ {
		if p.Data[i].Value != other.Data[i].Value {
			return false
		}
		if p.Data[i].Timestamp != other.Data[i].Timestamp {
			return false
		}
	}
	return true
}
