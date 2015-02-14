package points

import (
	"fmt"
	"strconv"
	"strings"
)

// Point value/time pair
type Point struct {
	Value     float64
	Timestamp int64
}

// Points from carbon clients
type Points struct {
	Metric string
	Data   []*Point
}

// New creates new instance of Points
func New() *Points {
	return &Points{}
}

// OnePoint create Points instance with single point
func OnePoint(metric string, value float64, timestamp int64) *Points {
	return &Points{
		Metric: metric,
		Data: []*Point{
			&Point{
				Value:     value,
				Timestamp: timestamp,
			},
		},
	}
}

// Copy returns copy of object
func (p *Points) Copy() *Points {
	return &Points{
		Metric: p.Metric,
		Data:   p.Data,
	}
}

// ParseText parse text protocol Point
//  host.Point.value 42 1422641531\n
func ParseText(line string) (*Points, error) {

	row := strings.Split(strings.Trim(line, "\n \t\r"), " ")
	if len(row) != 3 {
		return nil, fmt.Errorf("bad message: %#v", line)
	}

	if row[0] == "" {
		// @TODO: add more checks
		return nil, fmt.Errorf("bad message: %#v", line)
	}

	value, err := strconv.ParseFloat(row[1], 64)

	if err != nil {
		return nil, fmt.Errorf("bad message: %#v", line)
	}

	tsf, err := strconv.ParseFloat(row[2], 64)

	if err != nil {
		// @TODO: add more checks
		return nil, fmt.Errorf("bad message: %#v", line)
	}

	return OnePoint(row[0], value, int64(tsf)), nil
}

// Append point
func (p *Points) Append(onePoint *Point) *Points {
	p.Data = append(p.Data, onePoint)
	return p
}

// Add
func (p *Points) Add(value float64, timestamp int64) *Points {
	p.Data = append(p.Data, &Point{
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
