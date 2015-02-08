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
func (points *Points) Append(p *Point) {
	points.Data = append(points.Data, p)
}
