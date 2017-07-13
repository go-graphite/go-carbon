package parse

import (
	"errors"

	"github.com/gogo/protobuf/proto"
	"github.com/lomik/go-carbon/helper/carbonpb"
	"github.com/lomik/go-carbon/points"
)

func Protobuf(body []byte) ([]*points.Points, error) {

	payload := &carbonpb.Payload{}
	err := proto.Unmarshal(body, payload)

	if err != nil {
		return []*points.Points{}, err
	}

	if payload.Metrics == nil {
		return []*points.Points{}, errors.New("empty message")
	}

	result := make([]*points.Points, len(payload.Metrics))

	for i := 0; i < len(payload.Metrics); i++ {
		m := payload.Metrics[i]

		if m == nil {
			return result, errors.New("metric is empty")
		}

		if len(m.Metric) == 0 {
			return result, errors.New("name is empty")
		}

		if len(m.Metric) > 16384 {
			return result, errors.New("name too long")
		}

		if m.Points == nil || len(m.Points) == 0 {
			return result, errors.New("points is empty")
		}

		r := points.OnePoint(m.Metric, m.Points[0].Value, int64(m.Points[0].Timestamp))
		for j := 1; j < len(m.Points); j++ {
			r = r.Add(m.Points[j].Value, int64(m.Points[j].Timestamp))
		}

		result[i] = r
	}

	return result, err
}
