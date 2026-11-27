package carbonserver

import (
	"math"
	"strconv"

	protov2 "github.com/go-graphite/protocol/carbonapi_v2_pb"
	"google.golang.org/protobuf/encoding/protojson"
)

// marshalRenderJSON keeps the protobuf JSON wire contract while traversing the
// two large numeric arrays directly. The protobuf encoder still owns the small
// header, including UTF-8 validation, escaping, field names and zero omission.
func marshalRenderJSON(response *protov2.MultiFetchResponse) ([]byte, error) {
	if response == nil || len(response.Metrics) == 0 {
		return []byte("{}"), nil
	}
	out := append([]byte(nil), `{"metrics":[`...)
	for i, metric := range response.Metrics {
		if i != 0 {
			out = append(out, ',', ' ')
		}
		if metric == nil {
			out = append(out, '{', '}')
			continue
		}
		header, err := protojson.Marshal(&protov2.FetchResponse{Name: metric.Name, StartTime: metric.StartTime, StopTime: metric.StopTime, StepTime: metric.StepTime})
		if err != nil {
			return nil, err
		}
		out = append(out, header[:len(header)-1]...)
		field := func(name string) {
			if out[len(out)-1] != '{' {
				out = append(out, ',', ' ')
			}
			out = append(out, '"')
			out = append(out, name...)
			out = append(out, '"', ':', '[')
		}
		if len(metric.Values) != 0 {
			field("values")
			for j, value := range metric.Values {
				if j != 0 {
					out = append(out, ',', ' ')
				}
				out = appendRenderFloat(out, value)
			}
			out = append(out, ']')
		}
		if len(metric.IsAbsent) != 0 {
			field("isAbsent")
			for j, value := range metric.IsAbsent {
				if j != 0 {
					out = append(out, ',', ' ')
				}
				out = strconv.AppendBool(out, value)
			}
			out = append(out, ']')
		}
		out = append(out, '}')
	}
	return append(out, ']', '}'), nil
}

func appendRenderFloat(out []byte, value float64) []byte {
	switch {
	case math.IsNaN(value):
		return append(out, `"NaN"`...)
	case math.IsInf(value, 1):
		return append(out, `"Infinity"`...)
	case math.IsInf(value, -1):
		return append(out, `"-Infinity"`...)
	}
	format := byte('f')
	if abs := math.Abs(value); abs != 0 && (abs < 1e-6 || abs >= 1e21) {
		format = 'e'
	}
	out = strconv.AppendFloat(out, value, format, -1, 64)
	if n := len(out); format == 'e' && n >= 4 && out[n-4] == 'e' && out[n-3] == '-' && out[n-2] == '0' {
		out[n-2] = out[n-1]
		out = out[:n-1]
	}
	return out
}
