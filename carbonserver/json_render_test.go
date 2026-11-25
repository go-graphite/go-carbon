package carbonserver

import (
	"bytes"
	"math"
	"math/rand/v2"
	"testing"

	protov2 "github.com/go-graphite/protocol/carbonapi_v2_pb"
	"google.golang.org/protobuf/encoding/protojson"
)

func TestRenderJSONMatchesProtobufOracle(t *testing.T) {
	t.Parallel()
	values := []float64{0, math.Copysign(0, -1), math.NaN(), math.Inf(1), math.Inf(-1), 1e-7, 1e-6, 1e20, 1e21, math.SmallestNonzeroFloat64, math.MaxFloat64}
	rng := rand.New(rand.NewPCG(51, 19))
	for range 32768 {
		values = append(values, math.Float64frombits(rng.Uint64()))
	}
	tests := []*protov2.MultiFetchResponse{
		nil, {}, {Metrics: []*protov2.FetchResponse{nil, {}}},
		{Metrics: []*protov2.FetchResponse{{Name: "a,b\"c\\d\n\x00\u2028\u2029<>&é", StartTime: math.MinInt32, StopTime: math.MaxInt32, StepTime: -1, Values: values, IsAbsent: []bool{true, false, true}}}},
		{Metrics: []*protov2.FetchResponse{{Values: []float64{1}}, {IsAbsent: []bool{false}}, {Name: "bad\xff"}}},
	}
	for i, input := range tests {
		want, werr := protojson.Marshal(input)
		got, gerr := marshalRenderJSON(input)
		if (werr == nil) != (gerr == nil) {
			t.Fatalf("case %d: error mismatch: got %v, want %v", i, gerr, werr)
		}
		if werr == nil && !bytes.Equal(got, want) {
			t.Fatalf("case %d: JSON wire differs: got length %d, want %d", i, len(got), len(want))
		}
	}
}
