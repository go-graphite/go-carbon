package carbonserver

import (
	"math"
	"testing"

	protov2 "github.com/go-graphite/protocol/carbonapi_v2_pb"
	"google.golang.org/protobuf/encoding/protojson"
)

// The deployed encoder uses a space after commas. Upstream deliberately makes
// this vary with the executable hash, so rebuilding must not change this wire
// contract. Keep the legacy spelling of floats and escaped strings as well.
func TestJSONMatchesLegacyWireFormatting(t *testing.T) {
	t.Parallel()
	response := &protov2.MultiFetchResponse{Metrics: []*protov2.FetchResponse{{
		Name: "a,b\"c\\d", StartTime: 1, StopTime: 4, StepTime: 1,
		Values: []float64{0.1, math.Copysign(0, -1), 1e100}, IsAbsent: []bool{false, false, true},
	}}}
	got, err := protojson.Marshal(response)
	if err != nil {
		t.Fatal(err)
	}
	want := `{"metrics":[{"name":"a,b\"c\\d", "startTime":1, "stopTime":4, "stepTime":1, "values":[0.1, -0, 1e+100], "isAbsent":[false, false, true]}]}`
	if string(got) != want {
		t.Fatalf("JSON wire bytes changed\ngot:  %s\nwant: %s", got, want)
	}
}
