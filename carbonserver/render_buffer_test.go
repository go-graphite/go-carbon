package carbonserver

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/cache"
	"github.com/go-graphite/go-whisper"
	"go.uber.org/zap"
)

type renderBufferTestCase struct {
	name    string
	targets map[timeRange][]target
	buffer  int
	count   int
	cancel  bool
}

// Compare serialized output from the real producer with its original buffer
// and the bounded buffer, including requests larger than either queue.
func TestRenderResponseBufferPreservesStream(t *testing.T) {
	t.Parallel()
	listener, tr, expanded := renderBufferFixture(t)
	for _, tc := range []renderBufferTestCase{
		{name: "empty", buffer: 0},
		{name: "unknown", targets: map[timeRange][]target{tr: {{Name: "unknown"}}}, buffer: 0},
		{name: "single", targets: map[timeRange][]target{tr: {{Name: "metric"}}}, buffer: 1, count: 1},
		{name: "directory", targets: map[timeRange][]target{tr: {{Name: "branch"}}}, buffer: 1},
		{name: "failed-read", targets: map[timeRange][]target{tr: {{Name: "missing"}}}, buffer: 1},
		{name: "ranges-and-aliases", targets: map[timeRange][]target{
			tr: {{Name: "metric", PathExpression: "alias1"}, {Name: "metric", PathExpression: "alias2"}},
			{from: tr.from - 10, until: tr.until - 10}: {{Name: "metric", PathExpression: "older"}},
		}, buffer: 3, count: 3},
		{name: "wide", targets: map[timeRange][]target{tr: {{Name: "wide.*"}}}, buffer: 1000, count: 1001},
		{name: "cancelled", targets: map[timeRange][]target{tr: {{Name: "metric"}}}, buffer: 1, cancel: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			capacity := renderResponseBufferSize(tc.targets, expanded)
			if capacity != tc.buffer {
				t.Fatalf("buffer = %d, want %d", capacity, tc.buffer)
			}
			for _, format := range []responseFormat{jsonFormat, protoV2Format, protoV3Format, pickleFormat} {
				t.Run(fmt.Sprint(format), func(t *testing.T) {
					want := collectRenderBuffer(t, listener, format, tc, expanded, 1000)
					got := collectRenderBuffer(t, listener, format, tc, expanded, capacity)
					if len(got) != tc.count || !slices.EqualFunc(got, want, bytes.Equal) {
						t.Fatalf("bounded buffer returned %d responses, original returned %d, expected %d; serialized responses differ", len(got), len(want), tc.count)
					}
				})
			}
		})
	}
}

func renderBufferFixture(t *testing.T) (*CarbonserverListener, timeRange, map[string]globs) {
	t.Helper()
	root := t.TempDir()
	retentions, err := whisper.ParseRetentionDefs("1s:1h")
	if err != nil {
		t.Fatal(err)
	}
	w, err := whisper.Create(filepath.Join(root, "metric.wsp"), retentions, whisper.Last, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := w.Close(); err != nil {
			t.Error(err)
		}
	}()
	now := int(time.Now().Unix())
	if err := w.UpdateMany([]*whisper.TimeSeriesPoint{{Time: now - 5, Value: math.Copysign(0, -1)}, {Time: now - 3, Value: 42.5}}); err != nil {
		t.Fatal(err)
	}
	wide := globs{}
	for i := range 1001 {
		name := fmt.Sprintf("wide%d", i)
		if err := os.Link(filepath.Join(root, "metric.wsp"), filepath.Join(root, name+".wsp")); err != nil {
			t.Fatal(err)
		}
		wide.Files = append(wide.Files, name)
		wide.Leafs = append(wide.Leafs, true)
	}
	listener := NewCarbonserverListener(cache.New().Get)
	listener.logger, listener.accessLogger = zap.NewNop(), zap.NewNop()
	listener.SetWhisperData(root)
	listener.SetMaxMetricsRendered(2000)
	tr := timeRange{from: int32(now - 10), until: int32(now)}
	return listener, tr, map[string]globs{
		"metric":  {Files: []string{"metric"}, Leafs: []bool{true}},
		"branch":  {Files: []string{"directory"}, Leafs: []bool{false}},
		"missing": {Files: []string{"missing"}, Leafs: []bool{true}},
		"wide.*":  wide,
	}
}

func collectRenderBuffer(t *testing.T, listener *CarbonserverListener, format responseFormat, tc renderBufferTestCase, expanded map[string]globs, buffer int) [][]byte {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if tc.cancel {
		cancel()
	}
	ch := make(chan response, buffer)
	go listener.prepareDataStream(ctx, format, tc.targets, expanded, ch)
	var output [][]byte
	timeout := time.NewTimer(10 * time.Second)
	defer timeout.Stop()
	for {
		select {
		case r, ok := <-ch:
			if !ok {
				slices.SortFunc(output, bytes.Compare)
				return output
			}
			wire := renderBufferWire(t, r, format)
			output = append(output, wire)
		case <-timeout.C:
			t.Fatal("render stream did not finish")
		}
	}
}

func renderBufferWire(t *testing.T, r response, format responseFormat) []byte {
	t.Helper()
	var wire []byte
	var err error
	if format == protoV2Format || format == jsonFormat {
		wire, err = r.proto2().MarshalVT()
	} else {
		wire, err = r.proto3().MarshalVT()
	}
	if err != nil {
		t.Fatal(err)
	}
	return wire
}
