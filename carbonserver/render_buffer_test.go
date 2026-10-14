package carbonserver

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/cache"
	"github.com/go-graphite/go-whisper"
	"go.uber.org/zap"
)

// Drive the real producer with its original buffer and the bounded buffer. The
// serialized responses must agree even when the consumer starts after a send,
// the same target occurs in several ranges, or no response can be produced.
func TestRenderResponseBufferPreservesStream(t *testing.T) {
	root := t.TempDir()
	retentions, err := whisper.ParseRetentionDefs("1s:1h")
	if err != nil {
		t.Fatal(err)
	}
	w, err := whisper.Create(filepath.Join(root, "metric.wsp"), retentions, whisper.Last, 0)
	if err != nil {
		t.Fatal(err)
	}
	now := int(time.Now().Unix())
	if err := w.UpdateMany([]*whisper.TimeSeriesPoint{{Time: now - 5, Value: -0.0}, {Time: now - 3, Value: 42.5}}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	for i := range 1001 {
		if err := os.Link(filepath.Join(root, "metric.wsp"), filepath.Join(root, fmt.Sprintf("wide%d.wsp", i))); err != nil {
			t.Fatal(err)
		}
	}
	listener := NewCarbonserverListener(cache.New().Get)
	listener.logger, listener.accessLogger = zap.NewNop(), zap.NewNop()
	listener.SetWhisperData(root)
	listener.SetMaxMetricsRendered(2000)
	tr := timeRange{from: int32(now - 10), until: int32(now)}
	wide := globs{}
	for i := range 1001 {
		wide.Files = append(wide.Files, fmt.Sprintf("wide%d", i))
		wide.Leafs = append(wide.Leafs, true)
	}
	expanded := map[string]globs{
		"metric":  {Files: []string{"metric"}, Leafs: []bool{true}},
		"branch":  {Files: []string{"directory"}, Leafs: []bool{false}},
		"missing": {Files: []string{"missing"}, Leafs: []bool{true}},
		"wide.*":  wide,
	}
	for _, tc := range []struct {
		name    string
		targets map[timeRange][]target
		buffer  int
		count   int
		cancel  bool
	}{
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
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				if tc.cancel {
					cancel()
				}
				collect := func(buffer int) [][]byte {
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
							output = append(output, wire)
						case <-timeout.C:
							t.Fatal("render stream did not finish")
						}
					}
				}
				want, got := collect(1000), collect(capacity)
				if len(got) != tc.count || !slices.EqualFunc(got, want, bytes.Equal) {
					t.Fatalf("format %v: bounded buffer returned %d responses, original returned %d, expected %d; serialized responses differ", format, len(got), len(want), tc.count)
				}
			}
		})
	}
}
