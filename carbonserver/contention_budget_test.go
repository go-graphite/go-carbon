package carbonserver

import (
	"bytes"
	"context"
	"os"
	"testing"

	"github.com/go-graphite/go-carbon/internal/contentiontest"
)

func TestCarbonserverContentionBudget(t *testing.T) {
	if os.Getenv("GO_CARBON_CONTENTION") != "1" {
		t.Skip("set GO_CARBON_CONTENTION=1")
	}
	l, names, _ := fetchWorkersFixture(t, 16)
	want, err := l.fetchSingleMetric(context.Background(), names[0], "alias", 1789999872, 1790000000)
	if err != nil {
		t.Fatal(err)
	}
	want.Name = "normalized"
	wire, err := want.proto2().MarshalVT()
	if err != nil {
		t.Fatal(err)
	}
	contentiontest.Run(t, 256, 32, 48, func(w, i int) {
		got, err := l.fetchSingleMetric(context.Background(), names[w%len(names)], "alias", 1789999872, 1790000000)
		if err != nil {
			t.Error(err)
			return
		}
		got.Name = "normalized"
		data, err := got.proto2().MarshalVT()
		if err != nil || !bytes.Equal(data, wire) {
			t.Error("concurrent render changed output", err)
		}
	})
}
