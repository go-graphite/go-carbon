package v2read

import (
	"context"
	"errors"
	"testing"
)

func TestReadObservationIncludesFailedFetch(t *testing.T) {
	r, err := New(1, 1<<20, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := r.FetchMany(ctx, []string{"missing"}, 0, 60, 60, nil); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	count, sum, buckets := r.ReadObservation()
	if count != 1 || sum < 0 || len(buckets) != 9 {
		t.Fatalf("count=%d sum=%f buckets=%v", count, sum, buckets)
	}
	var previous uint64
	for _, bound := range ReadBounds() {
		if buckets[bound] < previous || buckets[bound] > count {
			t.Fatalf("noncumulative histogram: %v", buckets)
		}
		previous = buckets[bound]
	}
}
