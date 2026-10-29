package persister

import (
	"context"
	"errors"
	"reflect"
	"testing"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/points"
)

type transferBatchStore struct{ failedSharedStore }

func (s transferBatchStore) UpdateBatch(context.Context, []store.MetricUpdate, *store.ContinuationMaintenance) error {
	*s.calls = append(*s.calls, "batch")
	return s.err
}
func TestTransferFlushConfirmsOnlyCommittedBatch(t *testing.T) {
	for _, fail := range []bool{false, true} {
		var calls []string
		batch := &points.Points{Metric: "pending", Data: []points.Point{{Timestamp: 100, Value: 1}}}
		p := NewWhisper(t.TempDir(), nil, NewWhisperAggregation(), nil, func(string) (*points.Points, bool) { return batch, true }, func(*points.Points) { calls = append(calls, "confirm") }, nil)
		p.SetRequeue(func(*points.Points) { calls = append(calls, "requeue") })
		var err error
		if fail {
			err = errors.New("commit failed")
		}
		p.SetMetricStore(transferBatchStore{failedSharedStore{&calls, err}})
		p.SetWriteoutReady(func(string) bool { return false })
		got := p.FlushForTransfer(context.Background(), []string{"pending"})
		want := []string{"batch", "confirm"}
		if fail {
			want = []string{"batch", "requeue"}
		}
		if !reflect.DeepEqual(calls, want) || (got != nil) != fail {
			t.Fatal(calls, got)
		}
	}
}
