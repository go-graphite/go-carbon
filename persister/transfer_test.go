package persister

import (
	"context"
	"errors"
	"fmt"
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

// Dispatch to the native backend can commit a prefix. Recover only its suffix,
// and account the real fallback writes rather than ErrFormat itself.
type transferFallbackStore struct {
	transferBatchStore
	failAt, callsMade int
	failure           error
}

func (s *transferFallbackStore) UpdateMany(context.Context, string, []points.Point) error {
	i := s.callsMade
	s.callsMade++
	if i == s.failAt {
		return s.failure
	}
	return nil
}
func TestTransferFlushFallbackOwnershipAndAccounting(t *testing.T) {
	for _, failure := range []error{context.Canceled, context.DeadlineExceeded, errors.New("disk failure"), errors.Join(context.Canceled, errors.New("disk failure"))} {
		for failAt := 0; failAt <= 8; failAt++ {
			t.Run(fmt.Sprintf("%v/%d", failure, failAt), func(t *testing.T) {
				var calls []string
				wrapped := &transferFallbackStore{transferBatchStore: transferBatchStore{failedSharedStore{&calls, store.ErrFormat}}, failAt: failAt, failure: failure}
				names := []string{"a", "b", "c", "d", "e", "f", "g", "h"}
				confirmed, requeued := map[string]int{}, map[string]int{}
				p := NewWhisper(t.TempDir(), nil, NewWhisperAggregation(), nil, func(name string) (*points.Points, bool) { return points.OnePoint(name, 1, 100), true }, func(v *points.Points) { confirmed[v.Metric]++ }, nil)
				p.SetRequeue(func(v *points.Points) { requeued[v.Metric]++ })
				p.SetMetricStore(wrapped)
				err := p.FlushForTransfer(context.Background(), names)
				if (err != nil) != (failAt < 8) {
					t.Fatal(err)
				}
				for i, name := range names {
					if i < failAt {
						if confirmed[name] != 1 || requeued[name] != 0 {
							t.Fatal(name, confirmed, requeued)
						}
					} else if confirmed[name] != 0 || requeued[name] != 1 {
						t.Fatal(name, confirmed, requeued)
					}
				}
				st, _ := p.WriteStats()
				wantFailures := uint64(0)
				if failAt < 8 && transferFlushReason(failure) == 3 {
					wantFailures = 1
				}
				if st.Updates != uint64(failAt) || st.Failures != wantFailures {
					t.Fatal(st)
				}
				var retries uint64
				for _, n := range p.TransferFlushRequeues() {
					retries += n
				}
				if (retries == 1) != (failAt < 8) {
					t.Fatal(p.TransferFlushRequeues())
				}
			})
		}
	}
}

func TestTransferFlushStorageErrorsRemainFailures(t *testing.T) {
	for _, failure := range []error{errors.New("disk I/O"), fmt.Errorf("prepare: %w", errors.Join(context.Canceled, errors.New("disk I/O")))} {
		var calls []string
		confirmed, requeued := 0, 0
		p := NewWhisper(t.TempDir(), nil, nil, nil, func(n string) (*points.Points, bool) { return points.OnePoint(n, 1, 1), true }, func(*points.Points) { confirmed++ }, nil)
		p.SetRequeue(func(*points.Points) { requeued++ })
		p.SetMetricStore(transferBatchStore{failedSharedStore{&calls, failure}})
		if err := p.FlushForTransfer(context.Background(), []string{"a", "b", "c", "d", "e", "f", "g", "h"}); !errors.Is(err, failure) {
			t.Fatal(err)
		}
		st, _ := p.WriteStats()
		if st.Failures != 8 || confirmed != 0 || requeued != 8 || p.TransferFlushRequeues()["storage_error"] != 1 {
			t.Fatal(st, confirmed, requeued, p.TransferFlushRequeues())
		}
	}
}
