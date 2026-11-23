package whisperio

import (
	"context"
	"errors"
	"testing"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

func TestSpliceWindowAliasesGuard(t *testing.T) {
	fragment := func(ts ...int64) []spliceFragment {
		var result []spliceFragment
		for _, stamp := range ts {
			result = append(result, spliceFragment{first: stamp, count: 1})
		}
		return result
	}
	for _, count := range []int{1024, 2_000_000} {
		r := store.Retention{Step: 1, Count: count}
		epoch := int64(3 * count)
		window := store.NativeWindow{From: epoch, Until: epoch + 130}
		for _, tt := range []struct {
			name            string
			donor, survivor []spliceFragment
			stale           int
			safe            bool
		}{
			{"stale", fragment(epoch), fragment(epoch - int64(count)), 1, true},
			{"target-wins", fragment(epoch), fragment(epoch), 0, true},
			{"newer-outside", fragment(epoch), fragment(epoch + int64(count)), 0, false},
			{"outside-aliases", fragment(epoch), fragment(epoch-int64(count), epoch-2*int64(count)), 0, false},
			{"donor-outside", fragment(epoch - 1), nil, 0, false},
			{"future", fragment(epoch), fragment(epoch + 2*int64(count)), 0, false},
		} {
			t.Run(tt.name, func(t *testing.T) {
				stale, safe, err := spliceWindowAliases(context.Background(), tt.donor, tt.survivor, r, window, epoch+int64(count))
				if err != nil || stale != tt.stale || safe != tt.safe {
					t.Fatalf("got %d %v %v", stale, safe, err)
				}
			})
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, err := spliceWindowAliases(ctx, fragment(100), nil, store.Retention{Step: 1, Count: 10}, store.NativeWindow{From: 100, Until: 101}, 200); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}
