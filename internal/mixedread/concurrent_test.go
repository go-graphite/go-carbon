package mixedread

import (
	"context"
	"errors"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/vfs"
	"github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/partsread"
	"github.com/go-graphite/go-carbon/internal/whisperio"
)

type overlapProbeCodec struct {
	*whisperio.ContinuationCodec
	before func() error
}

func (c *overlapProbeCodec) NewFrozenReadDecoder(io chunkstore.ContinuationIO, m chunkstore.Metadata, clock func() time.Time) (chunkstore.FrozenReadDecoder, error) {
	if c.before != nil {
		if err := c.before(); err != nil {
			return nil, err
		}
	}
	return c.ContinuationCodec.NewFrozenReadDecoder(io, m, clock)
}
func coldOverlayFixture(t *testing.T, before func() error, read func(context.Context, func(partsread.Sample) error) error) *Reader {
	t.Helper()
	const now = 1794700800
	ctx := context.Background()
	fs := vfs.NewMem()
	codec := &overlapProbeCodec{ContinuationCodec: &whisperio.ContinuationCodec{ReuseBlockKeys: true, DirectLateWrites: true}}
	opts := chunkstore.Options{ExactBlocks: true, TimeBuckets: true, Continuation: codec, Now: func() time.Time { return time.Unix(now, 0) }}
	s, err := chunkstore.Open("legacy", opts.WithFS(fs))
	if err != nil {
		t.Fatal(err)
	}
	cfg := chunkstore.MetricConfig{Name: "metric", Retentions: []chunkstore.Retention{{Step: 1, Count: 1000}}, AggregationMethod: chunkstore.Last, XFilesFactor: .5}
	if _, err = s.Create(ctx, cfg); err != nil {
		t.Fatal(err)
	}
	if err = s.UpdateMany(ctx, cfg.Name, []chunkstore.Point{{Timestamp: now - 100, Value: 5}, {Timestamp: now - 99, Value: 7}}); err != nil {
		t.Fatal(err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	opts.ReadOnly = true
	s, err = chunkstore.Open("legacy", opts.WithFS(fs))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	metric := partsread.Metric{ID: 42, Archives: []partsread.Archive{{Step: 1, Points: 1000}}, AggregationMethod: "last", XFilesFactor: .5}
	frozen, err := s.ResolveFrozen(ctx, cfg.Name)
	if err != nil {
		t.Fatal(err)
	}
	binding, err := BindLegacy(frozen, metric)
	if err != nil {
		t.Fatal(err)
	}
	slot := int64((now - 100) % 1000)
	source, err := partsread.NewSlotIndex([]partsread.SlotBlock{{ID: 42, Archive: metric.Archives[0], Slots: partsread.SlotRange{From: slot, Until: slot + 1}, Key: []byte("part")}}, func(ctx context.Context, _ []byte, _ []partsread.SlotRange, yield func(partsread.Sample) error) error {
		return read(ctx, yield)
	})
	if err != nil {
		t.Fatal(err)
	}
	cat := catalog(map[string]partsread.Metric{cfg.Name: metric})
	pub := new(partsread.Publication)
	pub.Publish(partsread.View{Catalog: entryCatalog{Catalog: cat, entries: map[string]catalogEntry{cfg.Name: {metric, binding}}}, Parts: []partsread.Source{source}})
	t.Cleanup(pub.Close)
	snap, err := pub.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(snap.Close)
	r, err := New(Options{Legacy: s, LegacyCatalog: cat, Snapshot: snap, UnifiedCatalog: true, Parts: partsread.Adapter{Catalog: pub, Select: func(string) *partsread.Publication { return pub }, Budget: partsread.NewBudget(8 << 20), WorkBytes: 1 << 20, Now: opts.Now}})
	if err != nil {
		t.Fatal(err)
	}
	codec.before = before
	return r
}

func TestColdOverlayReadsOverlapAndPreserveBits(t *testing.T) {
	for _, bits := range []uint64{math.Float64bits(-13), 0x8000000000000000, 0x7ff8000000000042} {
		t.Run(string(rune(bits%26+'a')), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			part, legacy := make(chan struct{}), make(chan struct{})
			var partOnce, legacyOnce sync.Once
			r := coldOverlayFixture(t, func() error {
				select {
				case <-part:
					legacyOnce.Do(func() { close(legacy) })
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			}, func(c context.Context, yield func(partsread.Sample) error) error {
				partOnce.Do(func() { close(part) })
				select {
				case <-legacy:
					return yield(partsread.Sample{Time: 1794700700, Bits: bits, Sequence: 1})
				case <-ctx.Done():
					return ctx.Err()
				}
			})
			for range 2 {
				err := r.Fetch(ctx, "metric", 1794700800-900, 1794700800-98, func(s *partsread.Series) error {
					i := (int64(1794700700) - s.From) / s.Step
					if math.Float64bits(s.Values[i]) != bits || s.Values[i+1] != 7 {
						t.Fatal("overlay changed exact values")
					}
					return nil
				})
				if err != nil {
					t.Fatal(err)
				}
			}
			if r.opts.Parts.Budget.Used() != 0 {
				t.Fatal("admission leaked")
			}
		})
	}
}
func TestColdOverlayCancellationJoinsPartRead(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	entered, finished := make(chan struct{}), make(chan struct{})
	r := coldOverlayFixture(t, func() error {
		select {
		case <-entered:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}, func(c context.Context, _ func(partsread.Sample) error) error {
		close(entered)
		defer close(finished)
		<-c.Done()
		return c.Err()
	})
	done := make(chan error, 1)
	go func() {
		done <- r.Fetch(ctx, "metric", 1794700800-900, 1794700800-98, func(*partsread.Series) error { return errors.New("consumer called after cancellation") })
	}()
	select {
	case <-entered:
		cancel()
	case <-ctx.Done():
		t.Fatal("part read did not start")
	}
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel=%v", err)
	}
	select {
	case <-finished:
	default:
		t.Fatal("part read outlived handle")
	}
	if r.opts.Parts.Budget.Used() != 0 {
		t.Fatal("admission leaked")
	}
}
func TestColdOverlayPanicReturnsToCaller(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	entered := make(chan struct{})
	sentinel := errors.New("part reader panic")
	r := coldOverlayFixture(t, func() error {
		select {
		case <-entered:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}, func(context.Context, func(partsread.Sample) error) error { close(entered); panic(sentinel) })
	defer func() {
		if recover() != sentinel {
			t.Error("part panic did not reach caller")
		}
		if r.opts.Parts.Budget.Used() != 0 {
			t.Error("admission leaked")
		}
	}()
	_ = r.Fetch(ctx, "metric", 1794700800-900, 1794700800-98, func(*partsread.Series) error { return nil })
}
