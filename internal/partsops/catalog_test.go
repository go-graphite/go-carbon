package partsops

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/partsformat"
	"github.com/go-graphite/go-carbon/tags"
)

type recordingCommitter struct {
	batches []Batch
	fail    bool
}

func (r *recordingCommitter) Commit(_ context.Context, b Batch) error {
	if r.fail {
		return errors.New("injected commit failure")
	}
	r.batches = append(r.batches, b)
	return nil
}
func testMetric(name string) Metric {
	return Metric{Scope: Scope{PlacementGroup: "service", VShard: partsformat.Route(partsformat.ID(name))}, Metadata: store.Metadata{MetricConfig: store.MetricConfig{Name: name, Retentions: []store.Retention{{Step: 1, Count: 60}}}, Generation: 1, Revision: 1, LastUpdate: time.Unix(1000, 0)}}
}

func TestCatalogCommitConflictFailureAndRecreation(t *testing.T) {
	if !Visible(0, Tombstone{}) {
		t.Fatal("unstamped legacy hidden without tombstone")
	}
	ctx := context.Background()
	m := testMetric("service.cpu")
	sink := &recordingCommitter{}
	o, err := NewOwner(Snapshot{Scope: m.Scope, Metrics: []Metric{m}}, sink)
	if err != nil {
		t.Fatal(err)
	}
	before := o.Snapshot()
	plan := PlanExpiration(before, ExpirationPolicy{Fallback: time.Hour}, time.Unix(10000, 0), nil)
	if len(plan.Delete) != 1 {
		t.Fatal(plan)
	}
	sink.fail = true
	if err := o.Apply(ctx, nil, plan.Delete); err == nil || !reflect.DeepEqual(o.Snapshot(), before) {
		t.Fatal("failed commit became visible", err)
	}
	sink.fail = false
	newer := cloneMetric(m)
	newer.Metadata.Revision++
	newer.Metadata.LastUpdate = time.Unix(10001, 0)
	if err := o.Apply(ctx, []Metric{newer}, nil); err != nil {
		t.Fatal(err)
	}
	if err := o.Apply(ctx, nil, plan.Delete); !errors.Is(err, ErrConflict) {
		t.Fatal("stale expiration removed activity", err)
	}
	plan = PlanExpiration(o.Snapshot(), ExpirationPolicy{Fallback: time.Hour}, time.Unix(20000, 0), nil)
	if err := o.Apply(ctx, nil, plan.Delete); err != nil {
		t.Fatal(err)
	}
	dead := o.Snapshot()
	if len(dead.Metrics) != 0 || len(dead.Tombstones) != 1 || Visible(1, dead.Tombstones[0]) {
		t.Fatal(dead)
	}
	// A crash restores the durable tombstone; an old SST cannot resurrect it.
	restarted, err := NewOwner(dead, sink)
	if err != nil {
		t.Fatal(err)
	}
	if err := restarted.Apply(ctx, []Metric{m}, nil); !errors.Is(err, ErrConflict) {
		t.Fatal("resurrection accepted", err)
	}
	m.Metadata.Generation = 2
	if err := restarted.Apply(ctx, []Metric{m}, nil); err != nil {
		t.Fatal(err)
	}
	if !Visible(2, dead.Tombstones[0]) {
		t.Fatal("new incarnation hidden")
	}
	if sink.batches[0].ExpectedRevision != 0 || sink.batches[0].Revision != 1 {
		t.Fatal(sink.batches)
	}
}

func TestCatalogSnapshotsIndependentOfWriter(t *testing.T) {
	m := testMetric("service.load")
	o, err := NewOwner(Snapshot{Scope: m.Scope, Metrics: []Metric{m}}, &recordingCommitter{})
	if err != nil {
		t.Fatal(err)
	}
	frozen := o.Snapshot()
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 500 {
				s := o.Snapshot()
				s.Metrics[0].Metadata.Retentions[0].Count = 999
			}
		}()
	}
	for range 100 {
		m.Metadata.Revision++
		if err := o.Apply(context.Background(), []Metric{m}, nil); err != nil {
			t.Fatal(err)
		}
	}
	wg.Wait()
	if frozen.Metrics[0].Metadata.Revision != 1 || o.Snapshot().Metrics[0].Metadata.Retentions[0].Count != 60 {
		t.Fatal("snapshot aliased writer")
	}
}

func TestCatalogUsesCurrentTagNormalization(t *testing.T) {
	for _, raw := range []string{"metric;z=last;a=first", "metric;x=old;x=new", "metric;a=1;z=2", "metric;bad", ";a=1"} {
		canonical, err := tags.Normalize(raw)
		if err != nil {
			continue
		}
		m := testMetric(canonical)
		if _, err := NewOwner(Snapshot{Scope: m.Scope, Metrics: []Metric{m}}, &recordingCommitter{}); err != nil {
			t.Fatal(raw, err)
		}
		got, e := NormalizeName(raw, true)
		if e != nil || got != canonical {
			t.Fatal("tag normalization differs", got, e)
		}
		got, e = NormalizeName(raw, false)
		if e != nil || got != raw {
			t.Fatal("disabled tags changed name", got, e)
		}
	}
}

func TestCatalogRejectsCrossScopeAndAliasing(t *testing.T) {
	m := testMetric("service.cpu")
	o, _ := NewOwner(Snapshot{Scope: m.Scope}, &recordingCommitter{})
	bad := m
	bad.Scope.PlacementGroup = "other"
	if err := o.Apply(context.Background(), []Metric{bad}, nil); err == nil {
		t.Fatal("cross-placement-group write")
	}
	bad = m
	bad.Scope.VShard ^= 1
	if err := o.Apply(context.Background(), []Metric{bad}, nil); err == nil {
		t.Fatal("cross-vshard write")
	}
	if err := o.Apply(context.Background(), []Metric{m}, nil); err != nil {
		t.Fatal(err)
	}
	m.Metadata.Retentions[0].Count = 999
	if o.Snapshot().Metrics[0].Metadata.Retentions[0].Count != 60 {
		t.Fatal("caller mutated committed metadata")
	}
}
