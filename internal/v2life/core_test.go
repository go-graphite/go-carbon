package v2life

import (
	"errors"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/go-graphite/go-carbon/internal/v2core"
)

func TestCoreCatalogAtomicSealExpiryAndResume(t *testing.T) {
	for _, blind := range []bool{false, true} {
		t.Run(map[bool]string{false: "sst", true: "blind"}[blind], func(t *testing.T) {
			root := t.TempDir()
			m := testMetric("service.resumes")
			m.Metadata.LastUpdate = time.Unix(1000, 123456789)
			c, err := NewCoreCatalog(Snapshot{Scope: m.Scope})
			if err != nil {
				t.Fatal(err)
			}
			cfg := v2core.Config{JournalIdentity: "life-test", Shards: 1, Owners: 1, Producers: 1, MaxRows: 64, PartitionSeconds: 60, BlindBatch: blind}
			now := m.Metadata.LastUpdate
			var expire *ExpirationPolicy
			hooks := v2core.Hooks{PlanSeal: func(_ int, in v2core.SealInput) (v2core.SealPlan, error) {
				var upserts []Metric
				if len(in.Rows) > 0 {
					upserts = []Metric{m}
				}
				plan, _, err := c.Plan(upserts, expire, now, nil)
				return plan, err
			}}
			e, err := v2core.Open(root, cfg, hooks)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if e != nil {
					_ = e.Close()
				}
			}()
			if err = e.AppendBatches(0, []v2core.Batch{{ID: 1, Points: []v2core.Row{{ID: 1, Time: 1000, Bits: 1, Seq: 1}}}}, 1, true); err != nil {
				t.Fatal(err)
			}
			frozen := e.DBs()[0].NewSnapshot()
			defer func() {
				if frozen != nil {
					_ = frozen.Close()
				}
			}()
			first, err := ReadCoreCatalog(frozen, m.Scope)
			if err != nil || len(first.Metrics) != 1 || !first.Metrics[0].Metadata.LastUpdate.Equal(m.Metadata.LastUpdate) {
				t.Fatal(first, err)
			}
			expire = &ExpirationPolicy{Fallback: time.Hour}
			now = now.Add(time.Hour)
			if err = e.Append(0, nil, 2, true); err != nil {
				t.Fatal(err)
			}
			snap := e.DBs()[0].NewSnapshot()
			dead, err := ReadCoreCatalog(snap, m.Scope)
			_ = snap.Close()
			if err != nil || len(dead.Metrics) != 0 || len(dead.Tombstones) != 1 || Visible(1, dead.Tombstones[0]) || e.DurableFrontiers()[0][0] != 2 {
				t.Fatal(dead, err)
			}
			old, err := ReadCoreCatalog(frozen, m.Scope)
			if err != nil || !reflect.DeepEqual(first, old) {
				t.Fatal("pinned catalog changed", old, err)
			}
			_ = frozen.Close()
			frozen = nil
			if err = e.Close(); err != nil {
				t.Fatal(err)
			}
			e = nil
			// Reopen restores the exact deletion and clock. The original incarnation
			// remains hidden after restart; accepted new writes get authority generation 2.
			db, err := pebble.Open(filepath.Join(root, "owner-000"), &pebble.Options{})
			if err != nil {
				t.Fatal(err)
			}
			s := db.NewSnapshot()
			restored, err := ReadCoreCatalog(s, m.Scope)
			_ = s.Close()
			_ = db.Close()
			if err != nil || !reflect.DeepEqual(restored, dead) {
				t.Fatal(restored, err)
			}
			c, err = NewCoreCatalog(restored)
			if err != nil {
				t.Fatal(err)
			}
			if _, _, err = c.Plan([]Metric{m}, nil, now, nil); !errors.Is(err, ErrConflict) {
				t.Fatal("old copy resurrected", err)
			}
			m.Metadata.Generation = 2
			m.Metadata.LastUpdate = now
			e, err = v2core.Open(root, cfg, hooks)
			if err != nil {
				t.Fatal(err)
			}
			if err = e.AppendBatches(0, []v2core.Batch{{ID: 1, Points: []v2core.Row{{ID: 1, Time: now.Unix(), Bits: 2, Seq: 2}}}}, 3, true); err != nil {
				t.Fatal(err)
			}
			snap = e.DBs()[0].NewSnapshot()
			live, err := ReadCoreCatalog(snap, m.Scope)
			_ = snap.Close()
			if err != nil || len(live.Metrics) != 1 || !Visible(2, live.Tombstones[0]) {
				t.Fatal(live, err)
			}
			if err = e.Close(); err != nil {
				t.Fatal(err)
			}
			e = nil
		})
	}
}

func TestCoreCatalogPlanningAndIngestFailureStayPrivate(t *testing.T) {
	m := testMetric("service.failure")
	c, err := NewCoreCatalog(Snapshot{Scope: m.Scope})
	if err != nil {
		t.Fatal(err)
	}
	plan, _, err := c.Plan([]Metric{m}, nil, time.Time{}, nil)
	if err != nil || len(c.Snapshot().Metrics) != 0 {
		t.Fatal("planning published", err)
	}
	if plan.Abort != nil {
		plan.Abort()
	}
	if len(c.Snapshot().Metrics) != 0 {
		t.Fatal("abort published")
	}
	cfg := v2core.Config{JournalIdentity: "failed-life", Shards: 1, Producers: 1, MaxRows: 64, PartitionSeconds: 60}
	e, err := v2core.Open(t.TempDir(), cfg, v2core.Hooks{PlanSeal: func(_ int, _ v2core.SealInput) (v2core.SealPlan, error) {
		p, _, err := c.Plan([]Metric{m}, nil, time.Time{}, nil)
		// Deliberate reserved-key validation failure, after successful planning.
		p.Files[0][0].Key = []byte{0}
		return p, err
	}})
	if err != nil {
		t.Fatal(err)
	}
	if err = e.AppendBatches(0, []v2core.Batch{{ID: 1, Points: []v2core.Row{{ID: 1, Time: 1000}}}}, 1, true); err == nil {
		t.Fatal("expected atomic seal failure")
	}
	if e.DurableFrontiers()[0][0] != 0 || len(c.Snapshot().Metrics) != 0 {
		t.Fatal("failed seal advanced lifecycle/frontier")
	}
	s := e.DBs()[0].NewSnapshot()
	got, err := ReadCoreCatalog(s, m.Scope)
	_ = s.Close()
	if err != nil || len(got.Metrics) != 0 {
		t.Fatal(got, err)
	}
	if err = e.Close(); err == nil {
		t.Fatal("owner failure not sticky")
	}
}
