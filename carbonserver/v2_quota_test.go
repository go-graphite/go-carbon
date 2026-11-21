package carbonserver

import (
	"sync"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/points"
)

func TestV2QuotaStoragePendingExpiryAndThroughput(t *testing.T) {
	q, err := NewV2Quotas([]*Quota{{Pattern: "/", Metrics: 2, Throughput: 4, DroppingPolicy: QDPNew}}, time.Hour, func(string) (int64, int64, int64) { return 100, 0, 10 })
	if err != nil {
		t.Fatal(err)
	}
	p := func(n string) *points.Points { return points.OnePoint(n, 1, 1) }
	if !q.ShouldThrottleMetric(p("a.first"), false) {
		t.Fatal("unready quota admitted")
	}
	if err = q.Publish([]V2QuotaMetric{{Name: "classic.first", LogicalSize: 100, DataPoints: 10}}, q.Fence()); err != nil {
		t.Fatal(err)
	}
	if q.ShouldThrottleMetric(p("v2.second"), false) {
		t.Fatal("exact metric limit rejected")
	}
	if !q.ShouldThrottleMetric(p("v2.third"), false) {
		t.Fatal("pending name did not reserve host storage")
	}
	// The catalog hasn't observed the pending second name yet: refresh must keep it.
	if err = q.Publish([]V2QuotaMetric{{Name: "classic.first", LogicalSize: 100, DataPoints: 10}}, q.Fence()); err != nil {
		t.Fatal(err)
	}
	if !q.ShouldThrottleMetric(p("v2.third"), false) {
		t.Fatal("refresh forgot pending storage")
	}
	if err = q.Publish([]V2QuotaMetric{{Name: "classic.first", LogicalSize: 100, DataPoints: 10}, {Name: "v2.second", LogicalSize: 100, DataPoints: 10}}, q.Fence()); err != nil {
		t.Fatal(err)
	}
	recorder := q.index.Load().throughputs.load("/").dpRecorder()
	// Expire both confirmed names; storage frees, consumed throughput does not.
	if err = q.Publish(nil, q.Fence()); err != nil {
		t.Fatal(err)
	}
	if q.index.Load().throughputs.load("/").dpRecorder() != recorder {
		t.Fatal("publication replaced throughput recorder")
	}
	if q.ShouldThrottleMetric(p("v2.third"), false) {
		t.Fatal("expiry did not release storage")
	}
	if !q.ShouldThrottleMetric(p("v2.third"), true) {
		t.Fatal("expiry reset consumed throughput")
	}
}

func TestV2QuotaNamespaceExpiryKeepsThroughput(t *testing.T) {
	q, err := NewV2Quotas([]*Quota{{Pattern: "ns", Throughput: 1, DroppingPolicy: QDPNew}}, time.Hour, func(string) (int64, int64, int64) { return 1, 0, 1 })
	if err != nil {
		t.Fatal(err)
	}
	if err = q.Publish([]V2QuotaMetric{{Name: "ns.old", LogicalSize: 1, DataPoints: 1}}, 0); err != nil {
		t.Fatal(err)
	}
	if q.ShouldThrottleMetric(points.OnePoint("ns.old", 1, 1), false) {
		t.Fatal("first rejected")
	}
	if err = q.Publish([]V2QuotaMetric{{Name: "ns.old", LogicalSize: 1, DataPoints: 1}}, q.Fence()); err != nil {
		t.Fatal(err)
	}
	if err = q.Publish(nil, q.Fence()); err != nil {
		t.Fatal(err)
	}
	if !q.ShouldThrottleMetric(points.OnePoint("ns.new", 1, 1), false) {
		t.Fatal("expired namespace reset throughput")
	}
}

func TestV2QuotaConcurrentNewNames(t *testing.T) {
	q, err := NewV2Quotas([]*Quota{{Pattern: "/", Metrics: 1, DroppingPolicy: QDPNew}}, time.Hour, func(string) (int64, int64, int64) { return 1, 0, 1 })
	if err != nil {
		t.Fatal(err)
	}
	if err = q.Publish(nil, 0); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	accepted := make(chan string, 32)
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			name := string(rune('a' + i))
			if !q.ShouldThrottleMetric(points.OnePoint(name, 1, 1), false) {
				accepted <- name
			}
		}(i)
	}
	wg.Wait()
	close(accepted)
	if len(accepted) != 1 {
		t.Fatalf("accepted %d new names at limit1", len(accepted))
	}
}

func TestV2QuotaRetirementFencesConcurrentAdmission(t *testing.T) {
	q, err := NewV2Quotas([]*Quota{{Pattern: "/", Metrics: 1, DroppingPolicy: QDPNew}}, time.Hour, func(string) (int64, int64, int64) { return 1, 0, 1 })
	if err != nil {
		t.Fatal(err)
	}
	if err = q.Publish([]V2QuotaMetric{{Name: "old", LogicalSize: 1, DataPoints: 1}}, 0); err != nil {
		t.Fatal(err)
	}
	old, _ := q.known.Load("old")
	if err = q.Publish(nil, q.Fence()); err != nil {
		t.Fatal(err)
	}
	if old.(*v2QuotaName).admit(1) {
		t.Fatal("retired membership accepted concurrent writer")
	}
	if q.ShouldThrottleMetric(points.OnePoint("old", 1, 1), false) {
		t.Fatal("recreation could not reserve released storage")
	}
	if !q.ShouldThrottleMetric(points.OnePoint("another", 1, 1), false) {
		t.Fatal("recreated pending reservation lost")
	}
	if q.Rejected() != 1 {
		t.Fatalf("rejected=%d, want1", q.Rejected())
	}
}
