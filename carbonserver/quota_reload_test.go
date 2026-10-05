package carbonserver

import (
	"fmt"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/points"
	"github.com/lomik/zapwriter"
)

func TestQuotaReloadWhileServing(t *testing.T) {
	defer zapwriter.Test()()
	l := savedIndex(t, "namespace.existing")
	l.SetEstimateSize(func(string) (int64, int64, int64) { return 1024, 1024, 60 })
	l.SetQuotaUsageReportFrequency(time.Minute)
	l.SetQuotas([]*Quota{{Pattern: "/", Metrics: 1}})
	l.WarmupIndex()
	waitWarmup(t, l)
	if err := l.Listen("127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}
	url := "http://" + l.tcpListener.Addr().String() + "/metrics/find/?query=namespace.existing&format=json"
	defer checkQuotaReads(t, url)()
	for _, limit := range []int64{20, 1, 30, 1, 0} {
		var rules []*Quota
		if limit > 0 {
			rules = []*Quota{{Pattern: "/", Metrics: limit}}
		}
		if err := l.ReloadQuotas(rules); err != nil {
			t.Fatal(err)
		}
		deadline := time.Now().Add(5 * time.Second)
		for l.ShouldThrottleMetric(points.OnePoint("namespace.new", 1, 1), false) != (limit == 1) {
			if time.Now().After(deadline) {
				t.Fatalf("limit %d did not become effective", limit)
			}
			time.Sleep(time.Millisecond)
		}
		if !l.MetricExists("namespace.existing") {
			t.Fatal("reload lost live index")
		}
	}
	if err := l.ReloadQuotas([]*Quota{{Pattern: "[", Metrics: 1}}); err == nil {
		t.Fatal("invalid pattern accepted")
	}
	if l.ShouldThrottleMetric(points.OnePoint("namespace.new", 1, 1), false) {
		t.Fatal("invalid reload changed old limits")
	}
}

func TestQuotaReloadPreservesUsageAndRemovesRules(t *testing.T) {
	ti := newTrie(".wsp", 0, func(string) (int64, int64, int64) { return 1, 1, 1 })
	if _, err := ti.insert("/namespace/existing.wsp", 1, 1, 1, 0); err != nil {
		t.Fatal(err)
	}
	apply := func(q ...*Quota) {
		t.Helper()
		if _, err := ti.applyQuotas(time.Hour, q...); err != nil {
			t.Fatal(err)
		}
	}
	apply(&Quota{Pattern: "namespace", Metrics: 1, Throughput: 3})
	ti.refreshUsage(ti.throughputs)
	recorder := ti.throughputs.load("namespace")
	if !recorder.withinQuota(2, time.Hour) {
		t.Fatal("initial throughput rejected")
	}
	atomic.StoreInt64(&recorder.dpRecorder().dataPoints, 2)
	apply(&Quota{Pattern: "namespace", Metrics: 20, Throughput: 100})
	if ti.throughputs.load("namespace") != recorder {
		t.Fatal("reload replaced throughput accounting")
	}
	apply(&Quota{Pattern: "namespace", Metrics: 1, Throughput: 3})
	if !recorder.withinQuota(1, time.Hour) || recorder.withinQuota(2, time.Hour) {
		t.Fatal("reload reset consumed throughput")
	}
	if _, err := ti.applyQuotas(time.Hour, &Quota{Pattern: "["}, &Quota{Pattern: "namespace", Metrics: 100}); err == nil {
		t.Fatal("invalid pattern accepted")
	}
	if recorder.quota().Metrics != 1 {
		t.Fatal("invalid rules partially changed enforcement")
	}
	meta := ti.quotaNodes["namespace"]
	if meta.withinQuota(1, 0, 0, 0, 0) {
		t.Fatal("existing usage was lost on reload")
	}
	apply(&Quota{Pattern: "/", Metrics: 100})
	if !meta.withinQuota(1, 0, 0, 0, 0) || ti.throughputs.load("namespace") != nil {
		t.Fatal("removed rule still enforced")
	}
	ti.refreshUsage(ti.throughputs) // removed metadata must be safe for statistics
}

// checkQuotaReads continuously checks the live listener while rules change and
// returns a cleanup that reports any read failure to the owning test.
func checkQuotaReads(t *testing.T, url string) func() {
	t.Helper()
	stop := make(chan struct{})
	failures := make(chan error, 1)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			res, err := http.Get(url)
			if err != nil {
				select {
				case failures <- err:
				default:
				}
				return
			}
			body, err := io.ReadAll(res.Body)
			res.Body.Close()
			if err != nil || res.StatusCode != 200 {
				select {
				case failures <- fmt.Errorf("read during reload: %d %s: %w", res.StatusCode, body, err):
				default:
				}
				return
			}
		}
	}()
	return func() {
		close(stop)
		wg.Wait()
		select {
		case err := <-failures:
			t.Error(err)
		default:
		}
	}
}
