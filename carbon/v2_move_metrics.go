package carbon

import (
	"fmt"
	"strconv"

	"github.com/go-graphite/go-carbon/helper"
	"github.com/go-graphite/go-carbon/internal/v2daemon"
	"github.com/prometheus/client_golang/prometheus"
)

var v2MoveValues = []string{"phase", "parts_done", "parts_total", "bytes_done", "bytes_total", "megabytes_per_second", "eta_seconds", "failures", "bytes_transferred", "verifications"}
var v2MoveDesc = prometheus.NewDesc("v2_move_progress", "Durable move progress; phase: pending=0 received=1 verified=2 staged=3 active=4 done=5.", []string{"move", "measure"}, nil)
var v2MoveOwnerDesc = prometheus.NewDesc("v2_move_owner_state", "Durable owner state; pending=0 verified=2 staged=3 active=4.", []string{"move", "owner"}, nil)
var v2MoveRegressionDesc = prometheus.NewDesc("v2_move_regressions_total", "Refused attempts to decrease durable move progress in this process.", nil, nil)

func moveValues(s v2daemon.MoveStatus) []float64 {
	return []float64{v2daemon.MovePhaseNumber(s.Phase), float64(s.PartsDone), float64(s.PartsTotal), float64(s.BytesDone), float64(s.BytesTotal), s.MBps, s.ETASeconds, float64(s.FailureCount), float64(s.BytesTransferred), float64(s.VerifyCount)}
}
func collectV2Moves(a *v2daemon.MoveAdmin, ch chan<- prometheus.Metric) {
	if a == nil {
		return
	}
	ch <- prometheus.MustNewConstMetric(v2MoveRegressionDesc, prometheus.CounterValue, float64(a.MoveRegressions()))
	for _, s := range a.MoveStatuses() {
		for i, v := range moveValues(s) {
			ch <- prometheus.MustNewConstMetric(v2MoveDesc, prometheus.GaugeValue, v, s.ID, v2MoveValues[i])
		}
		for i, o := range s.Owners {
			ch <- prometheus.MustNewConstMetric(v2MoveOwnerDesc, prometheus.GaugeValue, v2daemon.MovePhaseNumber(o.State), s.ID, strconv.Itoa(i))
		}
	}
}

type v2MoveStats struct{ admin *v2daemon.MoveAdmin }

func (s v2MoveStats) Stat(send helper.StatCallback) {
	send("regressions", float64(s.admin.MoveRegressions()))
	for _, m := range s.admin.MoveStatuses() {
		for i, v := range moveValues(m) {
			send(m.ID+"."+v2MoveValues[i], v)
		}
		for i, o := range m.Owners {
			send(fmt.Sprintf("%s.owner.%d.state", m.ID, i), v2daemon.MovePhaseNumber(o.State))
		}
	}
}
