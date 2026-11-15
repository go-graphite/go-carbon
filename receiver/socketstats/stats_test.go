package socketstats

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

func TestExports(t *testing.T) {
	c := &Collector{current: snapshot{host: map[string]uint64{"ListenDrops": 42}, listeners: map[string]queues{"tcp": {3, 2048, 1000, 2}}, hostOK: true, socketsOK: true}}
	registry := prometheus.NewPedanticRegistry()
	prometheus.WrapRegistererWith(prometheus.Labels{"cluster": "test"}, registry).MustRegister(c)
	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	if len(families) != 8 {
		t.Fatalf("got %d metric families", len(families))
	}
	stats := map[string]float64{}
	c.Stat(func(name string, v float64) { stats[name] = v })
	if stats["host_tcp_listen_drops_total"] != 42 || stats["tcp.receiver_tcp_connections"] != 2 || stats["tcp.receiver_tcp_accept_queue_limit"] != 2048 {
		t.Fatal(stats)
	}
	for _, family := range families {
		for _, m := range family.Metric {
			found := false
			for _, l := range m.Label {
				if l.GetName() == "cluster" && l.GetValue() == "test" {
					found = true
				}
			}
			if !found {
				t.Fatal("missing registry labels")
			}
		}
	}
}
