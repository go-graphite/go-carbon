package carbon

import (
	"time"

	"github.com/go-graphite/go-carbon/carbonserver"
	"github.com/lomik/zapwriter"
	"go.uber.org/zap"
)

const buckydIndexRefreshInterval = 30 * time.Second

// metricIndexRefresher coalesces transfer mutations into bounded catalog scans.
// A buffered signal also records mutations that arrive during an active scan.
type metricIndexRefresher struct {
	changes chan struct{}
	stop    chan struct{}
	done    chan struct{}
}

func startMetricIndexRefresher(listener *carbonserver.CarbonserverListener, interval time.Duration) *metricIndexRefresher {
	r := &metricIndexRefresher{
		changes: make(chan struct{}, 1),
		stop:    make(chan struct{}),
		done:    make(chan struct{}),
	}
	go func() {
		defer close(r.done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-r.stop:
				return
			case <-ticker.C:
				select {
				case <-r.changes:
					if err := listener.RefreshMetricStoreIndex(); err != nil {
						zapwriter.Logger("buckyd").Error("refresh shared metric index", zap.Error(err))
					}
				default:
				}
			}
		}
	}()
	return r
}

func (r *metricIndexRefresher) notify() {
	select {
	case r.changes <- struct{}{}:
	default:
	}
}

func (r *metricIndexRefresher) close() {
	close(r.stop)
	<-r.done
}
