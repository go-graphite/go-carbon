package carbon

import (
	"net"

	"github.com/go-graphite/go-carbon/receiver/socketstats"
)

// startSocketStats runs after receiver setup, including startup-capture and
// inherited listeners. Use bound addresses so custom listeners and port 0 work.
func (app *App) startSocketStats() {
	listeners := map[string]*net.TCPAddr{}
	for _, r := range app.Receivers {
		if bound, ok := r.Receiver.(interface{ Addr() net.Addr }); ok {
			if addr, ok := bound.Addr().(*net.TCPAddr); ok && addr != nil {
				listeners[r.Name] = addr
			}
		}
	}
	app.SocketStats = socketstats.New(listeners)
	if app.SocketStats != nil && app.Config.Prometheus.Enabled {
		app.PromRegisterer.MustRegister(app.SocketStats)
	}
}
