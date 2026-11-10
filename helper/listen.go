package helper

import (
	"errors"
	"fmt"
	"net"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/lomik/zapwriter"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

var listenerBindRetries atomic.Uint64

// ListenerBindMetrics reports retries across listeners, including the profiling
// listener that starts before the application and its metrics registry.
func ListenerBindMetrics() prometheus.Collector {
	return prometheus.NewCounterFunc(prometheus.CounterOpts{Name: "listener_bind_retries_total", Help: "Listener bind retries after address already in use."}, func() float64 { return float64(listenerBindRetries.Load()) })
}

// ListenTCPAddress resolves a configured TCP endpoint before binding it with
// the same bounded retry policy as receivers using a resolved TCPAddr.
func ListenTCPAddress(network, address string) (*net.TCPListener, error) {
	addr, err := net.ResolveTCPAddr(network, address)
	if err != nil {
		return nil, err
	}
	return ListenTCP(network, addr)
}

// ListenTCP gives a killed predecessor up to ten minutes to close its sockets.
// Large processes can finish releasing memory before releasing their file table.
// Retry only EADDRINUSE; malformed addresses and other errors fail immediately.
func ListenTCP(network string, addr *net.TCPAddr) (*net.TCPListener, error) {
	return listenTCP(network, addr, 10*time.Minute, time.Second)
}

func listenTCP(network string, addr *net.TCPAddr, timeout, interval time.Duration) (*net.TCPListener, error) {
	deadline := time.Now().Add(timeout)
	for {
		ln, err := net.ListenTCP(network, addr)
		if err == nil || !errors.Is(err, syscall.EADDRINUSE) {
			return ln, err
		}
		left := time.Until(deadline)
		if left <= 0 {
			return nil, fmt.Errorf("listener address remained in use for %s: %w", timeout, err)
		}
		listenerBindRetries.Add(1)
		zapwriter.Logger("listener").Error("listener address still in use; retrying", zap.Error(err), zap.Duration("remaining", left))
		time.Sleep(min(interval, left))
	}
}
