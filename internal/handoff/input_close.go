package handoff

import (
	"errors"
	"io"
	"os"
	"syscall"
	"time"

	"github.com/lomik/zapwriter"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

// InputRetentionTimeout bounds retries after the first failed detached transfer.
// Tests may inject a short window. Repeated failures never extend it.
var InputRetentionTimeout = 2 * time.Minute

var ErrInputRetentionExpired = errors.New("detached input retention expired")

func boundedDeadline(timeout time.Duration, until time.Time) time.Time {
	deadline := time.Now().Add(timeout)
	if !until.IsZero() && until.Before(deadline) {
		return until
	}
	return deadline
}

// InputCloseStats counts discarded connections and their retained state. Kernel
// bytes are sampled immediately before close, without reading the socket. An
// unknown count is separate from zero. Leftovers include decoder state as well
// as unparsed wire bytes, so they are not a count of lost points.
type InputCloseStats struct {
	Connections, KernelUnreadBytes, KernelUnreadUnknown, LeftoverBytes uint64
}

type inputCloseMetrics struct {
	connections, kernel, unknown, leftover *prometheus.CounterVec
}

var closeMetrics = func() *inputCloseMetrics {
	counter := func(name, help string) *prometheus.CounterVec {
		return prometheus.NewCounterVec(prometheus.CounterOpts{Name: name, Help: help}, []string{"reason"})
	}
	return &inputCloseMetrics{
		connections: counter("input_handoff_closed_connections_total", "Owned input connections discarded without a committed handoff."),
		kernel:      counter("input_handoff_closed_kernel_unread_bytes_total", "Unread kernel receive-buffer bytes sampled immediately before discarding input connections; excludes unknown counts."),
		unknown:     counter("input_handoff_closed_kernel_unread_unknown_total", "Discarded input connections whose kernel unread-byte count was unavailable."),
		leftover:    counter("input_handoff_closed_leftover_bytes_total", "Retained decoder state and unparsed user-space bytes discarded with input connections; not a count of lost points."),
	}
}()

// InputCloseMetrics exports process-wide close counters in an application registry.
// The final ERROR logs remain available after the predecessor stops serving metrics.
func InputCloseMetrics() prometheus.Collector { return closeMetrics }

func (m *inputCloseMetrics) Describe(ch chan<- *prometheus.Desc) {
	for _, c := range []*prometheus.CounterVec{m.connections, m.kernel, m.unknown, m.leftover} {
		c.Describe(ch)
	}
}

func (m *inputCloseMetrics) Collect(ch chan<- prometheus.Metric) {
	for _, c := range []*prometheus.CounterVec{m.connections, m.kernel, m.unknown, m.leftover} {
		c.Collect(ch)
	}
}

// KernelUnread returns the Linux SIOCINQ count, or -1 and an error when unknown.
func KernelUnread(conn io.Closer) (int, error) {
	c, ok := conn.(syscall.Conn)
	if !ok {
		return -1, errors.ErrUnsupported
	}
	raw, err := c.SyscallConn()
	if err != nil {
		return -1, err
	}
	return kernelUnread(raw)
}

// CloseInputConn discards an owned socket and reports its kernel and user-space
// bytes. Only ownership loss increments counters; releasing duplicates does not.
func CloseInputConn(conn io.Closer, leftover int, logger *zap.Logger, reason string) InputCloseStats {
	return closeInputConn(conn, leftover, logger, reason, true)
}

func closeInputConn(conn io.Closer, leftover int, logger *zap.Logger, reason string, discarded bool) InputCloseStats {
	n, readErr := KernelUnread(conn)
	err := conn.Close()
	if errors.Is(err, os.ErrClosed) || errors.Is(err, syscall.EBADF) {
		return InputCloseStats{}
	}
	s := InputCloseStats{Connections: 1, LeftoverBytes: uint64(leftover)}
	if readErr != nil {
		n = -1
		s.KernelUnreadUnknown = 1
	} else {
		s.KernelUnreadBytes = uint64(n)
	}
	fields := []zap.Field{zap.String("reason", reason), zap.Int("kernel_unread_bytes", n),
		zap.Int("leftover_bytes", leftover), zap.Bool("discarded", discarded), zap.Error(err)}
	if readErr != nil {
		fields = append(fields, zap.NamedError("kernel_unread_error", readErr))
	}
	if f, ok := conn.(interface{ Name() string }); ok {
		fields = append(fields, zap.String("file", f.Name()))
	}
	if discarded {
		closeMetrics.connections.WithLabelValues(reason).Inc()
		closeMetrics.kernel.WithLabelValues(reason).Add(float64(s.KernelUnreadBytes))
		closeMetrics.unknown.WithLabelValues(reason).Add(float64(s.KernelUnreadUnknown))
		closeMetrics.leftover.WithLabelValues(reason).Add(float64(s.LeftoverBytes))
		logger.Error("input handoff socket discarded", fields...)
	} else if n != 0 || leftover != 0 {
		logger.Info("input handoff socket copy released", fields...)
	}
	return s
}

// Close discards input owned by this process. Successful transfers release only
// duplicate descriptors; failed receiving attempts have never owned the sockets.
func (in *Input) Close() { in.Discard(zapwriter.Logger("handoff"), "input_abandoned") }

// Discard closes owned input and logs exact totals, including unknown counts.
func (in *Input) Discard(logger *zap.Logger, reason string) InputCloseStats {
	return in.close(logger, reason, true)
}

func (in *Input) closeCopies(reason string) {
	in.close(zapwriter.Logger("handoff"), reason, false)
}

func (in *Input) close(logger *zap.Logger, reason string, discarded bool) InputCloseStats {
	var total InputCloseStats
	for _, r := range in.Receivers {
		if r.Listener != nil {
			_ = r.Listener.Close()
		}
		for _, c := range r.Conns {
			s := closeInputConn(c.File, len(c.Leftover), logger, reason, discarded)
			total.Connections += s.Connections
			total.KernelUnreadBytes += s.KernelUnreadBytes
			total.KernelUnreadUnknown += s.KernelUnreadUnknown
			total.LeftoverBytes += s.LeftoverBytes
		}
	}
	in.Receivers = nil
	if discarded && total.Connections > 0 {
		logger.Error("input handoff sockets discarded", zap.String("reason", reason),
			zap.Uint64("connections_closed", total.Connections), zap.Uint64("kernel_unread_bytes", total.KernelUnreadBytes),
			zap.Uint64("kernel_unread_unknown", total.KernelUnreadUnknown), zap.Uint64("leftover_bytes", total.LeftoverBytes))
	}
	return total
}
