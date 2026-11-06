package tcp

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"net"
	"sync/atomic"
	"time"

	"github.com/go-graphite/go-carbon/helper"
	"github.com/go-graphite/go-carbon/internal/handoff"
	"github.com/go-graphite/go-carbon/points"
	"github.com/go-graphite/go-carbon/receiver/stream/codec"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

const (
	streamConnections = iota
	streamFrames
	streamPayloadBytes
	streamNamesDefined
	streamResets
	streamDecodeErrors
	streamCounterCount
)

var streamStatNames = [streamCounterCount]string{
	"streamConnections", "streamFrames", "streamPayloadBytes", "streamNamesDefined", "streamResets", "streamDecodeErrors",
}
var streamPromNames = [streamCounterCount]string{
	"stream_connections_tcp_total", "stream_frames_tcp_total", "stream_payload_bytes_tcp_total", "stream_names_defined_tcp_total", "stream_resets_tcp_total", "stream_decode_errors_tcp_total",
}
var streamPromHelp = [streamCounterCount]string{
	"Counter of stream connections received via the TCP endpoint.",
	"Counter of validated stream frames received via the TCP endpoint.",
	"Counter of validated stream payload bytes received via the TCP endpoint.",
	"Counter of stream dictionary names defined via the TCP endpoint.",
	"Counter of stream dictionary resets received via the TCP endpoint.",
	"Counter of stream protocol or transport failures via the TCP endpoint.",
}

// SetStreamPointOut installs an optional stream sink with a per-id opaque slot.
// The sink may cache a reference in *slot; resets clear all slots, and restart
// snapshots recreate them empty. It must not retain the slot pointer or modify
// name. The hook takes precedence over SetPointOut for stream input only.
// Existing connections adopt it at a frame boundary. Buffered receivers keep
// using their configured store. No cache implementation is required here.
func (rcv *TCP) SetStreamPointOut(out func(name []byte, value float64, timestamp int64, slot *any)) {
	if rcv.buffer == nil {
		rcv.streamPointOut.Store(&out)
	}
}

func (rcv *TCP) configureStream(o *Options) error {
	rcv.streamDisabled = o.Compression != ""
	rcv.streamOptions = codec.Options{MaxFrameSize: o.StreamMaxFrameSize, MaxNames: o.StreamMaxNames, DictionaryBytes: o.StreamDictionaryBytes}
	d, err := codec.NewDecoder(rcv.streamOptions)
	if err != nil {
		return err
	}
	d.Close()
	budget := o.StreamDictionaryTotalBytes
	if budget == 0 {
		budget = 3 << 30
	}
	perConnection := o.StreamDictionaryBytes
	if perConnection == 0 {
		perConnection = codec.DefaultDictionaryBytes
	}
	if budget < int64(perConnection) {
		return fmt.Errorf("stream-dictionary-total-bytes must cover one stream dictionary")
	}
	rcv.streamDictionaryBudget = budget
	return nil
}

func (rcv *TCP) streamFailure(conn net.Conn, err error) {
	if rcv.plannedClose(err) {
		return
	}
	rcv.streamCounters[streamDecodeErrors].Add(1)
	atomic.AddUint32(&rcv.errors, 1)
	rcv.logger.Error("stream decode failed", zap.String("peer", conn.RemoteAddr().String()), zap.Error(err))
}

// handleStream runs under handlePlain's connection lifecycle. Every accepted
// frame is validated before rows reach the same sink as plaintext points.
func (rcv *TCP) handleStream(conn net.Conn, r *bufio.Reader, rest *bytes.Reader, tracked *handoffConn) bool {
	rcv.streamSeen.Store(true)
	rcv.streamCounters[streamConnections].Add(1)
	var preamble [codec.PreambleSize]byte
	if n, err := io.ReadFull(r, preamble[:]); err != nil {
		if tracked.detaching() {
			return rcv.detach(tracked, r, rest, preamble[:n])
		}
		rcv.streamFailure(conn, err)
		return false
	}
	flags, err := codec.ParsePreamble(preamble[:])
	if err != nil {
		rcv.streamFailure(conn, err)
		return false
	}
	opts := rcv.streamOptions
	opts.Flags = flags
	reservation := int64(opts.DictionaryBytes)
	if reservation == 0 {
		reservation = codec.DefaultDictionaryBytes
	}
	budget := rcv.streamDictionaryBudget
	if budget == 0 {
		budget = 3 << 30
	}
	for {
		used := rcv.streamDictionaryReserved.Load()
		if reservation > budget-used {
			rcv.streamFailure(conn, fmt.Errorf("stream dictionary byte budget exhausted: reserved %d, connection %d, budget %d", used, reservation, budget))
			return false
		}
		if rcv.streamDictionaryReserved.CompareAndSwap(used, used+reservation) {
			break
		}
	}
	defer rcv.streamDictionaryReserved.Add(-reservation)
	d, err := codec.NewDecoder(opts)
	if err != nil {
		rcv.streamFailure(conn, err)
		return false
	}
	defer d.Close()
	heldNames, heldBytes := int64(0), int64(0)
	defer func() { rcv.streamRetainedNames.Add(-heldNames); rcv.streamRetainedBytes.Add(-heldBytes) }()
	if resolve := rcv.streamNameResolver.Load(); resolve != nil {
		d.SetNameResolver(*resolve)
	}
	var pointOut func([]byte, float64, int64)
	if out := rcv.pointOut.Load(); out != nil {
		pointOut = *out
	}
	var streamOut func([]byte, float64, int64, *any)
	if out := rcv.streamPointOut.Load(); out != nil {
		streamOut = *out
	}
	sinkReady := false
	capture := newCaptureBuffer(rcv.capture)
	if capture.out != nil {
		defer func() {
			if err := capture.flush(); err != nil {
				rcv.logger.Error("input capture failed", zap.Error(err))
			}
		}()
	}
	visit := func(_ uint32, name []byte, value float64, ts int64, slot *any) {
		if !sinkReady {
			if streamOut == nil {
				if out := rcv.streamPointOut.Load(); out != nil {
					streamOut = *out
				}
			}
			if pointOut == nil && streamOut == nil {
				if out := rcv.pointOut.Load(); out != nil {
					pointOut = *out
				}
			}
			sinkReady = true
		}
		switch {
		case capture.out != nil:
			capture.point(name, value, ts)
		case streamOut != nil:
			streamOut(name, value, ts, slot)
		case pointOut != nil:
			pointOut(name, value, ts)
		default:
			rcv.out(points.OnePoint(string(name), value, ts))
		}
	}
	for {
		if rest != nil && rest.Len() == 0 {
			rest.Reset(nil)
		}
		if tracked.detaching() {
			return rcv.detachStream(conn, r, rest, tracked, d, flags)
		}
		// Resolve after the read, at the first row, so a sink installed while
		// waiting for input is adopted. An installed stream hook stays local.
		sinkReady = streamOut != nil
		if resolve := rcv.streamNameResolver.Load(); resolve != nil {
			d.SetNameResolver(*resolve)
		}

		tracked.setReadDeadline(conn, time.Now().Add(2*time.Minute))
		info, err := d.ReadFrame(r, visit)
		if capture.out != nil {
			if capture.flush() != nil {
				return false
			}
		}
		if err != nil {
			if tracked.detaching() {
				return rcv.detachStream(conn, r, rest, tracked, d, flags)
			}
			if err != io.EOF { //nolint:errorlint // Only a direct EOF at a frame boundary is a clean shutdown.
				rcv.streamFailure(conn, err)
			}
			return false
		}
		names, bytes := int64(d.NameCount()), int64(d.RetainedBytes())
		rcv.streamRetainedNames.Add(names - heldNames)
		rcv.streamRetainedBytes.Add(bytes - heldBytes)
		heldNames, heldBytes = names, bytes
		atomic.AddUint32(&rcv.metricsReceived, info.Rows)
		rcv.streamCounters[streamFrames].Add(1)
		rcv.streamCounters[streamPayloadBytes].Add(uint64(info.PayloadBytes))
		rcv.streamCounters[streamNamesDefined].Add(uint64(info.Names))
		if info.Kind == codec.ResetFrame {
			rcv.streamCounters[streamResets].Add(1)
		}
	}
}

// detachStream serializes protocol state into ordinary wire input. ReadFrame
// never emits an incomplete frame; its consumed bytes follow the last committed
// state, before reader-buffered and inherited bytes. The successor autodetects
// this preamble and replays state frames without emitting points.
func (rcv *TCP) detachStream(conn net.Conn, r *bufio.Reader, rest *bytes.Reader, tracked *handoffConn, d *codec.Decoder, flags byte) bool {
	prefix, err := codec.AppendPreamble(nil, flags)
	if err == nil {
		prefix, err = d.Snapshot(prefix)
	}
	if err != nil {
		rcv.streamFailure(conn, err)
		leftover := len(prefix) + len(d.Pending()) + r.Buffered()
		if rest != nil {
			leftover += rest.Len()
		}
		if tracked.ahead != nil {
			leftover += len(tracked.ahead.pending(nil))
		}
		handoff.CloseInputConn(tracked.conn, leftover, rcv.logger, "snapshot_failed")
		return false
	}
	prefix = append(prefix, d.Pending()...)
	return rcv.detachOwned(tracked, r, rest, prefix)
}

func (rcv *TCP) streamStat(send helper.StatCallback) {
	if rcv.streamSeen.Load() && rcv.streamPromRetention[0] != nil {
		rcv.streamPromRetention[0].WithLabelValues().Set(float64(rcv.streamRetainedNames.Load()))
		rcv.streamPromRetention[1].WithLabelValues().Set(float64(rcv.streamRetainedBytes.Load()))
	}
	send("streamRetainedNames", float64(rcv.streamRetainedNames.Load()))
	send("streamRetainedBytes", float64(rcv.streamRetainedBytes.Load()))
	for i, name := range streamStatNames {
		n := float64(rcv.streamCounters[i].Swap(0))
		send(name, n)
		if rcv.promStreamCounters[i] != nil {
			if n != 0 {
				rcv.promStreamCounters[i].WithLabelValues().Add(n)
			}
		}
	}
}
func (rcv *TCP) initStreamPrometheus(reg prometheus.Registerer) {
	for i, spec := range []struct{ name, help string }{
		{"stream_dictionary_retained_names_tcp", "Wire ids retained by live stream connections."},
		{"stream_dictionary_retained_bytes_tcp", "Allocated live stream id-table capacity, excluding shared cache series."},
	} {
		rcv.streamPromRetention[i] = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: spec.name, Help: spec.help}, nil)
		reg.MustRegister(rcv.streamPromRetention[i])
	}

	for i, name := range streamPromNames {
		// Instantiate a series only on use, preserving the metrics exposed by
		// existing plaintext/zstd-only receivers.
		c := prometheus.NewCounterVec(prometheus.CounterOpts{Name: name, Help: streamPromHelp[i]}, nil)
		rcv.promStreamCounters[i] = c
		reg.MustRegister(c)
	}
}

// SetStreamNameResolver binds wire ids to store-wide immutable series identities.
// Definitions and re-resolution may consult the store index; hot rows do not.
func (rcv *TCP) SetStreamNameResolver(resolve func([]byte) any) {
	if rcv.buffer == nil {
		rcv.streamNameResolver.Store(&resolve)
	}
}
