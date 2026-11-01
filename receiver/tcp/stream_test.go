package tcp

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"reflect"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/points"
	"github.com/go-graphite/go-carbon/receiver/stream/codec"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func streamWire(t *testing.T, flags byte) []byte {
	t.Helper()
	e, err := codec.NewEncoder(codec.Options{Flags: flags, MaxNames: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	b, err := codec.AppendPreamble(nil, flags)
	if err != nil {
		t.Fatal(err)
	}
	for i, name := range []string{"example.a", "example.b", "example.a", "example.c"} {
		if err := e.Add([]byte(name), float64(i)+0.25, 1700000000+int64(i)); err != nil {
			t.Fatal(err)
		}
	}
	b, err = e.Flush(b)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func runStream(t *testing.T, wire []byte, configure func(*TCP)) ([]*points.Points, map[string]float64, int) {
	t.Helper()
	server, client := net.Pipe()
	defer client.Close()
	var logs bytes.Buffer
	core := zapcore.NewCore(zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig()), zapcore.AddSync(&logs), zap.ErrorLevel)
	var got []*points.Points
	r := &TCP{logger: zap.New(core), decompressor: newDecompressor(""), out: func(p *points.Points) { got = append(got, p) }}
	if configure != nil {
		configure(r)
	}
	r.Start()
	defer r.Stop()
	done := make(chan struct{})
	go func() { r.HandleConnection(server); close(done) }()
	_ = client.SetWriteDeadline(time.Now().Add(3 * time.Second))
	_, _ = client.Write(wire)
	_ = client.Close()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("handler did not finish")
	}
	stats := map[string]float64{}
	r.Stat(func(n string, v float64) { stats[n] = v })
	r.Stat(func(n string, v float64) {
		if v != 0 {
			t.Errorf("stat %s not reset: %g", n, v)
		}
	})
	return got, stats, bytes.Count(logs.Bytes(), []byte{'\n'})
}

func TestStreamTCP(t *testing.T) {
	for _, flags := range []byte{0, codec.FlagZstd} {
		wire := streamWire(t, flags)
		var want []*points.Points
		d, err := codec.NewDecoder(codec.Options{Flags: flags})
		if err != nil {
			t.Fatal(err)
		}
		defer d.Close()
		input := bytes.NewReader(wire[codec.PreambleSize:])
		var frames, names, payload, resets float64
		for input.Len() > 0 {
			info, err := d.ReadFrame(input, func(_ uint32, n []byte, v float64, ts int64, _ *any) {
				want = append(want, points.OnePoint(string(n), v, ts))
			})
			if err != nil {
				t.Fatal(err)
			}
			frames++
			names += float64(info.Names)
			payload += float64(info.PayloadBytes)
			if info.Kind == codec.ResetFrame {
				resets++
			}
		}
		for _, fast := range []bool{false, true} {
			var fastGot []*points.Points
			got, stats, logs := runStream(t, wire, func(r *TCP) {
				if fast {
					r.SetPointOut(func(n []byte, v float64, ts int64) { fastGot = append(fastGot, points.OnePoint(string(n), v, ts)) })
				}
			})
			if fast {
				if len(got) != 0 {
					t.Fatal("fast sink also called fallback")
				}
				got = fastGot
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("points differ: got %v want %v", got, want)
			}
			for key, want := range map[string]float64{"metricsReceived": 4, "streamConnections": 1, "streamFrames": frames, "streamPayloadBytes": payload, "streamNamesDefined": names, "streamResets": resets, "streamDecodeErrors": 0} {
				if stats[key] != want {
					t.Errorf("%s=%g want %g", key, stats[key], want)
				}
			}
			if logs != 0 {
				t.Fatal("unexpected ERROR log")
			}
		}
	}
}

func TestStreamFailuresAreLoud(t *testing.T) {
	valid := streamWire(t, 0)
	for _, tc := range []struct {
		name  string
		wire  []byte
		limit int
	}{
		{name: "version", wire: func() []byte { b := bytes.Clone(valid); b[len(codec.Magic)]++; return b }()},
		{name: "flags", wire: func() []byte { b := bytes.Clone(valid); b[codec.PreambleSize-1] = 128; return b }()},
		{name: "magic", wire: []byte("\x00bad magic")},
		{name: "crc", wire: func() []byte { b := bytes.Clone(valid); b[codec.PreambleSize+codec.HeaderSize] ^= 1; return b }()},
		{name: "oversized", limit: 128, wire: func() []byte {
			b := bytes.Clone(valid[:codec.PreambleSize+codec.HeaderSize])
			binary.BigEndian.PutUint32(b[codec.PreambleSize+4:], 1024)
			return b
		}()},
		{name: "truncated_header", wire: valid[:codec.PreambleSize+4]},
		{name: "truncated_payload", wire: valid[:codec.PreambleSize+codec.HeaderSize+2]},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, stats, logs := runStream(t, tc.wire, func(r *TCP) { r.streamOptions.MaxFrameSize = tc.limit })
			if len(got) != 0 || stats["streamDecodeErrors"] != 1 || stats["errors"] != 1 || logs != 1 {
				t.Fatalf("failure not loud: got %d points, stats=%v, logs=%d", len(got), stats, logs)
			}
		})
	}
}

func TestStreamCompressionDisablesDetection(t *testing.T) {
	for _, compression := range []string{"gzip", "snappy"} {
		wire := streamWire(t, 0)
		// The helper compresses the wire before sending; it remains legacy plaintext
		// after decompression, even though the first decompressed byte is NUL.
		got := handlePlainConnectionConfigured(t, compression, wire, func(r *TCP) { r.streamDisabled = true })
		if got.metricsReceived != 0 {
			t.Fatal("compressed connection autodetected stream")
		}
	}
}

func TestStreamPrometheus(t *testing.T) {
	reg := prometheus.NewRegistry()
	r := &TCP{}
	r.InitPrometheus(reg)
	for i := range r.streamCounters {
		r.streamCounters[i].Add(uint64(i + 1))
	}
	r.Stat(func(string, float64) {})
	families, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	counts := map[string]float64{}
	for _, f := range families {
		counts[f.GetName()] = f.Metric[0].GetCounter().GetValue()
	}
	for i, name := range streamPromNames {
		if counts[name] != float64(i+1) {
			t.Fatalf("%s=%g", name, counts[name])
		}
	}
}

func TestStreamFragmentedTCP(t *testing.T) {
	options := NewOptions()
	options.Listen = "127.0.0.1:0"
	got := make(chan *points.Points, 16)
	r, err := newTCP("tcp", options, func(p *points.Points) { got <- p })
	if err != nil {
		t.Fatal(err)
	}
	defer r.Stop()
	conn, err := net.Dial("tcp", r.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetWriteDeadline(time.Now().Add(3 * time.Second))
	for _, b := range streamWire(t, codec.FlagZstd) {
		if _, err := conn.Write([]byte{b}); err != nil {
			t.Fatal(err)
		}
	}
	for range 4 {
		select {
		case <-got:
		case <-time.After(3 * time.Second):
			t.Fatal("missing point")
		}
	}
}

func TestStreamEmptyConnection(t *testing.T) {
	wire, _ := codec.AppendPreamble(nil, 0)
	_, stats, logs := runStream(t, wire, nil)
	if stats["streamDecodeErrors"] != 0 || logs != 0 {
		t.Fatal("clean EOF treated as failure")
	}
	// Ensure the reader's sentinel remains a clean frame-boundary EOF.
	d, err := codec.NewDecoder(codec.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	if _, err := d.ReadFrame(bytes.NewReader(nil), nil); !errors.Is(err, io.EOF) {
		t.Fatal(err)
	}
}

func TestStreamPointOutSlots(t *testing.T) {
	type reference struct{ name string }
	created, reused := 0, 0
	var got []*points.Points
	fallback := 0
	_, stats, logs := runStream(t, streamWire(t, 0), func(r *TCP) {
		r.SetPointOut(func([]byte, float64, int64) { fallback++ })
		r.SetStreamPointOut(func(name []byte, value float64, ts int64, slot *any) {
			if *slot == nil {
				*slot = &reference{name: string(name)}
				created++
			} else {
				reused++
				if (*slot).(*reference).name != string(name) {
					t.Fatal("slot reused by another name")
				}
			}
			got = append(got, points.OnePoint(string(name), value, ts))
		})
	})
	if created != 3 || reused != 1 || fallback != 0 || len(got) != 4 || stats["metricsReceived"] != 4 || logs != 0 {
		t.Fatalf("created=%d reused=%d fallback=%d points=%d stats=%v logs=%d", created, reused, fallback, len(got), stats, logs)
	}
	buffered := &TCP{buffer: make(chan *points.Points, 1)}
	buffered.SetStreamPointOut(func([]byte, float64, int64, *any) {})
	if buffered.streamPointOut.Load() != nil {
		t.Fatal("buffered receiver adopted unbuffered hook")
	}
}

func TestStreamPointOutAdoptedDuringRead(t *testing.T) {
	options := NewOptions()
	options.Listen = "127.0.0.1:0"
	slow, fast := make(chan struct{}, 1), make(chan struct{}, 1)
	r, err := newTCP("tcp", options, func(*points.Points) { slow <- struct{}{} })
	if err != nil {
		t.Fatal(err)
	}
	defer r.Stop()
	conn, err := net.Dial("tcp", r.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	e, err := codec.NewEncoder(codec.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	b, _ := codec.AppendPreamble(nil, 0)
	for i := range 2 {
		if i == 1 {
			r.SetStreamPointOut(func([]byte, float64, int64, *any) { fast <- struct{}{} })
		}
		if err := e.Add([]byte("example.metric"), float64(i), 1700000000+int64(i)); err != nil {
			t.Fatal(err)
		}
		b, err = e.Flush(b)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := conn.Write(b); err != nil {
			t.Fatal(err)
		}
		b = b[:0]
		expected := slow
		if i == 1 {
			expected = fast
		}
		select {
		case <-expected:
		case <-time.After(3 * time.Second):
			t.Fatal("did not adopt sink")
		}
	}
}
