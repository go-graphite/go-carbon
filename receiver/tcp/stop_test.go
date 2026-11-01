package tcp

import (
	"bytes"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/points"
	"github.com/go-graphite/go-carbon/receiver"
	"github.com/go-graphite/go-carbon/receiver/stream/codec"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func TestPlannedConnectionClose(t *testing.T) {
	for _, protocol := range []string{"plaintext", "zstd", "stream", "stream-preamble"} {
		t.Run(protocol, func(t *testing.T) {
			var logs bytes.Buffer
			core := zapcore.NewCore(zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig()), zapcore.Lock(zapcore.AddSync(&logs)), zap.InfoLevel)
			r, err := newTCP("tcp", &Options{Listen: "127.0.0.1:0", Enabled: true}, func(*points.Points) {})
			if err != nil {
				t.Fatal(err)
			}
			defer r.Stop()
			r.logger = zap.New(core)
			c, err := net.Dial("tcp", r.Addr().String())
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			var wire []byte
			switch protocol {
			case "plaintext":
				wire = []byte("example.a 1 1700000000\npartial")
			case "zstd":
				wire = encodeFrames(t, []byte("example.a 1 1700000000\n"), 1024)
				wire = append(wire, 0x28, 0xb5, 0x2f, 0xfd)
			case "stream":
				wire = streamWire(t, codec.FlagZstd)
			case "stream-preamble":
				wire = []byte{0}
			}
			if _, err := c.Write(wire); err != nil {
				t.Fatal(err)
			}
			deadline := time.Now().Add(5 * time.Second)
			for atomic.LoadInt32(&r.active) == 0 {
				if time.Now().After(deadline) {
					t.Fatal("connection did not start")
				}
				time.Sleep(time.Millisecond)
			}
			r.Stop()
			if strings.Contains(logs.String(), `"level":"error"`) {
				t.Fatal("planned close logged at ERROR", logs.String())
			}
			if strings.Count(logs.String(), "input connection closed during shutdown") != 1 {
				t.Fatal("expected one INFO for the locally closed connection", logs.String())
			}
			if r.errors != 0 || r.zstdStats.decodeErrors != 0 || r.streamCounters[streamDecodeErrors].Load() != 0 {
				t.Fatal("planned close counted as a read/decode error")
			}
		})
	}
}

func TestPlannedCloseOnlySuppressesLocalShutdown(t *testing.T) {
	for _, stopping := range []bool{false, true} {
		for _, readErr := range []error{net.ErrClosed, fmt.Errorf("read: %w", net.ErrClosed), syscall.ECONNRESET, errors.New("invalid frame")} {
			t.Run(fmt.Sprintf("%v/%v", stopping, readErr), func(t *testing.T) {
				var logs bytes.Buffer
				core := zapcore.NewCore(zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig()), zapcore.Lock(zapcore.AddSync(&logs)), zap.InfoLevel)
				r := &TCP{logger: zap.New(core)}
				r.stopping.Store(stopping)
				c, peer := net.Pipe()
				defer c.Close()
				defer peer.Close()
				r.streamFailure(c, readErr)
				want := stopping && errors.Is(readErr, net.ErrClosed)
				if (strings.Count(logs.String(), `"level":"info"`) == 1) != want ||
					(strings.Count(logs.String(), `"level":"error"`) == 1) == want ||
					(r.streamCounters[streamDecodeErrors].Load() == 0) != want {
					t.Fatal("incorrect error classification", logs.String())
				}
			})
		}
	}
}

func TestStopTCP(t *testing.T) {
	addr, err := net.ResolveTCPAddr("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 10; i++ {
		r, err := receiver.New("tcp", map[string]interface{}{
			"protocol": "tcp",
			"listen":   addr.String(),
		},
			nil,
		)
		if err != nil {
			t.Fatal(err)
		}
		addr = r.(*TCP).Addr().(*net.TCPAddr) // listen same port in next iteration
		r.Stop()
	}
}

func TestStopPickle(t *testing.T) {
	addr, err := net.ResolveTCPAddr("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 10; i++ {
		r, err := receiver.New("pickle", map[string]interface{}{
			"protocol": "pickle",
			"listen":   addr.String(),
		},
			nil,
		)
		if err != nil {
			t.Fatal(err)
		}
		addr = r.(*TCP).Addr().(*net.TCPAddr) // listen same port in next iteration
		r.Stop()
	}
}

func TestStopConnectedTCP(t *testing.T) {
	test := newTCPTestCase(t, "tcp")
	defer test.Finish()

	ch := test.rcvChan
	test.Send("hello.world 42.15 1422698155\n")
	select {
	case msg := <-ch:
		test.Eq(msg, points.OnePoint("hello.world", 42.15, 1422698155))
	case <-time.After(5 * time.Second):
		t.Fatalf("Message #0 not received")
	}

	test.receiver.Stop()
	test.receiver = nil
	test.WaitClosed()
	_, err := test.conn.Write([]byte("metric.name -72.11 1422698155\n"))
	if err != nil && !errors.Is(err, syscall.EPIPE) && !errors.Is(err, syscall.ECONNRESET) {
		t.Fatal(err)
	}

	select {
	case <-ch:
		t.Fatalf("Message #0 received")
	default:
	}
}
