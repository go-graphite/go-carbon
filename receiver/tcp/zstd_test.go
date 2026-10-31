package tcp

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/points"
	"github.com/klauspost/compress/gzip"
	"github.com/klauspost/compress/zstd"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func encodeFrames(t testing.TB, plain []byte, size int) []byte {
	t.Helper()
	encoder, err := zstd.NewWriter(nil, zstd.WithEncoderConcurrency(1))
	if err != nil {
		t.Fatal(err)
	}
	defer encoder.Close()
	var wire []byte
	for len(plain) > 0 {
		n := min(size, len(plain))
		wire = encoder.EncodeAll(plain[:n], wire)
		plain = plain[n:]
	}
	return wire
}

func TestZstdPlaintextParity(t *testing.T) {
	plain := []byte("example.a 1.25 1700000000\nexample.b -42.5 1700000001\n" + strings.Repeat("x", 70000) + " 3 1700000002\n")
	want := handlePlainConnection(t, "", plain)
	for _, size := range []int{1, 59, 1 << 20} {
		for _, compression := range []string{"", "zstd"} {
			t.Run(fmt.Sprintf("%s/%d", compression, size), func(t *testing.T) {
				got := handlePlainConnectionConfigured(t, "", encodeFrames(t, plain, size), func(r *TCP) { r.compression = compression })
				if !reflect.DeepEqual(got, want) {
					t.Fatal("compressed and plaintext points/counters differ")
				}
			})
		}
	}
}

func newFrameReader(t testing.TB, wire []byte, limit int) *zstdFrames {
	t.Helper()
	r := &TCP{compression: "zstd", zstdMaxFrameSize: limit}
	z, err := r.dispatchFirstBytes(bufio.NewReader(bytes.NewReader(wire)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(z.decoder.Close)
	return z
}

func TestZstdFrameValidation(t *testing.T) {
	valid := encodeFrames(t, []byte("example.count 1 1700000000\n"), 1024)
	badCRC := append([]byte(nil), valid...)
	badCRC[len(badCRC)-1] ^= 1
	largeContent := appendRawZstdFrame(nil, make([]byte, 2048))
	largeWindow := []byte{0x28, 0xb5, 0x2f, 0xfd, 0, 0x50, 1, 0, 0}
	// No content-size header; a small window must not allow oversized output.
	unknown := append([]byte{0x28, 0xb5, 0x2f, 0xfd, 0, 0}, largeContent[9:]...)
	tooManyBlocks := []byte{0x28, 0xb5, 0x2f, 0xfd, 0, 0}
	tooManyBlocks = append(tooManyBlocks, make([]byte, 4096)...)
	badBlock := []byte{0x28, 0xb5, 0x2f, 0xfd, 0x20, 0, 7, 0, 0}
	skip := binary.LittleEndian.AppendUint32([]byte{0x50, 0x2a, 0x4d, 0x18}, 5000)
	for name, wire := range map[string][]byte{"checksum": badCRC, "content": largeContent, "window": largeWindow, "unknown-size": unknown, "compressed-bound": tooManyBlocks, "block": badBlock, "skip-bound": skip, "magic": []byte("xxxx")} {
		t.Run(name, func(t *testing.T) {
			z := newFrameReader(t, wire, 1024)
			if _, err := io.ReadAll(z); err == nil {
				t.Fatal("accepted invalid or oversized frame")
			}
		})
	}
	for n := 1; n < len(valid); n++ {
		z := newFrameReader(t, valid[:n], 1024)
		if _, err := io.ReadAll(z); err == nil {
			t.Fatalf("accepted frame truncated at %d", n)
		}
	}
	// Skippable frames may appear between independent data frames.
	wire := append([]byte(nil), valid...)
	wire = binary.LittleEndian.AppendUint32(append(wire, 0x5f, 0x2a, 0x4d, 0x18), 3)
	wire = append(wire, 1, 2, 3)
	wire = append(wire, valid...)
	got, err := io.ReadAll(newFrameReader(t, wire, 1024))
	if err != nil || string(got) != "example.count 1 1700000000\nexample.count 1 1700000000\n" {
		t.Fatalf("skippable/concat: %v", err)
	}
}

func TestZstdFailureIsLoud(t *testing.T) {
	var logs bytes.Buffer
	core := zapcore.NewCore(zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig()), zapcore.AddSync(&logs), zap.ErrorLevel)
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
	_, _ = c.Write([]byte{0x28, 0xb5, 0x2f, 0xfd, 0xff})
	_ = c.Close()
	deadline := time.Now().Add(3 * time.Second)
	for atomic.LoadUint64(&r.zstdStats.decodeErrors) == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	r.Stop()
	if atomic.LoadUint64(&r.zstdStats.decodeErrors) != 1 || strings.Count(logs.String(), "zstd input failed") != 1 {
		t.Fatal("missing error counter or ERROR log")
	}
	reg := prometheus.NewRegistry()
	r.InitPrometheus(reg)
	r.Stat(func(string, float64) {})
	families, err := reg.Gather()
	if err != nil || len(families) != 6 {
		t.Fatalf("metrics: %d %v", len(families), err)
	}
}

func TestZstdHandoff(t *testing.T) {
	for _, mode := range []string{"decoded-tail", "partial-frame", "split-line", "idle"} {
		t.Run(mode, func(t *testing.T) {
			before, after := sequenceStore{wake: make(chan struct{}, 1)}, sequenceStore{wake: make(chan struct{}, 1)}
			blocked, release := make(chan struct{}), make(chan struct{})
			options := NewOptions()
			options.Listen = "127.0.0.1:0"
			old, err := newTCP("tcp", options, func(p *points.Points) {
				before.add(p)
				if mode == "decoded-tail" && p.Data[0].Value == 0 {
					close(blocked)
					<-release
				}
			})
			if err != nil {
				t.Fatal(err)
			}
			defer old.Stop()
			addr := old.Addr().String()
			c, err := net.Dial("tcp", addr)
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			first := []byte("c0.metric 0 1700000000\n")
			rest := []byte("c0.metric 1.25 1700000001\nc0.metric 2.5 1700000002\n")
			var prefix, suffix []byte
			switch mode {
			case "decoded-tail":
				// More than one line-reader buffer remains in the decoded frame.
				all := append(append([]byte(nil), first...), []byte("c0."+strings.Repeat("x", 150000)+" 1.25 1700000001\nc0.metric 2.5 1700000002\n")...)
				prefix = encodeFrames(t, all, len(all))
			case "partial-frame":
				frame := encodeFrames(t, rest, len(rest))
				prefix = append(encodeFrames(t, first, len(first)), frame[:len(frame)/2]...)
				suffix = frame[len(frame)/2:]
			case "split-line":
				prefix = encodeFrames(t, append(first, rest[:9]...), 1024)
				suffix = encodeFrames(t, rest[9:], 1024)
			default:
				prefix = encodeFrames(t, first, 1024)
				suffix = encodeFrames(t, rest, 1024)
			}
			if _, err = c.Write(prefix); err != nil {
				t.Fatal(err)
			}
			select {
			case <-before.wake:
			case <-time.After(3 * time.Second):
				t.Fatal("first point missing")
			}
			if mode == "decoded-tail" {
				<-blocked
			}
			hand := make(chan *Handoff, 1)
			errs := make(chan error, 1)
			go func() { h, e := old.Detach(); hand <- h; errs <- e }()
			if mode == "decoded-tail" {
				for {
					old.handoff.mu.Lock()
					detaching := old.handoff.detaching
					old.handoff.mu.Unlock()
					if detaching {
						break
					}
					time.Sleep(time.Millisecond)
				}
				close(release)
			}
			var h *Handoff
			select {
			case h = <-hand:
			case <-time.After(3 * time.Second):
				t.Fatal("detach blocked")
			}
			if err = <-errs; err != nil {
				t.Fatal(err)
			}
			if len(h.Conns) != 1 {
				t.Fatalf("lost connection: %d", len(h.Conns))
			}
			next, err := NewInherited("tcp", &Options{Listen: addr, Enabled: true}, after.add, h)
			if err != nil {
				t.Fatal(err)
			}
			defer next.Stop()
			if _, err = c.Write(suffix); err != nil {
				t.Fatal(err)
			}
			deadline := time.Now().Add(3 * time.Second)
			for before.count()+after.count() != 3 && time.Now().Before(deadline) {
				time.Sleep(time.Millisecond)
			}
			if before.count()+after.count() != 3 {
				t.Fatalf("before %d after %d", before.count(), after.count())
			}
			// The successor must classify the re-framed tail as zstd.
			if atomic.LoadUint64(&next.zstdStats.connections) != 1 {
				t.Fatal("successor did not autodetect zstd")
			}
			got := append(append([]int(nil), before.seqs["c0"]...), after.seqs["c0"]...)
			if !reflect.DeepEqual(got, []int{0, 1, 2}) {
				t.Fatalf("loss/duplicate/order: %v", got)
			}
		})
	}
}

// The private sample is opt-in and never printed. Timing includes frame
// splitting and DecodeAll, and reports throughput per decompressed byte.
func BenchmarkZstdSampleDecode(b *testing.B) {
	path := os.Getenv("ZSTD_SAMPLE")
	if path == "" {
		b.Skip("set ZSTD_SAMPLE to a local gzip sample")
	}
	f, err := os.Open(path)
	if err != nil {
		b.Fatal(err)
	}
	defer f.Close()
	gz, err := gzip.NewReader(f)
	if err != nil {
		b.Fatal(err)
	}
	defer gz.Close()
	plain, err := io.ReadAll(gz)
	if err != nil {
		b.Fatal(err)
	}
	wire := encodeFrames(b, plain, 1<<20)
	z := newFrameReader(b, wire, defaultZstdMaxFrameSize)
	input := bytes.NewReader(wire)
	// Verify the real sample without retaining a second full decoded copy.
	check := &sampleByteOracle{plain: plain}
	if n, err := io.Copy(check, z); err != nil || n != int64(len(plain)) {
		b.Fatal("sample byte parity failed", err)
	}
	input.Reset(wire)
	z.wire.Reset(input)
	b.SetBytes(int64(len(plain)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		input.Reset(wire)
		z.wire.Reset(input)
		n, err := io.Copy(io.Discard, z)
		if err != nil || n != int64(len(plain)) {
			b.Fatal("decode failed", err)
		}
	}
}

type sampleByteOracle struct {
	plain  []byte
	offset int
}

func (c *sampleByteOracle) Write(p []byte) (int, error) {
	if len(p) > len(c.plain)-c.offset || !bytes.Equal(p, c.plain[c.offset:c.offset+len(p)]) {
		return 0, fmt.Errorf("sample output differs")
	}
	c.offset += len(p)
	return len(p), nil
}
