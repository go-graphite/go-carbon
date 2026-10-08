package tcp

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"reflect"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/points"
	"github.com/go-graphite/go-carbon/receiver"
	"github.com/go-graphite/go-carbon/receiver/parse"
	"github.com/klauspost/compress/snappy"
	"go.uber.org/zap"
)

type tcpTestCase struct {
	*testing.T
	receiver *TCP
	conn     net.Conn
	rcvChan  chan *points.Points
}

func TestMain(m *testing.M) {
	Register()
	os.Exit(m.Run())
}

func newTCPTestCase(t *testing.T, protocol string) *tcpTestCase {
	test := &tcpTestCase{
		T: t,
	}

	addr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	if err != nil {
		t.Fatal(err)
	}

	test.rcvChan = make(chan *points.Points, 128)

	r, err := receiver.New(protocol, map[string]interface{}{
		"protocol": protocol,
		"listen":   addr.String(),
	},
		func(p *points.Points) {
			test.rcvChan <- p
		},
	)

	if err != nil {
		t.Fatal(err)
	}

	test.receiver = r.(*TCP)

	test.conn, err = net.Dial("tcp", test.receiver.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	// defer conn.Close()
	return test
}

func (test *tcpTestCase) Finish() {
	if test.conn != nil {
		test.conn.Close()
		test.conn = nil
	}
	if test.receiver != nil {
		test.receiver.Stop()
		test.receiver = nil
	}
}

func (test *tcpTestCase) Send(text string) {
	if _, err := test.conn.Write([]byte(text)); err != nil {
		test.Fatal(err)
	}
}

func (test *tcpTestCase) Eq(a *points.Points, b *points.Points) {
	if !a.Eq(b) {
		test.Fatalf("%#v != %#v", a, b)
	}
}

func TestTCP1(t *testing.T) {
	test := newTCPTestCase(t, "tcp")
	defer test.Finish()

	test.Send("hello.world 42.15 1422698155\n")

	time.Sleep(10 * time.Millisecond)

	select {
	case msg := <-test.rcvChan:
		test.Eq(msg, points.OnePoint("hello.world", 42.15, 1422698155))
	default:
		t.Fatalf("Message #0 not received")
	}
}

func TestTCP2(t *testing.T) {
	test := newTCPTestCase(t, "tcp")
	defer test.Finish()

	test.Send("hello.world 42.15 1422698155\nmetric.name -72.11 1422698155\n")

	time.Sleep(10 * time.Millisecond)

	select {
	case msg := <-test.rcvChan:
		test.Eq(msg, points.OnePoint("hello.world", 42.15, 1422698155))
	default:
		t.Fatalf("Message #0 not received")
	}

	select {
	case msg := <-test.rcvChan:
		test.Eq(msg, points.OnePoint("metric.name", -72.11, 1422698155))
	default:
		t.Fatalf("Message #1 not received")
	}
}

func TestTCPIssue176(t *testing.T) {
	test := newTCPTestCase(t, "tcp")
	defer test.Finish()

	test.Send("hello.world 1.096378e+06 1422698155\nmetric.name 1.096378e+06 1422698155\n")

	time.Sleep(10 * time.Millisecond)

	select {
	case msg := <-test.rcvChan:
		test.Eq(msg, points.OnePoint("hello.world", 1096378.0, 1422698155))
	default:
		t.Fatalf("Message #0 not received")
	}

	select {
	case msg := <-test.rcvChan:
		test.Eq(msg, points.OnePoint("metric.name", 1096378.0, 1422698155))
	default:
		t.Fatalf("Message #1 not received")
	}
}

func TestReadPlainLineMatchesReadBytes(t *testing.T) {
	errInjected := errors.New("injected read error")
	errWrappedBufferFull := fmt.Errorf("wrapped buffer error: %w", bufio.ErrBufferFull)
	boundary := strings.Repeat("a", 4095) + "\n"
	overSized := strings.Repeat("b", 8192) + "\n"
	tests := []struct {
		name     string
		data     string
		chunk    int
		terminal error
	}{
		{name: "short", data: "metric.one 1 1\n"},
		{name: "split", data: "metric.one 1 1\nmetric.two 2 2\n", chunk: 3},
		{name: "empty", data: "\n"},
		{name: "malformed", data: "not a graphite line\n"},
		{name: "boundary_4096", data: boundary},
		{name: "oversized", data: overSized},
		{name: "post_oversized", data: overSized + "metric.after 3 3\n", chunk: 37},
		{name: "unterminated_eof", data: "metric.unfinished 1 1"},
		{name: "unterminated_error", data: "metric.unfinished 1 1", chunk: 5, terminal: errInjected},
		{name: "unterminated_wrapped_buffer_error", data: "metric.unfinished 1 1", chunk: 5, terminal: errWrappedBufferFull},
	}
	for _, size := range []int{16, 4096, 8192} {
		for _, tt := range tests {
			t.Run(tt.name+"/buffer_"+strconv.Itoa(size), func(t *testing.T) {
				makeReader := func() io.Reader {
					return &chunkedReader{data: []byte(tt.data), chunk: tt.chunk, terminal: tt.terminal}
				}
				got := collectPlainLines(readPlainLine, bufio.NewReaderSize(makeReader(), size))
				want := collectPlainLines(func(reader *bufio.Reader) ([]byte, error) {
					return reader.ReadBytes('\n')
				}, bufio.NewReaderSize(makeReader(), size))
				if !reflect.DeepEqual(got, want) {
					t.Fatalf("read results = %#v, want %#v", got, want)
				}
			})
		}
	}
}

func TestHandleConnectionPlainTextMatchesReference(t *testing.T) {
	firstMetric := "metric.retained.after.refill"
	oversizedMetric := strings.Repeat("oversized.metric.", 320)
	payload := firstMetric + " 1.5 10\n" +
		"not a graphite line\n" +
		oversizedMetric + " 2.5 20\n" +
		"metric.after.oversized 3.5 30\n" +
		"metric.unterminated 4.5 40"
	want := referencePlainPoints([]byte(payload))

	for _, compression := range []string{"", "gzip", "snappy"} {
		t.Run(compressionOrIdentity(compression), func(t *testing.T) {
			got := handlePlainConnection(t, compression, []byte(payload))
			if !reflect.DeepEqual(got.points, want) {
				t.Fatalf("points = %#v, want %#v", got.points, want)
			}
			if got.points[0].Metric != firstMetric {
				t.Fatalf("first metric mutated after reader refill: %q", got.points[0].Metric)
			}
			if got.metricsReceived != uint32(len(want)) || got.errors != 1 || got.active != 0 {
				t.Fatalf("counters = received:%d errors:%d active:%d", got.metricsReceived, got.errors, got.active)
			}
		})
	}
}

func BenchmarkPlainLineReadAndParse(b *testing.B) {
	fixtures := [][]byte{
		[]byte("service.api.requests.count 1 1700000000\n"),
		[]byte("service.api.latency.p99 12.345 1700000001\n"),
		[]byte("booking.graphite.receiver.tcp.metrics.received 42 1700000002\n"),
	}
	for _, reader := range []struct {
		name string
		read func(*bufio.Reader) ([]byte, error)
	}{
		{name: "read_bytes", read: func(reader *bufio.Reader) ([]byte, error) { return reader.ReadBytes('\n') }},
		{name: "read_slice", read: readPlainLine},
	} {
		b.Run(reader.name, func(b *testing.B) {
			input := bytes.NewReader(nil)
			buffered := bufio.NewReaderSize(input, 4096)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				input.Reset(fixtures[i%len(fixtures)])
				buffered.Reset(input)
				line, err := reader.read(buffered)
				if err != nil {
					b.Fatal(err)
				}
				name, value, timestamp, err := parse.PlainLine(line)
				if err != nil {
					b.Fatal(err)
				}
				tcpBenchmarkPoint = points.OnePoint(string(name), value, timestamp)
			}
		})
	}
}

type lineReader func(*bufio.Reader) ([]byte, error)

type lineResult struct {
	line string
	err  error
}

type chunkedReader struct {
	data     []byte
	chunk    int
	terminal error
}

func (r *chunkedReader) Read(p []byte) (int, error) {
	if len(r.data) == 0 {
		if r.terminal != nil {
			return 0, r.terminal
		}
		return 0, io.EOF
	}
	n := len(r.data)
	if r.chunk > 0 && n > r.chunk {
		n = r.chunk
	}
	if n > len(p) {
		n = len(p)
	}
	copy(p, r.data[:n])
	r.data = r.data[n:]
	return n, nil
}

func collectPlainLines(read lineReader, reader *bufio.Reader) []lineResult {
	var results []lineResult
	for {
		line, err := read(reader)
		results = append(results, lineResult{line: string(line), err: err})
		if err != nil {
			return results
		}
	}
}

func referencePlainPoints(payload []byte) []*points.Points {
	reader := bufio.NewReader(bytes.NewReader(payload))
	var result []*points.Points
	for {
		line, err := reader.ReadBytes('\n')
		if err != nil {
			return result
		}
		if len(line) > 0 {
			name, value, timestamp, parseErr := parse.PlainLine(line)
			if parseErr == nil {
				result = append(result, points.OnePoint(string(name), value, timestamp))
			}
		}
	}
}

type handledPlainConnection struct {
	points          []*points.Points
	metricsReceived uint32
	errors          uint32
	active          int32
}

func handlePlainConnection(t *testing.T, compression string, payload []byte) handledPlainConnection {
	t.Helper()
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()
	var result []*points.Points
	rcv := &TCP{
		out:          func(p *points.Points) { result = append(result, p) },
		logger:       zap.NewNop(),
		decompressor: newDecompressor(compression),
	}
	rcv.Start()
	defer rcv.Stop()
	done := make(chan struct{})
	go func() {
		rcv.HandleConnection(server)
		close(done)
	}()
	if err := client.SetWriteDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}

	switch compression {
	case "gzip":
		writer := gzip.NewWriter(client)
		if _, err := writer.Write(payload); err != nil {
			t.Fatal(err)
		}
		if err := writer.Close(); err != nil {
			t.Fatal(err)
		}
	case "snappy":
		writer := snappy.NewBufferedWriter(client)
		if _, err := writer.Write(payload); err != nil {
			t.Fatal(err)
		}
		if err := writer.Close(); err != nil {
			t.Fatal(err)
		}
	default:
		if _, err := client.Write(payload); err != nil {
			t.Fatal(err)
		}
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("HandleConnection did not finish")
	}
	return handledPlainConnection{
		points:          result,
		metricsReceived: atomic.LoadUint32(&rcv.metricsReceived),
		errors:          atomic.LoadUint32(&rcv.errors),
		active:          atomic.LoadInt32(&rcv.active),
	}
}

func compressionOrIdentity(compression string) string {
	if compression == "" {
		return "identity"
	}
	return compression
}

var tcpBenchmarkPoint *points.Points
