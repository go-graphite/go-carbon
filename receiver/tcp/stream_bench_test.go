package tcp

import (
	"bytes"
	"net"
	"testing"
	"time"

	"go.uber.org/zap"
)

// BenchmarkPlainConnection measures the real connection handler, including its
// initial protocol detection, over 32K plaintext rows. The same benchmark can be
// copied to a baseline checkout to measure the added connection-level work.
func BenchmarkPlainConnection(b *testing.B) {
	const rows = 32768
	line := []byte("example.application.region.zone.service.worker.instance.measurement.requests.count 12345.25 1700000000\n")
	input := bytes.Repeat(line, rows)
	r := &TCP{logger: zap.NewNop(), decompressor: newDecompressor("")}
	r.Start()
	defer r.Stop()
	var received int
	r.SetPointOut(func([]byte, float64, int64) { received++ })
	conn := &benchmarkReadConn{}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		conn.Reader.Reset(input)
		r.HandleConnection(conn)
	}
	b.StopTimer()
	if received != b.N*rows {
		b.Fatalf("got %d rows", received)
	}
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*rows), "ns/point")
}

type benchmarkReadConn struct{ bytes.Reader }

func (*benchmarkReadConn) Write([]byte) (int, error)        { panic("unused") }
func (*benchmarkReadConn) Close() error                     { return nil }
func (*benchmarkReadConn) LocalAddr() net.Addr              { return &net.TCPAddr{} }
func (*benchmarkReadConn) RemoteAddr() net.Addr             { return &net.TCPAddr{} }
func (*benchmarkReadConn) SetDeadline(time.Time) error      { return nil }
func (*benchmarkReadConn) SetReadDeadline(time.Time) error  { return nil }
func (*benchmarkReadConn) SetWriteDeadline(time.Time) error { return nil }
