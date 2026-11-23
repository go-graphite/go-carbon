package ingestreplay

import (
	"bufio"
	"math"
	"net"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestMissingAndPlaintextBits(t *testing.T) {
	values := []float64{math.Copysign(0, -1), math.SmallestNonzeroFloat64, math.MaxFloat64, 1.0000000000000002, math.NaN(), math.Inf(1), 42}
	src := Series{Start: 100, Step: 10, Values: values}
	dst := Series{Start: 100, Step: 10, Values: []float64{math.NaN(), math.NaN(), math.NaN(), math.NaN(), math.NaN(), math.NaN(), 73}}
	pts := Missing(src, dst, 100, 170)
	if len(pts) != 4 {
		t.Fatal(pts)
	}
	var buf []byte
	for _, p := range pts {
		buf = AppendPoint(buf, "metric", p.Value, p.Timestamp)
	}
	scan := bufio.NewScanner(strings.NewReader(string(buf)))
	for i := 0; scan.Scan(); i++ {
		fields := strings.Fields(scan.Text())
		v, err := strconv.ParseFloat(fields[1], 64)
		if err != nil || math.Float64bits(v) != math.Float64bits(values[i]) {
			t.Fatal(i, v, err)
		}
	}
	if got := Missing(src, Series{Start: 100, Step: 20, Values: values}, 100, 170); len(got) != 0 {
		t.Fatal("resampled a different grid")
	}
	if got := Missing(src, dst, 110, 120); !reflect.DeepEqual(got, pts[1:2]) {
		t.Fatal(got)
	}
	if got := Missing(Series{Start: 100, Step: 10, Values: []float64{1}}, Series{Start: 100, Step: 10, Values: []float64{math.Inf(1)}}, 100, 110); len(got) != 0 {
		t.Fatal("overwrote an existing infinity")
	}
}

type shortConn struct {
	net.Conn
	bytes []byte
}

func (c *shortConn) SetWriteDeadline(time.Time) error { return nil }
func (c *shortConn) Write(b []byte) (int, error) {
	n := min(3, len(b))
	c.bytes = append(c.bytes, b[:n]...)
	return n, nil
}

func TestSendShortWrites(t *testing.T) {
	c := &shortConn{}
	if err := Send(c, []byte("metric 1.0000000000000002 100\n"), time.Second); err != nil {
		t.Fatal(err)
	}
	if string(c.bytes) != "metric 1.0000000000000002 100\n" {
		t.Fatal(string(c.bytes))
	}
}
