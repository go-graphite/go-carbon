// Package ingestreplay provides the point comparison and lossless plaintext
// encoding shared by sync and replica-backfill. It does not choose archives or
// establish receiver acceptance; callers must prove both before checkpointing.
package ingestreplay

import (
	"io"
	"math"
	"net"
	"strconv"
	"strings"
	"time"
)

type Series struct {
	Start, Step int64
	Values      []float64
}

func (s Series) At(ts int64) (float64, bool) {
	if s.Step <= 0 || ts < s.Start || (ts-s.Start)%s.Step != 0 {
		return math.NaN(), false
	}
	i := (ts - s.Start) / s.Step
	if i >= int64(len(s.Values)) {
		return math.NaN(), false
	}
	return s.Values[i], true
}

type Point struct {
	Timestamp int64
	Value     float64
}

func ValidName(name string) bool {
	return name != "" && !strings.ContainsAny(name, " \t\r\n*?[]{}")
}

// Select returns non-NaN source values at exact slots, preserving all bits.
func Select(source Series, slots []int64) []Point {
	var out []Point
	for _, ts := range slots {
		if v, ok := source.At(ts); ok && !math.IsNaN(v) {
			out = append(out, Point{ts, v})
		}
	}
	return out
}

// Missing selects finite source values at absent or NaN target slots. Existing
// infinities also win, matching transfer merge. Grids are never resampled.
func Missing(source, target Series, from, until int64) []Point {
	if source.Step <= 0 || source.Step != target.Step {
		return nil
	}
	var slots []int64
	for i, v := range source.Values {
		ts := source.Start + int64(i)*source.Step
		if ts < from || ts >= until || math.IsNaN(v) || math.IsInf(v, 0) {
			continue
		}
		v, ok := target.At(ts)
		if !ok || math.IsNaN(v) {
			slots = append(slots, ts)
		}
	}
	return Select(source, slots)
}

func AppendPoint(buf []byte, name string, value float64, timestamp int64) []byte {
	buf = append(buf, name...)
	buf = append(buf, ' ')
	buf = strconv.AppendFloat(buf, value, 'g', -1, 64)
	buf = append(buf, ' ')
	buf = strconv.AppendInt(buf, timestamp, 10)
	return append(buf, '\n')
}

// Send handles short writes; returning nil means the complete buffer reached
// the socket, not that an asynchronous receiver has committed it.
func Send(conn net.Conn, buf []byte, timeout time.Duration) error {
	if err := conn.SetWriteDeadline(time.Now().Add(timeout)); err != nil {
		return err
	}
	for len(buf) > 0 {
		n, err := conn.Write(buf)
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		buf = buf[n:]
	}
	return nil
}
