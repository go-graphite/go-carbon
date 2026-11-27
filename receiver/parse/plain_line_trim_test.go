package parse

import (
	"bytes"
	"fmt"
	"math"
	"strconv"
	"testing"
)

func TestPlainLineTrimDifferential(t *testing.T) {
	// bytes.Trim is the old boundary oracle. Tabs, NULs and non-ASCII bytes
	// must not silently join the accepted whitespace set.
	padding := []string{"", " ", "\r", "\n", " \r\n \n\r ", "\t", "\x00", "\xc2\xa0", "\xff"}
	for _, body := range []string{"metric.name -0 1794800000", "metric 1.25 1794800000.5", "metric NaN 1", "", "metric  1 2"} {
		for _, left := range padding {
			for _, right := range padding {
				line := []byte(left + body + right)
				wantName, wantValue, wantTime, wantErr := referencePlainLine(line)
				name, value, timestamp, err := PlainLine(line)
				if !bytes.Equal(name, wantName) || math.Float64bits(value) != math.Float64bits(wantValue) || timestamp != wantTime || (err == nil) != (wantErr == nil) || err != nil && err.Error() != wantErr.Error() {
					t.Fatalf("line %q changed: %q %v %d %v; want %q %v %d %v", line, name, value, timestamp, err, wantName, wantValue, wantTime, wantErr)
				}
			}
		}
	}
}

// Freeze the original parser so the trim cannot broaden accepted whitespace.
func referencePlainLine(p []byte) ([]byte, float64, int64, error) {
	p = bytes.Trim(p, " \n\r")

	i1 := bytes.IndexByte(p, ' ')
	if i1 < 1 {
		return nil, 0, 0, fmt.Errorf("bad message: %#v", string(p))
	}

	i2 := bytes.IndexByte(p[i1+1:], ' ')
	if i2 < 1 {
		return nil, 0, 0, fmt.Errorf("bad message: %#v", string(p))
	}
	i2 += i1 + 1

	i3 := len(p)

	value, err := strconv.ParseFloat(unsafeString(p[i1+1:i2]), 64)
	if err != nil || math.IsNaN(value) {
		return nil, 0, 0, fmt.Errorf("bad message: %#v", string(p))
	}

	tsf, err := strconv.ParseFloat(unsafeString(p[i2+1:i3]), 64)
	if err != nil || math.IsNaN(tsf) {
		return nil, 0, 0, fmt.Errorf("bad message: %#v", string(p))
	}

	return p[:i1], value, int64(tsf), nil
}
