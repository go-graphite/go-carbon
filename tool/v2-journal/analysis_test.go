package main

import (
	"github.com/go-graphite/go-carbon/internal/partlog"
	"github.com/go-graphite/go-carbon/points"
	"math"
	"testing"
)

func TestParallelRealPathExact(t *testing.T) {
	var fs []partlog.DictionaryFrame
	for i := range 40 {
		fs = append(fs, partlog.DictionaryFrame{Key: string(rune('a' + i%4)), Clock: math.MinInt64 + int64(i), Definition: []byte{0, 255}, Points: []points.Point{{Timestamp: int64(i) - 2, Value: math.Float64frombits(0x7ff8000000000042 + uint64(i))}}})
	}
	parallel(fs, 2, 2)
}
