package main

import (
	"math"
	"testing"

	"github.com/go-graphite/go-carbon/internal/partlog"
	"github.com/go-graphite/go-carbon/points"
)

// Exercise the real cut/flush/compare/accounting consumer, including a group
// crossing a cut and full signed clock/value representations.
func TestMeasuredIndependentBlocks(t *testing.T) {
	m := measurement{Codec: "PDC3", BlockMiB: 1, encoder: &partlog.PredictorV3Encoder{}, decoder: &partlog.PredictorV3Decoder{}}
	defer m.encoder.Close()
	defer m.decoder.Close()
	for i := 0; i < 30000; i++ {
		m.add(partlog.DictionaryFrame{Key: "a", Clock: math.MinInt64 + int64(i), Definition: []byte{0xff}, Points: []points.Point{{Timestamp: int64(i) - 2000, Value: math.Float64frombits(0x7ff8000000000000 + uint64(i))}}})
	}
	m.flush()
	if m.Points != 30000 || m.Blocks < 2 || m.Bytes <= m.Blocks*32 || m.EncodeNS <= 0 || m.DecodeNS <= 0 {
		t.Fatalf("accounting: %+v", m)
	}
}
