package tcp

import (
	"bytes"
	"strings"
	"testing"

	"github.com/go-graphite/go-carbon/receiver/stream/codec"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func TestNestedStreamRejected(t *testing.T) {
	for _, size := range []int{1, 1 << 20} {
		var logs bytes.Buffer
		core := zapcore.NewCore(zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig()), zapcore.AddSync(&logs), zap.ErrorLevel)
		for _, compression := range []string{"", "zstd"} {
			var r *TCP
			got := handlePlainConnectionConfigured(t, "", encodeFrames(t, streamWire(t, codec.FlagZstd), size), func(rcv *TCP) {
				r = rcv
				r.compression = compression
				r.logger = zap.New(core)
			})
			if got.metricsReceived != 0 || got.errors != 1 || r.streamCounters[streamConnections].Load() != 0 {
				t.Fatal("nested stream was not rejected by zstd input")
			}
		}
		if strings.Count(logs.String(), "zstd input failed") != 2 || !strings.Contains(logs.String(), "stream input cannot be wrapped in zstd frames") {
			t.Fatal("missing explicit nested stream ERROR logs")
		}
	}
}
