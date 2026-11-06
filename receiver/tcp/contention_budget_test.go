package tcp

import (
	"bytes"
	"fmt"
	"os"
	"testing"

	"github.com/go-graphite/go-carbon/cache"
	"github.com/go-graphite/go-carbon/internal/contentiontest"
	"go.uber.org/zap"
)

func TestReceiverContentionBudget(t *testing.T) {
	if os.Getenv("GO_CARBON_CONTENTION") != "1" {
		t.Skip("set GO_CARBON_CONTENTION=1")
	}
	c := cache.New()
	c.SetMaxSize(0)
	r := &TCP{logger: zap.NewNop(), decompressor: newDecompressor("")}
	r.SetPointOut(c.AddPoint)
	r.Start()
	defer r.Stop()
	var names [256]string
	var wire [256][]byte
	var conns [256]benchmarkReadConn
	for w := range names {
		names[w] = fmt.Sprintf("contention.receiver.metric.%d", w)
		wire[w] = bytes.Repeat(fmt.Appendf(nil, "%s 17.25 1790000000\n", names[w]), 64)
	}
	contentiontest.Run(t, 256, 32, 48, func(w, i int) {
		conns[w].Reader.Reset(wire[w])
		r.HandleConnection(&conns[w])
		if got, ok := c.Pop(names[w]); !ok || len(got.Data) != 64 {
			t.Error("receiver lost points")
		}
	})
}
