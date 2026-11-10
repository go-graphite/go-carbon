package cache

import (
	"fmt"
	"os"
	"testing"

	"github.com/go-graphite/go-carbon/internal/contentiontest"
)

func TestCacheContentionBudget(t *testing.T) {
	if os.Getenv("GO_CARBON_CONTENTION") != "1" {
		t.Skip("set GO_CARBON_CONTENTION=1")
	}
	for _, stream := range []bool{false, true} {
		t.Run(fmt.Sprintf("stream%v", stream), func(t *testing.T) {
			c := New()
			c.SetMaxSize(0)
			var names [256][]byte
			var slots [256]any
			for w := range names {
				names[w] = fmt.Appendf(nil, "contention.cache.metric.%d", w)
				if stream {
					slots[w] = c.ResolveStreamName(names[w])
				}
			}
			contentiontest.Run(t, 256, 4096, 48, func(w, i int) {
				if stream {
					c.AddStreamPoint(names[w], float64(i), int64(i), &slots[w])
				} else {
					c.AddPoint(names[w], float64(i), int64(i))
				}
				if i%256 == 255 {
					c.Pop(string(names[w]))
				}
			})
			if c.Size() != 0 {
				t.Fatal("points left after bounded ingestion")
			}
		})
	}
}
