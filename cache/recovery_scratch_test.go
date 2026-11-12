package cache

import (
	"fmt"
	"math"
	"strings"
	"testing"

	"github.com/go-graphite/go-carbon/internal/recovery"
	"github.com/go-graphite/go-carbon/points"
)

func TestPendingRecoveryEncoderScratchOwnership(t *testing.T) {
	for _, count := range []int{257, 10000} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			// Odd lengths exercise the unfinished encoder word. Special float
			// bits and irregular timestamps must survive the existing codec.
			data := make([]points.Point, count)
			for i := range data {
				data[i] = points.Point{Timestamp: int64(i*i - 100), Value: math.Float64frombits(uint64(i) * 0x9e3779b97f4a7c15)}
			}
			names := 3000
			if count > maxClaimScratch {
				names = 5
			}
			dir, root := t.TempDir(), t.TempDir()
			writeMemoryCheckpointData(t, dir, root, names, data)
			b, err := recovery.OpenBundle(dir, root)
			if err != nil {
				t.Fatal(err)
			}
			defer b.Close()
			c := New()
			c.SetMaxSize(uint64(names*count)*4 + 1)
			if err := c.AttachPendingRecoveryWithPreparedNames(b); err != nil {
				t.Fatal(err)
			}
			if err := c.RecoverPending(make(chan struct{}), 0); err != nil {
				t.Fatal(err)
			}
			for _, shard := range c.data {
				if cap(shard.claimEncoded) > maxClaimEncodeScratch {
					t.Fatal("retained oversized encoder scratch")
				}
				// Simulate reusing scratch for unrelated saved history. Published
				// item buffers, including partial tails, must be detached.
				for i := range shard.claimEncoded[:cap(shard.claimEncoded)] {
					shard.claimEncoded[:cap(shard.claimEncoded)][i] = 0xa5
				}
			}
			for i := range names {
				got := c.Get(fmt.Sprintf("synthetic.restore.%s%012d", strings.Repeat("long_prefix.", 30), i))
				if len(got) != len(data) {
					t.Fatalf("count %d: %d", i, len(got))
				}
				for j := range data {
					if got[j].Timestamp != data[j].Timestamp || math.Float64bits(got[j].Value) != math.Float64bits(data[j].Value) {
						t.Fatalf("point %d/%d changed", i, j)
					}
				}
			}
		})
	}
}
