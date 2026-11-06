package persister

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/cache"
	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/whisperio"
)

// Run unchanged in old/new source trees. Every metric has one producer, so
// concurrency cannot change the input order of equal-timestamp corrections.
// Hash timestamp + raw value bits + routing/retention/revision metadata, never
// decimal text or approximate float equality. Fixtures include NaN payloads,
// signed zeros, infinities, denormals, late data, duplicates and counter resets.
func TestContentionDifferential(t *testing.T) {
	for _, mode := range []string{"single", "batch", "transfer"} {
		t.Run(mode, func(t *testing.T) {
			const count, rounds, rows = 64, 8, 32
			ctx := context.Background()
			now := time.Unix(1790000000, 0)
			db, err := openTestStore(t, t.TempDir(), store.Options{Now: func() time.Time { return now },
				ExactBlocks: true, Continuation: &whisperio.ContinuationCodec{}, MetadataCacheSize: 8 << 20, SyncInterval: time.Hour})
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			c := cache.New()
			c.SetMaxSize(0)
			p := NewWhisper("", nil, NewWhisperAggregation(), nil, c.PopNotConfirmed, c.Confirm, c.Pop)
			p.SetMetricStore(db)
			p.SetRequeue(c.Requeue)
			var names [count]string
			for i := range names {
				names[i] = fmt.Sprintf("differential.metric.%03d", i)
				_, err := db.Create(ctx, store.MetricConfig{Name: names[i], Retentions: []store.Retention{{Step: 1, Count: 3600}, {Step: 60, Count: 1440}}, AggregationMethod: store.Last})
				if err != nil {
					t.Fatal(err)
				}
			}
			bits := []uint64{0, 1 << 63, 1, 0x0010000000000000, 0x7ff0000000000000, 0xfff0000000000000, 0x7ff8000000001234, 0x3ff0000000000001, 0x4045400000000000}
			var wg sync.WaitGroup
			for group := range count / 8 {
				wg.Go(func() {
					groupNames := names[group*8 : group*8+8]
					for round := range rounds {
						for m, name := range groupNames {
							for row := range rows {
								v := math.Float64frombits(bits[(m+row+round)%len(bits)])
								ts := now.Unix() - 600 + int64(round*rows+row)
								if row%7 == 0 {
									ts -= 5
								}
								c.AddPoint([]byte(name), v, ts)
							}
						}
						switch mode {
						case "single":
							for _, name := range groupNames {
								p.store(name)
							}
						case "batch":
							p.storeSharedBatch(groupNames)
						case "transfer":
							if err := p.FlushForTransfer(ctx, groupNames); err != nil {
								t.Error(err)
								return
							}
						}
					}
				})
			}
			wg.Wait()
			if !c.IsEmpty() || c.NotConfirmedLength() != 0 {
				t.Fatal("unconfirmed input remains")
			}
			h := sha256.New()
			var word [8]byte
			put := func(v uint64) { binary.LittleEndian.PutUint64(word[:], v); h.Write(word[:]) }
			for _, name := range names {
				s, err := db.Snapshot(ctx, name)
				if err != nil {
					t.Fatal(err)
				}
				h.Write([]byte(name))
				put(s.Metadata.ID)
				put(s.Metadata.Revision)
				put(uint64(s.Metadata.AggregationMethod))
				put(uint64(len(s.Archives)))
				for _, a := range s.Archives {
					put(uint64(a.Retention.Step))
					put(uint64(a.Retention.Count))
					put(uint64(len(a.Points)))
					for _, point := range a.Points {
						put(uint64(point.Timestamp))
						put(math.Float64bits(point.Value))
					}
				}
			}
			t.Logf("DIFFERENTIAL %s %x", mode, h.Sum(nil))
		})
	}
}
