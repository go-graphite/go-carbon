package v2daemon

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2core"
	"github.com/go-graphite/go-carbon/internal/v2part"
)

// BenchmarkAdmitParallel measures first-sight admission, the warm phase of
// a fresh store: every producer registers its own new names at once, as the
// receivers do when 10M names arrive for the first time. Admission must not
// serialize producers on a store-wide lock (warm 20-25 s vs 10 s on the
// six-arm host when it did).
func BenchmarkAdmitParallel(b *testing.B) {
	v2part.IDKeys.Store(true)
	defer v2part.IDKeys.Store(false)
	const producers, perProducer = 16, 1 << 16
	policies := make([][]byte, 4)
	for i := range policies {
		raw, err := v2core.EncodeRollupPolicy(v2core.RollupPolicy{Name: "x", Group: "bench", Config: sealrollup.Config{Method: sealrollup.Sum, Archives: []sealrollup.Archive{{Step: 1, Count: 86400 + int64(i)}}}})
		if err != nil {
			b.Fatal(err)
		}
		policies[i] = raw[len(`{"Name":"x"`):]
	}
	names := make([][]string, producers)
	defs := make([][][]byte, producers)
	for p := range names {
		for i := range perProducer {
			name := fmt.Sprintf("bench.admit.p%02d.host%04d.metric%06d", p, i%1009, i)
			names[p] = append(names[p], name)
			defs[p] = append(defs[p], append([]byte(`{"Name":"`+name+`"`), policies[i%len(policies)]...))
		}
	}
	var total time.Duration
	for b.Loop() {
		b.StopTimer()
		s, err := Open(b.TempDir(), Config{Layer: 3, Producers: producers, Shards: producers, GroupInterval: time.Hour, SealInterval: time.Hour, Hooks: v2core.Hooks{IDKeys: true, Identities: &v2core.PolicyCatalog{}}})
		if err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
		start := time.Now()
		var wg sync.WaitGroup
		errs := make([]error, producers)
		for p := range producers {
			wg.Go(func() {
				pr := s.Producers[p]
				for i, name := range names[p] {
					if _, _, err := pr.RegisterApplication(name, defs[p][i]); err != nil {
						errs[p] = err
						return
					}
				}
			})
		}
		wg.Wait()
		total += time.Since(start)
		b.StopTimer()
		for _, err := range errs {
			if err != nil {
				b.Fatal(err)
			}
		}
		if err := s.Close(); err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
	}
	b.ReportMetric(float64(total.Nanoseconds())/float64(b.N*producers*perProducer), "ns/name")
}
