package v2core

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2part"
)

// Identical source works against the eager baseline. This measures the full
// registered seal planner and publication encoding, excluding storage I/O.
func BenchmarkClosedWindowSealCPU(b *testing.B) {
	const names = 1024
	r := newCoreRollups()
	r.parallelism = 1
	input := make([]Batch, names)
	for i := range input {
		policy := closedBenchPolicy(b, fmt.Sprintf("bench.%05d", i))
		m, err := r.metric(uint64(i+1), policy)
		if err != nil {
			b.Fatal(err)
		}
		rollupTestInstall(r, m.id, m, freshMetadata)
		input[i] = Batch{ID: m.id, Registered: m, Points: make([]Row, 1)}
	}
	b.ReportAllocs()
	b.ResetTimer()
	start := historyCPUTime()
	for epoch := 0; epoch < b.N; epoch++ {
		for i := range input {
			input[i].Order, input[i].Clock = uint64(epoch+1), 1794700800+int64(epoch)
			input[i].Points[0] = Row{ID: uint64(i + 1), Time: input[i].Clock, Bits: uint64(epoch % 17), Seq: 1}
		}
		plan, err := r.plan(SealInput{Batches: input})
		if err != nil {
			b.Fatal(err)
		}
		plan.Commit()
	}
	b.ReportMetric(float64(historyCPUTime()-start)/float64(b.N*names), "cpu-ns/point")
}

func BenchmarkClosedWindowReadCost(b *testing.B) {
	for _, closed := range []bool{false, true} {
		for _, archive := range []int{1, 2} {
			b.Run(fmt.Sprintf("closed%t/archive%d", closed, archive), func(b *testing.B) {
				cfg := coreTestConfig()
				cfg.Shards = 1
				cfg.Producers = 1
				cfg.Rollups = true
				e, err := Open(b.TempDir(), cfg, Hooks{})
				if err != nil {
					b.Fatal(err)
				}
				defer e.Close()
				e.owners[0].rollups.now = func() time.Time { return time.Unix(1794700859, 0) }
				policy := closedBenchPolicy(b, "bench.metric")
				var rows []Row
				for i := int64(0); i < 60; i++ {
					rows = append(rows, Row{ID: 1, Time: 1794700800 + i, Bits: uint64(i), Seq: uint64(i + 1)})
				}
				if err = e.AppendBatches(0, []Batch{{ID: 1, Policy: policy, Order: 1, Clock: 1794700860, Points: rows}}, 1, true); err != nil {
					b.Fatal(err)
				}
				step := int64(60)
				count := int64(1440)
				if archive == 2 {
					step = 3600
					count = 8760
				}
				if closed {
					if err = e.AppendBatches(0, []Batch{{ID: 1, Policy: policy, Order: 2, Clock: 1794700800 + step, Points: []Row{{ID: 1, Time: 1794700800 + step, Seq: 61}}}}, 2, true); err != nil {
						b.Fatal(err)
					}
				}
				pin := e.DBs()[0].NewSnapshot()
				defer pin.Close()
				index, err := NewRollupSnapshot(context.Background(), pin, nil, nil, nil, 16<<20)
				if err != nil {
					b.Fatal(err)
				}
				physical := (1794700800 / step) % count
				ctx := context.Background()
				render := func() error {
					return index.ScanArchive(ctx, "bench.metric", archive, physical, physical+1, func(sealrollup.Record) error { return nil })
				}
				if err = render(); err != nil {
					b.Fatal(err)
				}
				b.ReportAllocs()
				b.ResetTimer()
				start := historyCPUTime()
				for i := 0; i < b.N; i++ {
					if err = render(); err != nil {
						b.Fatal(err)
					}
				}
				b.ReportMetric(float64(historyCPUTime()-start)/float64(b.N), "cpu-ns/render")
			})
		}
	}
}

func closedBenchPolicy(b *testing.B, name string) []byte {
	b.Helper()
	p, err := EncodeRollupPolicy(RollupPolicy{Name: v2part.ID(name), Group: "test", Config: sealrollup.Config{Method: sealrollup.Average, XFilesFactor: .1, Archives: []sealrollup.Archive{{Step: 1, Count: 3600}, {Step: 60, Count: 1440}, {Step: 3600, Count: 8760}}}})
	if err != nil {
		b.Fatal(err)
	}
	return p
}
