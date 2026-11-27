package v2read

import (
	"context"
	"errors"
	"fmt"
	"math"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/partsformat"
	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2core"
	whisper "github.com/go-graphite/go-whisper"
)

func testConfig() v2core.Config {
	return v2core.Config{Rollups: true, AppendParts: true, JournalIdentity: "read-test", Shards: 1, Owners: 1, Producers: 1, MaxRows: 1 << 20, PartitionSeconds: 86400, RawCodec: "run1"}
}
func await(t *testing.T, f func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !f() {
		if time.Now().After(deadline) {
			t.Fatal("publication timeout")
		}
		time.Sleep(time.Millisecond)
	}
}
func TestClassicPendingAndSealed(t *testing.T) {
	t.Parallel()
	for _, def := range []string{"1s:1d", "1s:1d,1m:30d,1h:1y,1d:10y", "1m:14d,30m:2y"} {
		for method := sealrollup.Average; method <= sealrollup.First; method++ {
			for _, xff := range []float32{0, .5, 1} {
				t.Run(fmt.Sprintf("%s/%d/%g", def, method, xff), func(t *testing.T) {
					t.Parallel() // each case owns its reader, engine and whisper oracle
					now := int64(1794700800)
					clock := now
					retentions := whisper.MustParseRetentionDefs(def)
					policy := v2core.RollupPolicy{Name: partsformat.ID("test.metric"), Group: "test", Config: sealrollup.Config{Method: method, XFilesFactor: xff}}
					for _, a := range retentions {
						policy.Config.Archives = append(policy.Config.Archives, sealrollup.Archive{Step: int64(a.SecondsPerPoint()), Count: int64(a.NumberOfPoints())})
					}
					encoded, err := v2core.EncodeRollupPolicy(policy)
					if err != nil {
						t.Fatal(err)
					}
					classic, err := whisper.CreateWithOptions(filepath.Join(t.TempDir(), "classic.wsp"), retentions, whisper.AggregationMethod(method), xff, &whisper.Options{Sparse: true, Now: func() time.Time { return time.Unix(clock, 0) }})
					if err != nil {
						t.Fatal(err)
					}
					defer classic.Close()
					reader, _ := New(1, 1<<30, 64<<20)
					// The oracle rewinds its clock a day below the newest batch clock.
					// Classic never deletes, so hold v2 age retirement (ArchiveClock)
					// below the oracle's history to compare every slot it renders.
					// It also holds idle closure below the data watermark: after each seal,
					// all current coarse windows must still be projected from source slots.
					engine, err := v2core.Open(t.TempDir(), testConfig(), v2core.Hooks{PublishPending: reader.PublishPending, Now: func() time.Time { return time.Unix(1, 0) }})
					if err != nil {
						t.Fatal(err)
					}
					defer func() {
						reader.Close()
						if err := engine.Close(); err != nil {
							t.Error(err)
						}
					}()
					step := policy.Config.Archives[0].Step
					start := now - 120*step
					dense := []v2core.Row{}
					for i := int64(0); i < 90; i++ {
						dense = append(dense, v2core.Row{ID: 7, Time: start + i*step, Bits: math.Float64bits(float64(i%17) - 8), Seq: uint64(i + 1)})
					}
					special := []v2core.Row{{ID: 7, Time: start + step, Bits: math.Float64bits(9), Seq: 101}}
					for i, b := range []uint64{0x7ff8000000000042, 1 << 63, 0, 0x7ff0000000000000, 0xfff0000000000000, 1} {
						special = append(special, v2core.Row{ID: 7, Time: start + int64(i+2)*step, Bits: b, Seq: uint64(102 + i)})
					}
					advance := step * policy.Config.Archives[0].Count
					groups := [][]v2core.Row{dense, special, {{ID: 7, Time: start + advance, Bits: math.Float64bits(17), Seq: 201}}, {{ID: 7, Time: start + step, Bits: math.Float64bits(127), Seq: 202}}, {{ID: 7, Time: now - 2*step, Bits: math.Float64bits(11), Seq: 203}}}
					clocks := []int64{now, now, now + advance, now + advance, now - 10*step}
					for i, rows := range groups {
						clock = clocks[i]
						ptrs := make([]*whisper.TimeSeriesPoint, len(rows))
						for j, p := range rows {
							ptrs[j] = &whisper.TimeSeriesPoint{Time: int(p.Time), Value: math.Float64frombits(p.Bits)}
						}
						if err = classic.UpdateMany(ptrs); err != nil {
							t.Fatal(err)
						}
						b := v2core.Batch{ID: 7, Policy: encoded, Order: uint64(i + 1), Clock: clock, Points: rows}
						if err = engine.AppendBatches(0, []v2core.Batch{b}, uint64(i+1), false); err != nil {
							t.Fatal(err)
						}
						// Wait for the publication that carries this batch: a background
						// merge publishes too, possibly before the batch reaches the head.
						await(t, func() bool {
							g := reader.owners[0].Load()
							if g == nil {
								return false
							}
							found := false
							g.view.Each(func(a *v2core.PendingApplication) bool {
								found = a.Order == b.Order
								return !found
							})
							return found
						})
						// Overwrite borrowed input after acknowledgment: the published head owns it.
						for j := range rows {
							rows[j].Bits = 0xdeadbeef
						}
						for _, seal := range []bool{false, true} {
							if seal {
								if err = engine.AppendBatches(0, nil, uint64(i+1), true); err != nil {
									t.Fatal(err)
								}
							}
							for _, span := range []int64{120 * step, advance + step, policy.Config.Archives[len(policy.Config.Archives)-1].Step * policy.Config.Archives[len(policy.Config.Archives)-1].Count} {
								for _, bounds := range [][2]int64{{clock - span, clock}, {clock, clock}, {clock + 1, clock + 2}, {clock - span, clock - span}} {
									want, e := classic.Fetch(int(bounds[0]), int(bounds[1]))
									if e != nil {
										t.Fatal(e)
									}
									err = reader.FetchMany(context.Background(), []string{"test.metric"}, bounds[0], bounds[1], clock, func(_ string, got *Series) error {
										if (got == nil) != (want == nil) {
											return fmt.Errorf("nil mismatch")
										}
										if got == nil {
											return nil
										}
										if got.From != int64(want.FromTime()) || got.Until != int64(want.UntilTime()) || got.Step != int64(want.Step()) || len(got.Values) != len(want.Values()) {
											return fmt.Errorf("shape got %d %d %d %d want %d %d %d %d", got.From, got.Until, got.Step, len(got.Values), want.FromTime(), want.UntilTime(), want.Step(), len(want.Values()))
										}
										for j, v := range want.Values() {
											if math.Float64bits(v) != math.Float64bits(got.Values[j]) {
												return fmt.Errorf("slot%d got%x want%x", j, math.Float64bits(got.Values[j]), math.Float64bits(v))
											}
										}
										return nil
									})
									if err != nil {
										t.Fatalf("group%d sealed%t range%v: %v", i, seal, bounds, err)
									}
								}
							}
						}
					}
				})
			}
		}
	}
}

func TestConcurrentPublicationAndClose(t *testing.T) {
	reader, _ := New(1, 1<<28, 1<<20)
	engine, err := v2core.Open(t.TempDir(), testConfig(), v2core.Hooks{PublishPending: reader.PublishPending})
	if err != nil {
		t.Fatal(err)
	}
	p, _ := v2core.EncodeRollupPolicy(v2core.RollupPolicy{Name: "metric", Group: "test", Config: sealrollup.Config{Method: sealrollup.Last, Archives: []sealrollup.Archive{{Step: 1, Count: 100}}}})
	var wg sync.WaitGroup
	done := make(chan struct{})
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-done:
					return
				default:
				}
				err := reader.FetchMany(context.Background(), []string{"metric"}, 900, 1000, 1000, func(_ string, s *Series) error { return nil })
				if err != nil && !errors.Is(err, ErrNotFound) {
					t.Error(err)
					return
				}
			}
		}()
	}
	for i := uint64(1); i <= 20; i++ {
		if err = engine.AppendBatches(0, []v2core.Batch{{ID: 1, Order: i, Clock: 1000, Policy: p, Points: []v2core.Row{{ID: 1, Time: 990, Bits: math.Float64bits(float64(i)), Seq: i}}}}, i, i%3 == 0); err != nil {
			t.Fatal(err)
		}
	}
	close(done)
	wg.Wait()
	reader.Close()
	if err = engine.Close(); err != nil {
		t.Fatal(err)
	}
	if len(reader.renders) != 0 {
		t.Fatal("render admission leak")
	}
}

func TestPinnedCallbackDoesNotBlockWriter(t *testing.T) {
	reader, _ := New(1, 1<<28, 1<<20)
	reader.Now = func() time.Time { return time.Unix(1000, 0) }
	engine, err := v2core.Open(t.TempDir(), testConfig(), v2core.Hooks{PublishPending: reader.PublishPending})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		reader.Close()
		if err := engine.Close(); err != nil {
			t.Error(err)
		}
	}()
	policy, _ := v2core.EncodeRollupPolicy(v2core.RollupPolicy{Name: "metric", Group: "test", Config: sealrollup.Config{Method: sealrollup.Last, Archives: []sealrollup.Archive{{Step: 1, Count: 100}}}})
	appendValue := func(order uint64, value float64) error {
		return engine.AppendBatches(0, []v2core.Batch{{ID: 1, Order: order, Clock: 1000, Policy: policy, Points: []v2core.Row{{ID: 1, Time: 990, Bits: math.Float64bits(value), Seq: order}}}}, order, true)
	}
	if err = appendValue(1, 1); err != nil {
		t.Fatal(err)
	}
	entered, release, done := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	go func() {
		done <- reader.FetchMany(context.Background(), []string{"metric"}, 989, 990, 1000, func(_ string, s *Series) error {
			close(entered)
			<-release
			if len(s.Values) != 1 || s.Values[0] != 1 {
				return fmt.Errorf("pinned result changed: %v", s.Values)
			}
			return nil
		})
	}()
	<-entered
	wrote := make(chan error, 1)
	go func() { wrote <- appendValue(2, 2) }()
	select {
	case err = <-wrote:
	case <-time.After(5 * time.Second):
		close(release)
		t.Fatal("writer waited for pinned consumer")
	}
	close(release)
	if err != nil {
		t.Fatal(err)
	}
	if err = <-done; err != nil {
		t.Fatal(err)
	}
	got, err := reader.Fetch(context.Background(), "metric", 989, 990)
	if err != nil {
		t.Fatal(err)
	}
	if got.Values()[0] != 2 || got.Metric.AggregationMethod != "last" {
		t.Fatalf("wrong detached series %+v", got)
	}
	// A later publication cannot reuse the detached response buffer.
	if err = appendValue(3, 3); err != nil {
		t.Fatal(err)
	}
	if got.Values()[0] != 2 {
		t.Fatal("detached values changed")
	}
}
