package v2core

import (
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
)

func TestCoreSealIntervalTimeSizeAndReopen(t *testing.T) {
	for _, size := range []bool{false, true} {
		t.Run(map[bool]string{false: "time", true: "size"}[size], func(t *testing.T) {
			cfg := coreTestConfig()
			cfg.Shards = 1
			cfg.Producers = 1
			cfg.MaxRows = 64
			cfg.SealMaxRows = 8
			cfg.SealInterval = 150 * time.Millisecond
			if size {
				cfg.SealInterval = time.Hour
			}
			root := t.TempDir()
			e, err := openCore(root, cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if e != nil {
					_ = e.close()
				}
			}()
			n := 1
			if size {
				n = 8
			}
			rows := make([]row, n)
			for i := range rows {
				rows[i] = row{ID: 1, Time: int64(i), Seq: uint64(i + 1), Bits: uint64(i)}
			}
			if err = e.submit(0, rows, 1, false); err != nil {
				t.Fatal(err)
			}
			if !size && e.durableFrontiers()[0][0] != 0 {
				t.Fatal("sealed before deadline")
			}
			until := time.Now().Add(2 * time.Second)
			for e.durableFrontiers()[0][0] != 1 {
				if time.Now().After(until) {
					t.Fatal("time/size did not seal")
				}
				time.Sleep(time.Millisecond)
			}
			if err = e.close(); err != nil {
				t.Fatal(err)
			}
			e = nil
			cfg.SealInterval = 10 * time.Second
			cfg.SealMaxRows = 32
			e, err = openCore(root, cfg)
			if err != nil {
				t.Fatal("cadence changed disk compatibility", err)
			}
			if e.durableFrontiers()[0][0] != 1 {
				t.Fatal("frontier lost")
			}
		})
	}
	cfg := coreTestConfig()
	cfg.SealInterval = -1
	if e, err := openCore(t.TempDir(), cfg); err == nil {
		_ = e.close()
		t.Fatal("negative cadence accepted")
	}
	cfg.SealInterval = 0
	cfg.SealMaxRows = -1
	if e, err := openCore(t.TempDir(), cfg); err == nil {
		_ = e.close()
		t.Fatal("negative row threshold accepted")
	}
}

func TestCoreSealIntervalPublishesBeforePlan(t *testing.T) {
	cfg := coreTestConfig()
	cfg.Shards = 1
	cfg.Producers = 1
	cfg.SealInterval = 10 * time.Second
	var published uint64
	e, err := openCoreWithHooks(t.TempDir(), cfg, coreHooks{
		PublishBatches: func(_ int, pin *pebble.Snapshot, batches []coreBatch, _ uint64) error {
			defer pin.Close()
			for _, b := range batches {
				published = max(published, b.Order)
			}
			return nil
		},
		PlanSeal: func(_ int, in coreSealInput) (coreSealPlan, error) {
			if published != 2 {
				t.Errorf("unpublished tail when seal plan starts: order%d", published)
			}
			return coreSealPlan{}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer e.close()
	for i := uint64(1); i <= 2; i++ {
		if err = e.submitBatches(0, []coreBatch{{ID: 1, Order: i, Points: []row{{ID: 1, Time: int64(i), Seq: i}}}}, i, false); err != nil {
			t.Fatal(err)
		}
	}
	if err = e.submitBatches(0, nil, 2, true); err != nil {
		t.Fatal(err)
	}
}
