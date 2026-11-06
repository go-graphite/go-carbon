package restartcpu

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

func loadHistory(t *testing.T, path string) {
	t.Helper()
	host, _ := os.Hostname()
	data, err := json.Marshal(history{Host: host, Steady: sample{time.Now().Add(-time.Minute), 10}, Updates: 1000})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
}

// A relay resize triples mandatory CPU before the next eligible history minute.
// The predecessor floor is insufficient; spare configured CPU must fund catch-up.
func TestConfiguredFloorAdmitsTripledLoad(t *testing.T) {
	path := filepath.Join(t.TempDir(), "restart-cpu.json")
	synctest.Test(t, func(t *testing.T) {
		loadHistory(t, path)
		started := time.Now()
		var writes atomic.Uint64
		b := startHistory(context.Background(), path, 120, 1.5, func() float64 {
			return 30*time.Since(started).Seconds() + float64(writes.Load())*.005
		})
		defer b.Stop()
		b.SetRateCeiling(3000)
		b.ObserveWriteout(1000, 0)
		time.Sleep(time.Second)
		exit, done := make(chan bool), make(chan struct{})
		go func() {
			defer close(done)
			for {
				select {
				case <-exit:
					return
				default:
				}
				if !b.WaitWriteoutBatch(exit, 10) {
					return
				}
				writes.Add(10)
				for range 10 {
					b.RecordWrite()
				}
				b.EndWriteout()
				time.Sleep(time.Second / 300)
			}
		}()
		time.Sleep(10 * time.Second)
		synctest.Wait()
		close(exit)
		<-done
		rate := float64(writes.Load()) / 10
		t.Logf("history=10 headroom=1.5 configured=120 mandatory=30 allowance=%.0f writeout=%.1f/s demand=3000/s", b.Cores(), rate)
		if b.Cores() != 120 || rate < 2900 {
			t.Fatal("stale pre-resize CPU history held writeout at the predecessor floor", b.Cores(), rate)
		}
	})
}

// Moving history away cannot override an old sampler's final save. Configured
// CPU must still win whether that save runs before or after successor selection.
func TestConfiguredFloorSurvivesPredecessorStopSave(t *testing.T) {
	for _, stopBeforeStart := range []bool{false, true} {
		t.Run(map[bool]string{false: "save_after_start", true: "save_before_start"}[stopBeforeStart], func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "restart-cpu.json")
			loadHistory(t, path)
			old := startHistory(context.Background(), path, 0, 1.5, func() float64 { return 0 })
			defer old.Stop()
			if err := os.Rename(path, path+".parked"); err != nil {
				t.Fatal(err)
			}
			if stopBeforeStart {
				if err := old.Stop(); err != nil {
					t.Fatal(err)
				}
			}
			next := startHistory(context.Background(), path, 120, 1.5, func() float64 { return 0 })
			defer next.Stop()
			if err := old.Stop(); err != nil {
				t.Fatal(err)
			}
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatal("predecessor did not reproduce the final re-save", err)
			}
			var saved history
			if err := json.Unmarshal(data, &saved); err != nil || saved.Steady.Cores != 10 {
				t.Fatal("predecessor saved unexpected history", saved, err)
			}
			if next.Cores() != 120 {
				t.Fatal("predecessor final save defeated configured override", next.Cores())
			}
		})
	}
}

func TestParkedDirectoryBlocksPredecessorSave(t *testing.T) {
	path := filepath.Join(t.TempDir(), "restart-cpu.json")
	loadHistory(t, path)
	old := startHistory(context.Background(), path, 0, 1.5, func() float64 { return 0 })
	defer old.Stop()
	if err := os.Rename(path, path+".parked"); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(path, 0700); err != nil {
		t.Fatal(err)
	}
	if err := old.Stop(); err == nil {
		t.Fatal("old sampler replaced the parked directory")
	}
	next := startHistory(context.Background(), path, 120, 1.5, func() float64 { return 0 })
	defer next.Stop()
	if next.Cores() != 120 || next.Source() != "fallback" {
		t.Fatal("parked directory did not select fallback", next.Cores(), next.Source())
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := next.Stop(); err != nil {
		t.Fatal("history could not resume after removing the directory", err)
	}
}
