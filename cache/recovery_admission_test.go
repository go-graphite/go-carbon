package cache

import (
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-graphite/go-carbon/points"
)

func TestRecoveryMemoryCrossesHalfCeilingAndSignalsPressure(t *testing.T) {
	c := New()
	c.SetMaxSize(20000)
	c.AddRecovered(&points.Points{Metric: "restored", Data: make([]points.Point, 10001)})
	if c.HasRecoveryCapacity(1) {
		t.Fatal("incident oracle did not bind at half cache")
	}
	if !c.WriteoutState().RecoveryBlocked {
		t.Fatal("half-cache stop is invisible to writeout")
	}
	c.SetRecoveryMemory(func() (uint64, bool) { return 1280 << 30, true })
	if !c.HasRecoveryCapacityFor("batch", 9999) {
		t.Fatal("free memory still held behind half-cache count")
	}
	if c.HasRecoveryCapacityFor("batch", 10000) {
		t.Fatal("full configured point ceiling lost")
	}
}

func TestRecoveryMemoryWindowGrowsShrinksAndExpires(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := New()
		available := uint64(1 << 20)
		valid := true
		c.SetRecoveryMemory(func() (uint64, bool) { return available, valid })
		if !c.HasRecoveryCapacityFor("batch", 1) {
			t.Fatal("initial memory grant")
		}
		first, _, _ := c.RecoveryCapacity()
		available *= 8
		time.Sleep(recoveryMemoryInterval)
		c.HasRecoveryCapacityFor("batch", 1)
		grown, _, _ := c.RecoveryCapacity()
		if grown != 8*first {
			t.Fatal("headroom did not grow", first, grown)
		}
		available = 0
		time.Sleep(recoveryMemoryInterval)
		if c.HasRecoveryCapacityFor("batch", 1) || !c.RecoveryBlocked() {
			t.Fatal("memory pressure did not stop admission and signal writeout")
		}
		window, _, _ := c.RecoveryCapacity()
		if window != 0 {
			t.Fatal(window)
		}
		available = 8 << 20
		valid = false
		time.Sleep(recoveryMemoryInterval)
		if c.HasRecoveryCapacityFor("batch", 1) {
			t.Fatal("unknown memory minted credit")
		}
		time.Sleep(2*recoveryMemoryInterval + time.Nanosecond)
		if c.RecoveryBlocked() {
			t.Fatal("completed/canceled recovery pressure never expired")
		}
	})
}

func TestRecoveryMemoryReservationsBoundConcurrentWorkers(t *testing.T) {
	c := New()
	c.SetRecoveryMemory(func() (uint64, bool) { return uint64(16 * (64 + 1024 + len("batch")) * 4), true })
	c.refreshRecoveryMemory(c.recoveryAdmission.Load(), time.Now())
	var wg sync.WaitGroup
	grants := make(chan bool, 32)
	for range 32 {
		wg.Go(func() { grants <- c.HasRecoveryCapacityFor("batch", 1) })
	}
	wg.Wait()
	close(grants)
	count := 0
	for grant := range grants {
		if grant {
			count++
		}
	}
	if count != 16 {
		t.Fatal("concurrent checks overspent byte reservation", count)
	}
	if !c.RecoveryBlocked() {
		t.Fatal("remaining workers not counted as pressure")
	}
}

func TestRecoveryMemoryDoesNotReallocateExistingSeries(t *testing.T) {
	c := New()
	c.AddRecovered(points.OnePoint("existing", 1, 1))
	c.SetRecoveryMemory(func() (uint64, bool) { return 4 * 1024, true })
	if !c.HasRecoveryCapacityFor("existing", 1) {
		t.Fatal("existing series charged another metric header")
	}
	window, remaining, _ := c.RecoveryCapacity()
	perPoint := c.recoveryAdmission.Load().limits.Load().bytesPerPoint
	if remaining != window-perPoint {
		t.Fatal("existing series charged more than point capacity", window, remaining, perPoint)
	}
	if c.HasRecoveryCapacityFor("new", 1) {
		t.Fatal("new series was not charged name/header allocation")
	}
}

func TestRecoveryMemoryPreservesByteCeiling(t *testing.T) {
	c := New()
	c.SetMaxBytes(1)
	c.SetRecoveryMemory(func() (uint64, bool) { return 1 << 30, true })
	c.AddRecovered(points.OnePoint("full", 1, 1))
	if c.HasRecoveryCapacityFor("batch", 1) {
		t.Fatal("explicit byte safety ceiling bypassed")
	}
}

func TestBlockedRecoveryReleasesBatchEligibility(t *testing.T) {
	c := New()
	c.SetMaxSize(20)
	if err := c.SetWriteoutBatching(100, time.Hour); err != nil {
		t.Fatal(err)
	}
	c.Add(points.OnePoint("buffered", 1, 1))
	if c.WriteoutReady("buffered") {
		t.Fatal("fixture was not held for batching")
	}
	c.SetRecoveryMemory(func() (uint64, bool) { return 0, true })
	if c.HasRecoveryCapacityFor("next", 1) {
		t.Fatal("pressure fixture admitted")
	}
	if !c.WriteoutReady("buffered") || c.WriteoutState().Eligible != 1 {
		t.Fatal("drain waited for a separate batching threshold")
	}
}
