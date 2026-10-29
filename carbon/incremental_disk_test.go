package carbon

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/persister"
	whisper "github.com/go-graphite/go-whisper"
	"golang.org/x/sync/semaphore"
)

func expireMigrationDisk(m *incrementalStoreMigration) {
	m.diskMu.Lock()
	m.diskChecked = time.Time{}
	m.diskMu.Unlock()
}

func checkMigrationDiskReleased(t *testing.T, m *incrementalStoreMigration) {
	t.Helper()
	m.diskMu.Lock()
	defer m.diskMu.Unlock()
	if m.diskReserved != 0 || m.spaceWaiting.Load() != 0 {
		t.Fatalf("leaked reservations: bytes=%d waiters=%d", m.diskReserved, m.spaceWaiting.Load())
	}
}

func TestIncrementalDiskConcurrentReservations(t *testing.T) {
	for _, completed := range []bool{false, true} {
		t.Run(fmt.Sprintf("completed=%t", completed), func(t *testing.T) {
			const workers, capacity = 1024, 64
			const reserve, reservation = 1024, 48
			var available atomic.Int64
			available.Store(reserve + capacity*reservation)
			m := &incrementalStoreMigration{diskFree: func(string) (uint64, error) {
				return uint64(max(0, available.Load())), nil
			}}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			admitted := make(chan func(), workers)
			failures := make(chan error, workers)
			var group sync.WaitGroup
			group.Add(workers)
			for range workers {
				go func() {
					defer group.Done()
					release, err := m.waitDisk(ctx, "", reserve+reservation, reservation)
					if err != nil {
						failures <- err
						return
					}
					if completed {
						if free := available.Add(-reservation); free < reserve {
							t.Errorf("admitted below reserve: free=%d reserve=%d", free, reserve)
						}
						release()
						release = nil
					}
					admitted <- release
				}()
			}
			waitMigration(t, func() bool { return m.spaceWaiting.Load() == workers-capacity })
			cancel()
			group.Wait()
			close(admitted)
			close(failures)
			if len(admitted) != capacity {
				t.Fatalf("admitted %d workers, want %d", len(admitted), capacity)
			}
			for err := range failures {
				if !errors.Is(err, context.Canceled) {
					t.Fatal(err)
				}
			}
			for release := range admitted {
				if release != nil {
					release()
				}
			}
			checkMigrationDiskReleased(t, m)
		})
	}
}

func TestIncrementalDiskCompletedWritesStayCharged(t *testing.T) {
	var available, calls atomic.Uint64
	available.Store(1000)
	m := &incrementalStoreMigration{diskFree: func(string) (uint64, error) {
		calls.Add(1)
		return available.Load(), nil
	}}
	for range 2 {
		release, err := m.waitDisk(t.Context(), "", 500, 400)
		if err != nil {
			t.Fatal(err)
		}
		available.Store(available.Load() - 400) // The output remains on disk after release.
		release()
		// Keep this sample fresh regardless of scheduler delays in the test.
		m.diskChecked = time.Now().Add(time.Hour)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		release, err := m.waitDisk(ctx, "", 500, 400)
		if release != nil {
			release()
		}
		done <- err
	}()
	waitMigration(t, func() bool { return m.spaceWaiting.Load() == 1 })
	if calls.Load() != 1 {
		t.Fatal("fresh sample was not shared")
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("completed writes were admitted twice: %v", err)
	}
	// A later sample discovers space reclaimed by another user of the store.
	available.Store(1000)
	expireMigrationDisk(m)
	release, err := m.waitDisk(t.Context(), "", 500, 400)
	if err != nil {
		t.Fatal(err)
	}
	release()
	if calls.Load() != 2 {
		t.Fatal("expired sample was not refreshed")
	}
	checkMigrationDiskReleased(t, m)
}

func TestIncrementalDiskReleaseDuringRefresh(t *testing.T) {
	started, finish := make(chan struct{}), make(chan struct{})
	var calls atomic.Uint64
	m := &incrementalStoreMigration{diskFree: func(string) (uint64, error) {
		call := calls.Add(1)
		if call == 1 {
			return 1000, nil
		}
		if call == 2 {
			close(started)
			<-finish
		}
		return 600, nil
	}}
	first, err := m.waitDisk(t.Context(), "", 500, 400)
	if err != nil {
		t.Fatal(err)
	}
	expireMigrationDisk(m)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		release, err := m.waitDisk(ctx, "", 500, 400)
		if release != nil {
			release()
		}
		done <- err
	}()
	<-started
	released := make(chan struct{})
	go func() { first(); close(released) }()
	select {
	case <-released:
	case <-time.After(time.Second):
		close(finish)
		t.Fatal("statfs blocked a reservation release")
	}
	// Cancellation must not wait for another worker's filesystem syscall or
	// count that syscall as low-space waiting.
	waitCtx, stop := context.WithCancel(t.Context())
	var waiters sync.WaitGroup
	for range 128 {
		waiters.Go(func() {
			release, err := m.waitDisk(waitCtx, "", 500, 400)
			if release != nil {
				release()
			}
			if !errors.Is(err, context.Canceled) {
				t.Errorf("refresh waiter returned %v", err)
			}
		})
	}
	stop()
	waiters.Wait()
	if calls.Load() != 2 || m.spaceWaiting.Load() != 0 {
		t.Fatal("refresh was duplicated or counted as low disk space")
	}
	close(finish)
	waitMigration(t, func() bool { return m.spaceWaiting.Load() == 1 })
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("refresh forgot a reservation released during statfs: %v", err)
	}
	checkMigrationDiskReleased(t, m)
}

func TestIncrementalDiskWaitResumesAfterReclaim(t *testing.T) {
	var available atomic.Uint64
	available.Store(1000)
	m := &incrementalStoreMigration{diskFree: func(string) (uint64, error) {
		return available.Load(), nil
	}}
	release, err := m.waitDisk(t.Context(), "", 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	release()
	available.Store(200)
	expireMigrationDisk(m)
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		release, err := m.waitDisk(ctx, "", 500, 400)
		if release != nil {
			release()
		}
		done <- err
	}()
	waitMigration(t, func() bool { return m.spaceWaiting.Load() == 1 })
	available.Store(1000)
	if err := <-done; err != nil {
		t.Fatalf("reclaimed space did not wake the waiter: %v", err)
	}
	checkMigrationDiskReleased(t, m)
}

func TestIncrementalDiskRefreshFailureAndCancellation(t *testing.T) {
	failure := errors.New("statfs failed")
	var calls atomic.Uint64
	m := &incrementalStoreMigration{diskFree: func(string) (uint64, error) {
		if calls.Add(1) == 1 {
			return 0, failure
		}
		return 1000, nil
	}}
	for range 2 {
		if release, err := m.waitDisk(t.Context(), "", 500, 400); release != nil || !errors.Is(err, failure) {
			t.Fatalf("failed sample admitted work: %v", err)
		}
		m.diskChecked = time.Now().Add(time.Hour)
	}
	if calls.Load() != 1 {
		t.Fatal("failed refresh was duplicated")
	}
	expireMigrationDisk(m)
	release, err := m.waitDisk(t.Context(), "", 500, 400)
	if err != nil {
		t.Fatal(err)
	}
	release()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if release, err := m.waitDisk(ctx, "", 1, 1); release != nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled worker admitted: %v", err)
	}
	checkMigrationDiskReleased(t, m)
}

func TestIncrementalDiskReleaseAfterConversionError(t *testing.T) {
	conf := NewConfig()
	conf.Whisper.DataDir = t.TempDir()
	conf.Whisper.StoreMigrationBytesInFlight = 1 << 20
	conf.Whisper.StoreMigrationMinFreeBytes = 100
	w, err := whisper.Create(filepath.Join(conf.Whisper.DataDir, "metric.wsp"),
		whisper.NewRetentionsNoPointer([]whisper.Retention{whisper.NewRetention(1, 60)}), whisper.Average, .5)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	p := persister.NewWhisper(conf.Whisper.DataDir, nil, nil, nil, nil, nil, nil)
	m := &incrementalStoreMigration{diskFree: func(string) (uint64, error) { return 2 << 20, nil }}
	bytes := semaphore.NewWeighted(conf.Whisper.StoreMigrationBytesInFlight)
	// Conversion fails after admission because no incremental store is attached.
	if err := m.convertAdmitted(t.Context(), p, conf, bytes, "metric"); err != nil {
		t.Fatal(err)
	}
	if m.skipped.Load() != 1 || m.working.Load() != 0 || m.active.Load() != 0 {
		t.Fatal("failed conversion leaked admission or was not skipped")
	}
	checkMigrationDiskReleased(t, m)
	if !bytes.TryAcquire(conf.Whisper.StoreMigrationBytesInFlight) {
		t.Fatal("failed conversion leaked the byte semaphore")
	}
	bytes.Release(conf.Whisper.StoreMigrationBytesInFlight)
}
