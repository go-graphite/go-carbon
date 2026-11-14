package chunkstore

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestNativeImportConcurrencyBudgetAndCancellation(t *testing.T) {
	for _, tc := range []struct {
		imports, workers int
		memory           int64
		invalid          bool
	}{{1, 4, 4 << 30, false}, {4, 8, 32 << 30, false}, {8, 8, 64 << 30, false},
		{0, 2, 2 << 30, true}, {-1, 2, 2 << 30, true}, {9, 2, 32 << 30, true},
		{4, 8, (32 << 30) - 1, true}, {8, 33, 512 << 30, true}} {
		if err := ValidateNativeImportBudget(tc.imports, tc.workers, tc.memory); (err != nil) != tc.invalid {
			t.Fatalf("%+v: %v", tc, err)
		}
	}
	s, err := Open(t.TempDir(), Options{NativeImportConcurrency: 2, NativeCombineWorkers: 4, NativeCombineMemory: 8 << 30, CacheSize: 1 << 20, MemTableSize: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if s.nativeWorkerMemory() != 1<<30 {
		t.Fatal("budget multiplied")
	}
	a, err := s.admitNative(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	b, err := s.admitNative(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer b()
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	if _, err := s.NativeImport(ctx, NativeRange{}, nativeUnreadable{}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("queued import read input or escaped admission", err)
	}
	a()
	c, err := s.admitNative(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	c()
	s.CancelNativeTransfers()
	ctx, stop := s.NativeTransferContext(context.Background())
	defer stop()
	if _, err := s.admitNative(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestNativeCombineOptions(t *testing.T) {
	for _, tc := range []struct {
		workers int
		memory  int64
		invalid bool
	}{{0, 0, false}, {1, 1 << 30, false}, {8, 8 << 30, false}, {32, 32 << 30, false}, {-1, 0, true}, {33, 64 << 30, true}, {4, 2 << 30, true}, {1, -1, true}} {
		s, err := Open(t.TempDir(), Options{NativeCombineWorkers: tc.workers, NativeCombineMemory: tc.memory, CacheSize: 1 << 20, MemTableSize: 1 << 20})
		if s != nil {
			defer s.Close()
		}
		if (err != nil) != tc.invalid {
			t.Fatalf("%+v: %v", tc, err)
		}
		if tc.invalid {
			continue
		}
		workers, memory := tc.workers, tc.memory
		if workers == 0 {
			workers = 2
		}
		if memory == 0 {
			memory = 2 << 30
		}
		if s.nativeCombineWorkers != workers || s.nativeWorkerMemory() != memory/int64(workers) {
			t.Fatal("incorrect budget partition")
		}
		// Unset import concurrency preserves the original one-slot gates and
		// total/workers scratch partition, including non-default worker budgets.
		s.initNativeContext()
		if s.NativeImportConcurrency() != 1 || cap(s.nativeAdmission) != 1 || cap(s.nativeExportAdmission) != 1 {
			t.Fatal("unset import concurrency changed admission")
		}
		release, err := s.admitNative(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_, err = s.admitNative(ctx)
		release()
		if !errors.Is(err, context.Canceled) {
			t.Fatal("second import escaped the default gate", err)
		}
	}
}

func TestNativeDefaultBudgetValidationMatchesSerial(t *testing.T) {
	for _, workers := range []int{-1, 0, 1, 2, 4, 32, 33} {
		for _, memory := range []int64{-1, 0, 1 << 30, (4 << 30) - 1, 4 << 30, 32 << 30} {
			before := ValidateNativeCombineBudget(workers, memory)
			after := ValidateNativeImportBudget(1, workers, memory)
			if (before == nil) != (after == nil) || (before != nil && before.Error() != after.Error()) {
				t.Fatalf("default changed validation for %d workers / %d bytes: %v -> %v", workers, memory, before, after)
			}
		}
	}
}

func TestNativeCombineConfiguredMemoryAdmission(t *testing.T) {
	r := TransferMetric{Config: MetricConfig{Name: "budget", Retentions: []Retention{{Step: 1, Count: 3_000_000}}}}
	s := &Store{nativeCombineWorkerMemory: 1 << 30}
	var skip *NativeSkip
	if err := s.admitNativeCombineRecords(r, r, true); !errors.As(err, &skip) {
		t.Fatal("missing memory admission", err)
	}
	s.nativeCombineWorkerMemory = 2 << 30
	if err := s.admitNativeCombineRecords(r, r, true); err != nil {
		t.Fatal("configured memory not used", err)
	}
	s.nativeCombineWorkerMemory = 1 << 30
	if err := s.admitNativeCombineRecords(r, r, true); !errors.As(err, &skip) {
		t.Fatal("memory share not enforced", err)
	}
}
