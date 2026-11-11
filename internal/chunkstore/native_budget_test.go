package chunkstore

import (
	"errors"
	"testing"
)

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
