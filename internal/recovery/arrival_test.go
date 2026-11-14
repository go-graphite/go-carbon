package recovery

import (
	"os"
	"path/filepath"
	"testing"
)

func TestArrivalCompanionVersionAndChecksum(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cache.test.bin")
	if got, err := ReadArrivals(path); got != nil || err != nil {
		t.Fatal(got, err)
	}
	raw := appendArrival([]byte("GCAGE001"), "metric", 123456789)
	if err := os.WriteFile(ArrivalPath(path), raw, 0600); err != nil {
		t.Fatal(err)
	}
	got, err := ReadArrivals(path)
	if err != nil || got["metric"] != 123456789 {
		t.Fatal(got, err)
	}
	raw[len(raw)-1] ^= 1
	if err := os.WriteFile(ArrivalPath(path), raw, 0600); err != nil {
		t.Fatal(err)
	}
	if got, err := ReadArrivals(path); got != nil || err == nil {
		t.Fatal("accepted corrupt metadata", got, err)
	}
	raw = []byte("GCAGE999")
	if err := os.WriteFile(ArrivalPath(path), raw, 0600); err != nil {
		t.Fatal(err)
	}
	if got, err := ReadArrivals(path); got != nil || err == nil {
		t.Fatal("accepted unknown version", got, err)
	}
}
