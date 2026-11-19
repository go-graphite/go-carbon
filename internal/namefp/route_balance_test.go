package namefp

import (
	"bufio"
	"compress/gzip"
	"encoding/binary"
	"io"
	"os"
	"strings"
	"testing"
)

// TestRouteRealNameBalance streams ten million canonical names from an FLC2
// capture. Opt-in: captures contain deployment-specific metric names.
func TestRouteRealNameBalance(t *testing.T) {
	path := os.Getenv("NAMEFP_BALANCE_FLC")
	if path == "" {
		t.Skip("set NAMEFP_BALANCE_FLC to an FLC2 gzip capture")
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	z, err := gzip.NewReader(f)
	if err != nil {
		t.Fatal(err)
	}
	defer z.Close()
	r := bufio.NewReaderSize(z, 4<<20)
	var magic [6]byte
	if _, err = io.ReadFull(r, magic[:]); err != nil || string(magic[:]) != "\x7fflc02" {
		t.Fatal("invalid FLC2 header", err)
	}
	owners := []int{2, 4, 8, 16, 32, 48, 64, 96, 128, 256}
	oldCounts, newCounts := make([][]int, len(owners)), make([][]int, len(owners))
	for i, n := range owners {
		oldCounts[i] = make([]int, n)
		newCounts[i] = make([]int, n)
	}
	const total = 10_000_000
	n, bytes := 0, 0
	var length [8]byte
	var scratch []byte
	for n < total {
		if _, err = io.ReadFull(r, length[:]); err != nil {
			t.Fatal("need ten million names", n, err)
		}
		size := binary.BigEndian.Uint64(length[:])
		if size > 1<<20 {
			t.Fatal("invalid FLC name length", size)
		}
		if cap(scratch) < int(size)+33 {
			scratch = make([]byte, int(size)+33)
		}
		record := scratch[:int(size)+33]
		if _, err = io.ReadFull(r, record); err != nil {
			t.Fatal(err)
		}
		path := strings.TrimPrefix(string(record[:size]), "/")
		if !strings.HasSuffix(path, ".wsp") {
			continue
		}
		name := strings.ReplaceAll(strings.TrimSuffix(path, ".wsp"), "/", ".")
		old := uint64(14695981039346656037)
		for i := range name {
			old = (old ^ uint64(name[i])) * 1099511628211
		}
		next := Route(name)
		for i, count := range owners {
			oldCounts[i][old%uint64(count)]++
			newCounts[i][next%uint64(count)]++
		}
		n++
		bytes += len(name)
	}
	for i, count := range owners {
		oldMax, newMax := 0, 0
		for j := range count {
			oldMax = max(oldMax, oldCounts[i][j])
			newMax = max(newMax, newCounts[i][j])
		}
		mean := float64(n) / float64(count)
		t.Logf("names=%d mean_bytes=%.2f owners=%d FNV_max_mean=%.6f XXH3_max_mean=%.6f relative=%.6f", n, float64(bytes)/float64(n), count, float64(oldMax)/mean, float64(newMax)/mean, float64(newMax)/float64(oldMax))
		if float64(newMax) > 1.01*float64(oldMax) {
			t.Errorf("owners %d: XXH3 max/mean more than 1%% above FNV", count)
		}
	}
}
