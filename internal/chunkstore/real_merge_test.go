package chunkstore

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"runtime"
	"sync"
	"testing"

	"github.com/cockroachdb/pebble/sstable"
)

// The optional immutable SST supplies actual integer/float operands without
// checking production data into the repository. The same test can be built
// against a source overlay of the previous implementation for allocation and
// byte comparisons. It never opens or modifies a live database.
func TestRealSSTMergeWorkspaces(t *testing.T) {
	path := os.Getenv("CHUNKSTORE_SST_FIXTURE")
	if path == "" {
		t.Skip("set CHUNKSTORE_SST_FIXTURE to an immutable captured SST")
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	readable, err := sstable.NewSimpleReadable(f)
	if err != nil {
		t.Fatal(err)
	}
	r, err := sstable.NewReader(readable, sstable.ReaderOptions{MergerName: "go-carbon.chunks.exact.v1"})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	it, err := r.NewIter(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	var samples [][]byte
	integers := 0
	for key, value := it.First(); key != nil && len(samples) < 2048; key, value = it.Next() {
		if len(key.UserKey) != 23 || key.UserKey[0] != 'c' {
			continue
		}
		data, _, err := value.Value(nil)
		if err != nil {
			t.Fatal(err)
		}
		var c chunk
		if len(data) == 0 || data[0] != exactChunkVersion || decodeChunk(data, &c) != nil || c.Deltas >= materializeAt/2 {
			continue
		}
		samples = append(samples, append([]byte(nil), data...))
		if data[1]&exactChunkInteger != 0 {
			integers++
		}
	}
	if err := it.Error(); err != nil {
		t.Fatal(err)
	}
	if err := it.Close(); err != nil {
		t.Fatal(err)
	}
	if len(samples) < 128 {
		t.Fatalf("too few real chunk operands: %d", len(samples))
	}
	merge := func(i int) ([]byte, error) {
		m, err := exactChunkMerger.Merge(nil, samples[i])
		if err != nil {
			return nil, err
		}
		if i%2 == 0 {
			err = m.MergeNewer(samples[(i+1)%len(samples)])
		} else {
			err = m.MergeOlder(samples[(i+1)%len(samples)])
		}
		if err != nil {
			return nil, err
		}
		result, closer, err := m.Finish(i%2 == 0)
		if closer != nil {
			_ = closer.Close()
			return nil, errors.New("merge output unexpectedly borrowed")
		}
		return result, err
	}
	golden := make([][32]byte, len(samples))
	checksum := sha256.New()
	for i, data := range samples {
		out, err := merge(i)
		if err != nil {
			t.Fatal(err)
		}
		golden[i] = sha256.Sum256(out)
		checksum.Write(data)
		checksum.Write(out)
	}
	// Finish's output remains owned even after thousands of workspace reuses.
	retained, err := merge(0)
	if err != nil {
		t.Fatal(err)
	}
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	const workers, rounds = 16, 512
	var group sync.WaitGroup
	for worker := range workers {
		group.Go(func() {
			for n := range rounds {
				i := (worker*37 + n*17) % len(samples)
				out, err := merge(i)
				if err != nil {
					t.Error(err)
					return
				}
				if sha256.Sum256(out) != golden[i] {
					t.Error("concurrent workspace reuse changed encoded bytes")
					return
				}
			}
		})
	}
	group.Wait()
	runtime.ReadMemStats(&after)
	if sha256.Sum256(retained) != golden[0] {
		t.Fatal("later merges changed previously returned bytes")
	}
	result, err := json.Marshal(map[string]any{
		"sampled_real_operands": len(samples), "operations": workers * rounds,
		"integer_operands": integers, "float_fallback_operands": len(samples) - integers,
		"allocated_bytes":         after.TotalAlloc - before.TotalAlloc,
		"mallocs":                 after.Mallocs - before.Mallocs,
		"input_and_output_sha256": hex.EncodeToString(checksum.Sum(nil)),
		"scope":                   "Same immutable production operands and mixed merge direction; isolated codec allocation/byte gate, not whole-service performance",
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Log(string(result))
}
