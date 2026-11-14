package chunkstore

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"math"
	"testing"

	"github.com/cockroachdb/pebble"
)

// Cache partitioning must not change persisted points, including overwrites,
// out-of-order updates and floating-point payloads across flush and reopen.
func TestBlockCacheShardParity(t *testing.T) {
	var oracle [32]byte
	for _, shards := range []int{64, 256} {
		t.Run(fmt.Sprint(shards), func(t *testing.T) {
			dir := t.TempDir()
			opts := Options{ExactBlocks: true, CacheSize: 64 << 20, CacheShards: shards}
			s, now := openTestStore(t, dir, opts)
			ctx := context.Background()
			for metric := range 16 {
				name := fmt.Sprintf("cache.parity.%d", metric)
				createTestMetric(t, s, name)
				for round := range 8 {
					points := make([]Point, 32)
					for i := range points {
						value := math.Float64frombits(uint64(0x3ff0000000000000) + uint64(metric*256+round*32+i))
						if i == 0 {
							value = math.Copysign(0, -1)
						}
						points[i] = Point{Timestamp: int64(now - now%60 - (31-i+round*7)*60), Value: value}
					}
					if err := s.UpdateMany(ctx, name, points); err != nil {
						t.Fatal(err)
					}
					if err := s.db.Flush(); err != nil {
						t.Fatal(err)
					}
				}
			}
			closeTestStore(t, s)
			s, _ = openTestStore(t, dir, opts)
			defer closeTestStore(t, s)
			it, err := s.db.NewIter(&pebble.IterOptions{})
			if err != nil {
				t.Fatal(err)
			}
			defer it.Close()
			h := sha256.New()
			var size [8]byte
			count := 0
			for it.First(); it.Valid(); it.Next() {
				value, err := it.ValueAndErr()
				if err != nil {
					t.Fatal(err)
				}
				for _, b := range [][]byte{it.Key(), value} {
					binary.LittleEndian.PutUint64(size[:], uint64(len(b)))
					h.Write(size[:])
					h.Write(b)
				}
				count++
			}
			if err := it.Error(); err != nil {
				t.Fatal(err)
			}
			if count == 0 {
				t.Fatal("empty persisted image")
			}
			got := [32]byte(h.Sum(nil))
			if shards == 64 {
				oracle = got
			} else if got != oracle {
				t.Fatalf("persisted bytes changed: %x != %x", got, oracle)
			}
		})
	}
}
