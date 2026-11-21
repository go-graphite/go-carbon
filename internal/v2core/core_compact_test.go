package v2core

import (
	"encoding/binary"
	"fmt"
	"math/rand/v2"
	"path/filepath"
	"slices"
	"testing"

	"github.com/cockroachdb/pebble"
)

func TestCoreCompactEveryRow(t *testing.T) {
	rng := rand.New(rand.NewPCG(813, 92))
	var w coreCompactWorkspace
	for _, n := range []int{1, 2, 255, 256, 4096} {
		rows := make([]row, n)
		for i := range rows {
			rows[i] = row{ID: rng.Uint64(), Time: int64(rng.Uint64()), Bits: rng.Uint64(), Seq: rng.Uint64()}
		}
		for i, bits := range []uint64{0, 1, 1 << 63, 0x7ff0000000000000, 0xfff0000000000000, 0x7ff8000000000042} {
			if i < n {
				rows[i].Bits = bits
			}
		}
		encoded, err := w.encode(rows)
		if err != nil {
			t.Fatal(err)
		}
		var got []row
		if err = w.visit(encoded, func(r row) error { got = append(got, r); return nil }); err != nil || !slices.Equal(rows, got) {
			t.Fatalf("n=%d roundtrip %v", n, err)
		}
		step := 1
		if n > 256 {
			step = 257
		}
		var d RawDecoder
		key := coreKey(rows[0], 0, 0, 0)
		binary.BigEndian.PutUint32(key[17:21], 1)
		var decoded []row
		if err = d.Visit(key, encoded, func(r row) error { decoded = append(decoded, r); return nil }); err != nil || !slices.Equal(rows, decoded) {
			t.Fatal("streaming decoder", err)
		}
		for i := 0; i < len(encoded); i += step {
			if err = w.visit(encoded[:i], func(row) error { return nil }); err == nil {
				t.Fatalf("accepted truncation %d/%d", i, len(encoded))
			}
		}
	}
}
func TestCoreCompactDifferentialAndReopen(t *testing.T) {
	t.Parallel()
	for _, appendParts := range []bool{false, true} {
		for _, blind := range []bool{false, true} {
			t.Run(fmt.Sprint(appendParts, blind), func(t *testing.T) {
				t.Parallel()
				cfg := coreTestConfig()
				cfg.AppendParts = appendParts
				cfg.BlindBatch = blind
				var digests []string
				for _, codec := range []string{"legacy", "run1"} {
					cfg.RawCodec = codec
					root := t.TempDir()
					e, err := openCore(root, cfg)
					if err != nil {
						t.Fatal(err)
					}
					for b := 1; b <= 9; b++ {
						for p := 0; p < cfg.Producers; p++ {
							if err = e.submit(p, coreRows(p, b), uint64(b), true); err != nil {
								t.Fatal(err)
							}
						}
					}
					var count int
					for _, o := range e.owners {
						snap := o.db.NewSnapshot()
						err = ScanRaw(snap, func(Row) error { count++; return nil })
						_ = snap.Close()
						if err != nil {
							t.Fatal(err)
						}
					}
					if count != 9*cfg.Producers*43 {
						t.Fatalf("scan count %d", count)
					}
					if err = e.close(); err != nil {
						t.Fatal(err)
					}
					digest, err := coreDigest(root, cfg)
					if err != nil {
						t.Fatal(err)
					}
					digests = append(digests, digest)
					e, err = openCore(root, cfg)
					if err != nil {
						t.Fatal(err)
					}
					if err = e.close(); err != nil {
						t.Fatal(err)
					}
					cfg.RawCodec = "unknown"
					if e, err = openCore(root, cfg); err == nil {
						e.close()
						t.Fatal("accepted unknown codec")
					}
					cfg.RawCodec = codec
					other := cfg
					if codec == "legacy" {
						other.RawCodec = "run1"
					} else {
						other.RawCodec = "legacy"
					}
					if e, err = openCore(root, other); err == nil {
						e.close()
						t.Fatal("accepted changed codec")
					}
				}
				if digests[0] != digests[1] {
					t.Fatal("codec changed row digest", digests)
				}
			})
		}
	}
}
func TestCoreCompactCorruption(t *testing.T) {
	t.Parallel()
	var w coreCompactWorkspace
	b, err := w.encode([]row{{ID: 9, Time: -1, Bits: 1, Seq: 99}})
	if err != nil {
		t.Fatal(err)
	}
	bad := slices.Clone(b)
	binary.BigEndian.PutUint32(bad[4:], ^uint32(0))
	if w.visit(bad, func(row) error { return nil }) == nil {
		t.Fatal("accepted corrupt lengths")
	}
	cfg := coreTestConfig()
	cfg.RawCodec = "run1"
	cfg.Shards = 1
	root := t.TempDir()
	e, err := openCore(root, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err = e.close(); err != nil {
		t.Fatal(err)
	}
	db, err := pebble.Open(filepath.Join(root, "owner-000"), &pebble.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	key := coreKey(row{ID: 9}, 0, 0, 0)
	binary.BigEndian.PutUint32(key[17:21], 1)
	if err = db.Set(key, bad, pebble.Sync); err != nil {
		t.Fatal(err)
	}
	snap := db.NewSnapshot()
	defer snap.Close()
	if _, err = Read(snap, 9); err == nil {
		t.Fatal("read accepted corrupt run")
	}
}
