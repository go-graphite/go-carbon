package v2core

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/cockroachdb/pebble/sstable"
	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2part"
)

// mergeByteDifferentialDigest is the SHA-256 of every merged part file and
// descriptor that TestMergeByteDifferential writes, with aligned dense runs
// storing slots only. mergeLogicalDifferentialDigest is the SHA-256 of what
// those parts decode to: keys, metadata envelopes, dense-run records and the
// descriptor without Bytes and SHA256. Re-record the byte digest for a format
// change only while the logical digest still matches; never re-record the
// logical digest for a performance change.
const (
	mergeByteDifferentialDigest    = "c73d672546832696a04259d332876f95a5d86e4020c0323d7eefe2d4824fb720"
	mergeLogicalDifferentialDigest = "ca27b5aac5153e0532809b44c70603c7be9310d6a6287c3cf335e826a976fb30"
)

// TestMergeByteDifferential runs multi-tier merges (seal parts, merges of
// merges plus a late part) over length-tied, prefix-sharing and non-ASCII
// names with late points, expired names, reset orders and a TTL floor, and
// pins every merged part's decoded content and bytes to the recorded digests.
func TestMergeByteDifferential(t *testing.T) {
	saved := mergePartID
	defer func() { mergePartID = saved }()
	var serial byte
	mergePartID = func(id *v2part.PartID) error {
		serial++
		*id = v2part.PartID{0xA5, serial}
		return nil
	}
	sum, logical := sha256.New(), sha256.New()
	for seed := uint64(1); seed <= 3; seed++ {
		for _, group := range []v2part.Group{"live", "g.long-group"} {
			mergeByteDifferentialCase(t, seed, group, sum, logical)
		}
	}
	if got := hex.EncodeToString(logical.Sum(nil)); got != mergeLogicalDifferentialDigest {
		t.Fatalf("decoded merge output changed: digest %s, want %s", got, mergeLogicalDifferentialDigest)
	}
	got := hex.EncodeToString(sum.Sum(nil))
	if got != mergeByteDifferentialDigest {
		t.Fatalf("merged part bytes changed: digest %s, want %s", got, mergeByteDifferentialDigest)
	}
}

func mergeByteDifferentialCase(t *testing.T, seed uint64, group v2part.Group, sum, logical io.Writer) {
	t.Helper()
	rng := rand.New(rand.NewPCG(seed, uint64(len(group))))
	dir := t.TempDir()
	if err := os.Mkdir(filepath.Join(dir, "parts"), 0700); err != nil {
		t.Fatal(err)
	}
	// Names share long prefixes, tie on length, and include escaped bytes.
	var names []v2part.ID
	seen := map[v2part.ID]bool{}
	for len(names) < 600 {
		var name string
		switch rng.IntN(4) {
		case 0:
			name = fmt.Sprintf("svc.%03d.m%d", rng.IntN(40), rng.IntN(10))
		case 1:
			name = fmt.Sprintf("svc.%03d.m%d", rng.IntN(40), 10+rng.IntN(90))
		case 2:
			name = fmt.Sprintf("a.very.long.shared.prefix.service.host-%02d.counter.%05d", rng.IntN(30), rng.IntN(100000))
		default:
			name = fmt.Sprintf("esc.\\x%02x.\xc3\xa9t\xc3\xa9.%d", rng.IntN(256), rng.IntN(1000))
		}
		if id := v2part.ID(name); !seen[id] {
			seen[id] = true
			names = append(names, id)
		}
	}
	config := sealrollup.Config{Method: sealrollup.Last, XFilesFactor: .5, Archives: []sealrollup.Archive{{Step: 1, Count: 600}, {Step: 60, Count: 60}}}
	digest, err := sealrollup.PolicyDigest(config)
	if err != nil {
		t.Fatal(err)
	}
	policies := map[v2part.ID][]byte{}
	for _, name := range names {
		policy, e := json.Marshal(RollupPolicy{Name: name, Group: group, Config: config})
		if e != nil {
			t.Fatal(e)
		}
		policies[name] = policy
	}
	// Seal i covers fine slots [base, base+span); late seals revisit older slots.
	writeInput := func(i int, base, span int64, keep func(int) bool, meta func(int, *RollupMetadata)) v2part.Descriptor {
		var rows []KV
		order := uint64(i + 1)
		for n, name := range names {
			if !keep(n) {
				continue
			}
			k := v2part.Key{Group: group, VShard: v2part.Route(name), Part: v2part.PartID{byte(seed), byte(i + 1)}, Metric: name}
			put := func(archive uint32, slot int64, time int64) {
				k.Archive, k.BlockStart = archive, slot
				enc, e := v2part.Encode(k)
				if e != nil {
					t.Fatal(e)
				}
				r := sealrollup.Record{Delta: sealrollup.Delta{Archive: int(archive), Slot: slot, Order: order, Sample: sealrollup.Sample{Time: time, Bits: math.Float64bits(float64(n*1000 + i))}}}
				rows = append(rows, KV{Key: enc, Value: sealrollup.EncodeRecord(r)})
			}
			// Some names write a long run (exact-run copy path), others few points.
			count := span
			if n%3 != 0 {
				count = 1 + int64(rng.IntN(4))
			}
			for s := base; s < base+count; s++ {
				put(0, s, 100000+s)
			}
			put(1, (base+count-1)/60, 100000+base+count-1)
			md := RollupMetadata{Version: 2, ID: uint64(n + 1), Policy: policies[name], Frontier: sealrollup.Frontier{Order: order, MaxTime: 100000 + base + count - 1, Policy: digest}, LastUpdate: int64(i+1) * 1e9, Revision: order}
			if meta != nil {
				meta(n, &md)
			}
			k.Archive, k.BlockStart = v2part.MetadataArchive, 0
			enc, e := v2part.Encode(k)
			if e != nil {
				t.Fatal(e)
			}
			rows = append(rows, KV{Key: enc, Value: AppendRollupMetadata(nil, md)})
		}
		slices.SortFunc(rows, func(a, b KV) int { return bytes.Compare(a.Key, b.Key) })
		path := filepath.Join(dir, fmt.Sprintf("in-%d.sst", i))
		d, e := WriteDenseRollupPart(path, rows, v2part.Provenance{Stream: "merge-bytes", Incarnation: v2part.PartID{7}, MinOrder: order, MaxOrder: order})
		if e != nil {
			t.Fatal(e)
		}
		if e = os.Rename(path, filepath.Join(dir, "parts", hex.EncodeToString(d.SHA256[:])+".sst")); e != nil {
			t.Fatal(e)
		}
		return d
	}
	merge := func(name string, inputs []v2part.Descriptor, ttl int64) v2part.Descriptor {
		path := filepath.Join(dir, name+".sst")
		d, e := mergePartFiles(context.Background(), dir, path, inputs, ttl, nil)
		if e != nil {
			t.Fatalf("seed%d %s %s: %v", seed, group, name, e)
		}
		b, e := os.ReadFile(path)
		if e != nil {
			t.Fatal(e)
		}
		_, _ = sum.Write(b)
		_, _ = fmt.Fprintf(sum, "%+v\n", d)
		mergeLogicalWrite(t, path, d, logical)
		if e = os.Rename(path, filepath.Join(dir, "parts", hex.EncodeToString(d.SHA256[:])+".sst")); e != nil {
			t.Fatal(e)
		}
		return d
	}
	sparse := func(mod, rem int) func(int) bool {
		return func(n int) bool { return int(v2part.Route(names[n]))%mod == rem || n%7 == rem }
	}
	// Tier 1: sparse seals.
	s0 := writeInput(0, 0, 20, sparse(3, 0), nil)
	s1 := writeInput(1, 20, 20, sparse(3, 1), nil)
	s2 := writeInput(2, 40, 20, sparse(3, 2), nil)
	s3 := writeInput(3, 60, 20, func(int) bool { return true }, nil)
	s4 := writeInput(4, 80, 20, func(n int) bool { return n%2 == 0 }, nil)
	m1 := merge("m1", []v2part.Descriptor{s0, s1, s2}, 0)
	m2 := merge("m2", []v2part.Descriptor{s3, s4}, 0)
	// Tier 2: a late seal rewrites older slots, expires and resets some names.
	late := writeInput(5, 10, 30, func(n int) bool { return n%5 != 1 }, func(n int, md *RollupMetadata) {
		md.Revision = 100
		if n%13 == 0 {
			md.ResetOrder = 2
		}
		if n%11 == 0 {
			md.Expired, md.ResetOrder = true, md.Frontier.Order
		}
	})
	m3 := merge("m3", []v2part.Descriptor{m1, m2, late}, 0)
	// Tier 3: TTL rewrite of the merged part with one more late seal.
	s6 := writeInput(6, 5, 20, func(n int) bool { return n%4 == 3 }, nil)
	merge("m4", []v2part.Descriptor{m3, s6}, 100000+50)
}

// mergeLogicalWrite hashes what a merged part decodes to: every key and
// trailer, each decoded metadata envelope re-encoded, every decoded dense-run
// record, other values raw, and the descriptor without Bytes and SHA256.
func mergeLogicalWrite(t *testing.T, path string, d v2part.Descriptor, h io.Writer) {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	readable, err := sstable.NewSimpleReadable(f)
	if err != nil {
		t.Fatal(err)
	}
	r, err := sstable.NewReader(readable, sstable.ReaderOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	it, err := r.NewIter(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer it.Close()
	for k, lv := it.First(); k != nil; k, lv = it.Next() {
		v, _, err := lv.Value(nil)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = fmt.Fprintf(h, "K %x %d\n", k.UserKey, k.Trailer)
		dk, err := v2part.Decode(k.UserKey)
		if err != nil {
			t.Fatal(err)
		}
		switch {
		case dk.Archive == v2part.MetadataArchive:
			m, err := DecodeRollupMetadata(dk.Metric, dk.Group, v)
			if err != nil {
				t.Fatal(err)
			}
			_, _ = fmt.Fprintf(h, "M %x\n", AppendRollupMetadata(nil, m))
		case len(v) > 4 && string(v[:4]) == "GCD\x01":
			rs, err := v2part.DecodeSlots(dk.Archive, dk.BlockStart, v)
			if err != nil {
				t.Fatal(err)
			}
			for _, rec := range rs {
				_, _ = fmt.Fprintf(h, "R %+v\n", rec)
			}
		default:
			_, _ = fmt.Fprintf(h, "V %x\n", v)
		}
	}
	d.Bytes, d.SHA256 = 0, v2part.Digest{}
	_, _ = fmt.Fprintf(h, "D %+v\n", d)
}
