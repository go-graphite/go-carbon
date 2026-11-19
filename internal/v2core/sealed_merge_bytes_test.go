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

	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2part"
)

// mergeDecodedDifferentialDigest is the SHA-256 of the same merged parts'
// content independent of run layout and envelope placement (every slot as a
// singleton record, every envelope at its own key, the descriptor without file
// bytes, digest or KV count), recorded on github atimofieiev/v2 e9795c4c62e3
// before merges repacked runs. Both envelope forms must reproduce it.
const mergeDecodedDifferentialDigest = "266d1b35f1f24860c11a88321445ea9debcfb4591932bab481fcdb14cef56f42"

// TestMergeByteDifferential runs multi-tier merges (seal parts, merges of
// merges plus a late part) over length-tied, prefix-sharing and non-ASCII
// names with late points, expired names, reset orders and a TTL floor, and
// pins decoded content to the unchanged digest and physical bytes/descriptors
// to an independent repeat. Physical bytes change with an order codec revision.
func TestMergeByteDifferential(t *testing.T) {
	if coreTestProcess(t) {
		return
	}
	saved := mergePartID
	defer func() { mergePartID = saved }()
	defer v2part.LegacyEnvelopeKV.Store(false)
	for _, legacy := range []bool{true, false} {
		var physical string
		for repeat := 0; repeat < 2; repeat++ {
			v2part.LegacyEnvelopeKV.Store(legacy)
			var serial byte
			mergePartID = func(id *v2part.PartID) error {
				serial++
				*id = v2part.PartID{0xA5, serial}
				return nil
			}
			sum, decoded := sha256.New(), sha256.New()
			for seed := uint64(1); seed <= 3; seed++ {
				for _, group := range []v2part.Group{"live", "g.long-group"} {
					mergeByteDifferentialCase(t, seed, group, sum, decoded)
				}
			}
			if got := hex.EncodeToString(decoded.Sum(nil)); got != mergeDecodedDifferentialDigest {
				t.Errorf("legacy=%v: merged part content changed: decoded digest %s, want %s", legacy, got, mergeDecodedDifferentialDigest)
			}
			got := hex.EncodeToString(sum.Sum(nil))
			if repeat == 0 {
				physical = got
			} else if got != physical {
				t.Errorf("legacy=%v: repeated merge bytes changed: %s != %s", legacy, got, physical)
			}
		}
	}
}

func mergeByteDifferentialCase(t *testing.T, seed uint64, group v2part.Group, sum, decoded io.Writer) {
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
		writeDecodedPart(t, decoded, path, d)
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
