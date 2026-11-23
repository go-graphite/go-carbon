package v2core

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/go-graphite/go-carbon/internal/partsformat"
	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2dict"
	"github.com/go-graphite/go-carbon/internal/v2part"
)

// TestIDKeyedMergeMatchesNameKeyed runs the same multi-tier merges (sparse
// seals, a late seal with resets and expired names, a TTL rewrite) over
// name-keyed parts and over the same parts re-keyed by dictionary IDs that
// are assigned in shuffled order, so ID order differs from name order. Every
// ID-keyed merge output must be ID-keyed, carry the newest input generation,
// and hold exactly the name-keyed output's records and envelopes, re-keyed.
// A deleted name's records drop; an ID the dictionary never assigned fails.
func TestIDKeyedMergeMatchesNameKeyed(t *testing.T) {
	if coreTestProcess(t) {
		return
	}
	saved, savedKeys := mergePartID, v2part.IDKeys.Load()
	defer func() { mergePartID, _ = saved, v2part.IDKeys.Swap(savedKeys) }()
	v2part.IDKeys.Store(true)
	var next v2part.PartID
	mergePartID = func(id *v2part.PartID) error { *id = next; return nil }

	const group = v2part.Group("live")
	rng := rand.New(rand.NewPCG(5, 9))
	var names []v2part.ID
	seen := map[v2part.ID]bool{}
	for len(names) < 300 {
		var name string
		switch rng.IntN(3) {
		case 0:
			name = fmt.Sprintf("svc.%03d.m%d", rng.IntN(40), rng.IntN(100))
		case 1:
			name = fmt.Sprintf("a.very.long.shared.prefix.host-%02d.counter.%05d", rng.IntN(30), rng.IntN(100000))
		default:
			name = fmt.Sprintf("esc.\\x%02x.\xc3\xa9t\xc3\xa9.%d", rng.IntN(256), rng.IntN(1000))
		}
		if id := v2part.ID(name); !seen[id] {
			seen[id] = true
			names = append(names, id)
		}
	}
	dict, err := v2dict.Open(t.TempDir(), v2dict.Options{NoMerge: true})
	if err != nil {
		t.Fatal(err)
	}
	defer dict.Close()
	ids := map[v2part.ID]uint32{}
	for _, n := range rng.Perm(len(names)) {
		e, _, err := dict.Append(string(group), v2part.Route(names[n]), []byte(names[n]), 0)
		if err != nil {
			t.Fatal(err)
		}
		ids[names[n]] = e.ID
	}
	if err = dict.Sync(); err != nil {
		t.Fatal(err)
	}
	generation := dict.Generation()

	config := sealrollup.Config{Method: sealrollup.Last, XFilesFactor: .5, Archives: []sealrollup.Archive{{Step: 1, Count: 600}, {Step: 60, Count: 60}}}
	digest, err := sealrollup.PolicyDigest(config)
	if err != nil {
		t.Fatal(err)
	}
	nameDir, idDir := t.TempDir(), t.TempDir()
	for _, dir := range []string{nameDir, idDir} {
		if err = os.Mkdir(filepath.Join(dir, "parts"), 0700); err != nil {
			t.Fatal(err)
		}
	}
	install := func(dir, path string, d v2part.Descriptor) {
		t.Helper()
		if err := os.Rename(path, filepath.Join(dir, "parts", hex.EncodeToString(d.SHA256[:])+".sst")); err != nil {
			t.Fatal(err)
		}
	}
	// idKey re-keys a decoded name key by its dictionary ID.
	idKey := func(k v2part.Key) []byte {
		t.Helper()
		k.ByID, k.Local, k.Metric = true, ids[k.Metric], ""
		b, err := v2part.Encode(k)
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	// expanded lists a part's singleton records and envelopes, name keys
	// re-keyed by ID, in key order.
	expanded := func(path string, d v2part.Descriptor) []KV {
		t.Helper()
		var out []KV
		err := v2part.ReadExpanded(context.Background(), path, &d, func(k v2part.Key, value []byte) error {
			var key []byte
			if k.ByID {
				key, _ = v2part.Encode(k)
			} else {
				key = idKey(k)
			}
			out = append(out, KV{Key: key, Value: bytes.Clone(value)})
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		slices.SortFunc(out, func(a, b KV) int { return bytes.Compare(a.Key, b.Key) })
		return out
	}
	partPath := func(dir string, d v2part.Descriptor) string {
		return filepath.Join(dir, "parts", hex.EncodeToString(d.SHA256[:])+".sst")
	}
	// writeInput writes seal i over fine slots [base, base+span) twice: name
	// keyed, and re-keyed through the dictionary by the ID-keyed writer.
	writeInput := func(i int, base, span int64, keep func(int) bool, meta func(int, *RollupMetadata)) (v2part.Descriptor, v2part.Descriptor) {
		t.Helper()
		var rows []KV
		order := uint64(i + 1)
		for n, name := range names {
			if !keep(n) {
				continue
			}
			k := v2part.Key{Group: group, VShard: v2part.Route(name), Part: v2part.PartID{9, byte(i + 1)}, Metric: name}
			put := func(archive uint32, slot int64, time int64) {
				k.Archive, k.BlockStart = archive, slot
				enc, err := v2part.Encode(k)
				if err != nil {
					t.Fatal(err)
				}
				r := sealrollup.Record{Delta: sealrollup.Delta{Archive: int(archive), Slot: slot, Order: order, Sample: sealrollup.Sample{Time: time, Bits: math.Float64bits(float64(n*1000 + i))}}}
				rows = append(rows, KV{Key: enc, Value: sealrollup.EncodeRecord(r)})
			}
			count := span
			if n%3 != 0 {
				count = 1 + int64(n%4)
			}
			for s := base; s < base+count; s++ {
				put(0, s, 100000+s)
			}
			put(1, (base+count-1)/60, 100000+base+count-1)
			policy, err := json.Marshal(RollupPolicy{Name: name, Group: group, Config: config})
			if err != nil {
				t.Fatal(err)
			}
			md := RollupMetadata{Version: 2, ID: uint64(n + 1), Policy: policy, Frontier: sealrollup.Frontier{Order: order, MaxTime: 100000 + base + count - 1, Policy: digest}, LastUpdate: int64(i+1) * 1e9, Revision: order}
			if meta != nil {
				meta(n, &md)
			}
			k.Archive, k.BlockStart = v2part.MetadataArchive, 0
			enc, err := v2part.Encode(k)
			if err != nil {
				t.Fatal(err)
			}
			rows = append(rows, KV{Key: enc, Value: AppendRollupMetadata(nil, md)})
		}
		slices.SortFunc(rows, func(a, b KV) int { return bytes.Compare(a.Key, b.Key) })
		path := filepath.Join(nameDir, fmt.Sprintf("in-%d.sst", i))
		d, err := WriteDenseRollupPart(path, rows, v2part.Provenance{Stream: "id-merge", Incarnation: v2part.PartID{7}, MinOrder: order, MaxOrder: order})
		if err != nil {
			t.Fatal(err)
		}
		install(nameDir, path, d)
		file := expanded(partPath(nameDir, d), d)
		spec := v2part.Spec{Scope: d.Scope, Source: d.Source, MinTime: d.Coverage.MinTime, MaxTime: d.Coverage.MaxTime, Policies: d.Coverage.Policies, MinExpiry: d.Coverage.MinExpiry, MaxExpiry: d.Coverage.MaxExpiry}
		path = filepath.Join(idDir, fmt.Sprintf("in-%d.sst", i))
		w, err := v2part.NewDenseWriter(path, spec)
		if err != nil {
			t.Fatal(err)
		}
		defer w.Abort()
		if err = w.IDKeyed(generation); err != nil {
			t.Fatal(err)
		}
		var shards []uint16
		for _, kv := range file {
			k, err := v2part.DecodeKey(kv.Key)
			if err != nil {
				t.Fatal(err)
			}
			if len(shards) == 0 || shards[len(shards)-1] != k.VShard {
				shards = append(shards, k.VShard)
			}
			if err = w.SetEncoded(kv.Key, kv.Value); err != nil {
				t.Fatal(err)
			}
		}
		di, err := w.Close()
		if err != nil {
			t.Fatal(err)
		}
		di.VShards = shards
		if err = di.Validate(); err != nil {
			t.Fatal(err)
		}
		install(idDir, path, di)
		return d, di
	}
	merges := 0
	// merge runs one merge over both forms and checks the ID-keyed output.
	merge := func(nameIn, idIn []v2part.Descriptor, retire int64) (v2part.Descriptor, v2part.Descriptor) {
		t.Helper()
		merges++
		next = v2part.PartID{0xA5, byte(merges)}
		path := filepath.Join(nameDir, "merged.sst")
		d, err := mergePartFiles(context.Background(), nameDir, path, nameIn, retire, nil)
		if err != nil {
			t.Fatal(err)
		}
		install(nameDir, path, d)
		path = filepath.Join(idDir, "merged.sst")
		di, err := mergeParts(context.Background(), idDir, path, idIn, retire, 0, true, nil, dict, nil)
		if err != nil {
			t.Fatalf("merge %d: %v", merges, err)
		}
		install(idDir, path, di)
		if di.DictGeneration != generation || !partsformat.IsIDKey(di.FirstKey) {
			t.Fatalf("merge %d: output generation %d, want ID-keyed at %d", merges, di.DictGeneration, generation)
		}
		if di.Coverage.Records != d.Coverage.Records || di.Coverage.MinTime != d.Coverage.MinTime || di.Coverage.MaxTime != d.Coverage.MaxTime || di.Coverage.MinExpiry != d.Coverage.MinExpiry || di.Coverage.MaxExpiry != d.Coverage.MaxExpiry || !slices.Equal(di.VShards, d.VShards) {
			t.Fatalf("merge %d: ID-keyed coverage %+v differs from %+v", merges, di.Coverage, d.Coverage)
		}
		want, got := expanded(partPath(nameDir, d), d), expanded(partPath(idDir, di), di)
		if len(got) != len(want) {
			t.Fatalf("merge %d: %d ID-keyed rows, want %d", merges, len(got), len(want))
		}
		for i := range want {
			if !bytes.Equal(got[i].Key, want[i].Key) || !bytes.Equal(got[i].Value, want[i].Value) {
				t.Fatalf("merge %d: row %d differs", merges, i)
			}
		}
		if d.Coverage.Records == 0 {
			t.Fatalf("merge %d: empty", merges)
		}
		return d, di
	}
	sparse := func(mod, rem int) func(int) bool {
		return func(n int) bool { return int(v2part.Route(names[n]))%mod == rem || n%7 == rem }
	}
	s0, s0i := writeInput(0, 0, 20, sparse(3, 0), nil)
	s1, s1i := writeInput(1, 20, 20, sparse(3, 1), nil)
	s2, s2i := writeInput(2, 40, 20, sparse(3, 2), nil)
	s3, s3i := writeInput(3, 60, 20, func(int) bool { return true }, nil)
	s4, s4i := writeInput(4, 80, 20, func(n int) bool { return n%2 == 0 }, nil)
	// A forced (full) window stops where name-keyed parts give way to ID-keyed ones.
	db, err := pebble.Open("", &pebble.Options{FS: vfs.NewMem()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	snapshot := db.NewSnapshot()
	defer snapshot.Close()
	bindings := map[string][]byte{}
	for _, d := range []v2part.Descriptor{s0, s1, s2, s3i, s4i} {
		b, err := d.MarshalBinary()
		if err != nil {
			t.Fatal(err)
		}
		bindings[hex.EncodeToString(d.SHA256[:])] = b
	}
	owner := &coreOwner{partSource: s0.Source, gc: &sealedPartGC{inventory: &RollupInventory{bindings: bindings}}}
	chosen, err := owner.selectMergeLimit(snapshot, math.MaxUint64, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(chosen) != 3 || chosen[0].SHA256 != s0.SHA256 || chosen[2].SHA256 != s2.SHA256 {
		t.Fatalf("forced merge window spans %d parts across key generations, want s0..s2", len(chosen))
	}
	m1, m1i := merge([]v2part.Descriptor{s0, s1, s2}, []v2part.Descriptor{s0i, s1i, s2i}, 0)
	m2, m2i := merge([]v2part.Descriptor{s3, s4}, []v2part.Descriptor{s3i, s4i}, 0)
	late, latei := writeInput(5, 10, 30, func(n int) bool { return n%5 != 1 }, func(n int, md *RollupMetadata) {
		md.Revision = 100
		if n%13 == 0 {
			md.ResetOrder = 2
		}
		if n%11 == 0 {
			md.Expired, md.ResetOrder = true, md.Frontier.Order
		}
	})
	m3, m3i := merge([]v2part.Descriptor{m1, m2, late}, []v2part.Descriptor{m1i, m2i, latei}, 0)
	s6, s6i := writeInput(6, 5, 20, func(n int) bool { return n%4 == 3 }, nil)
	merge([]v2part.Descriptor{m3, s6}, []v2part.Descriptor{m3i, s6i}, 100000+50)

	if _, err = mergeParts(context.Background(), idDir, filepath.Join(idDir, "mixed.sst"), []v2part.Descriptor{m3, s6i}, 0, 0, true, nil, dict, nil); err == nil {
		t.Fatal("merge of mixed key generations succeeded")
	}
	// A deleted name drops with all its records; the rest is unchanged.
	dropped := names[0]
	if ok, err := dict.Delete(string(group), v2part.Route(dropped), []byte(dropped)); err != nil || !ok {
		t.Fatal("delete", ok, err)
	}
	next = v2part.PartID{0xA5, 3} // m3i's part, so rows compare byte for byte
	path := filepath.Join(idDir, "dropped.sst")
	dd, err := mergeParts(context.Background(), idDir, path, []v2part.Descriptor{m1i, m2i, latei}, 0, 0, true, nil, dict, nil)
	if err != nil {
		t.Fatal(err)
	}
	prefix := idKey(v2part.Key{Group: group, VShard: v2part.Route(dropped), Partition: m3i.Scope.Partition, Part: next, Metric: dropped})
	prefix = prefix[:len(prefix)-5]
	var want []KV
	for _, kv := range expanded(partPath(idDir, m3i), m3i) {
		if !bytes.HasPrefix(kv.Key, prefix) {
			want = append(want, kv)
		}
	}
	got := expanded(path, dd)
	if len(want) == len(expanded(partPath(idDir, m3i), m3i)) || len(got) != len(want) {
		t.Fatalf("deleted name: %d rows, want %d", len(got), len(want))
	}
	for i := range want {
		if !bytes.Equal(got[i].Key, want[i].Key) || !bytes.Equal(got[i].Value, want[i].Value) {
			t.Fatalf("deleted name: row %d differs", i)
		}
	}
	// A dictionary that never assigned the parts' IDs fails the merge.
	empty, err := v2dict.Open(t.TempDir(), v2dict.Options{NoMerge: true})
	if err != nil {
		t.Fatal(err)
	}
	defer empty.Close()
	if _, err = mergeParts(context.Background(), idDir, filepath.Join(idDir, "foreign.sst"), []v2part.Descriptor{m1i, m2i}, 0, 0, true, nil, empty, nil); err == nil || !strings.Contains(err.Error(), "never assigned") {
		t.Fatalf("merge through a foreign dictionary: %v, want a never-assigned ID error", err)
	}
}

// TestMergeNamesCatalogLive: a merge keeps an ID-keyed metric without a
// dictionary read when the catalog identity holds that ID as the name's
// current one; any other ID (a revived name's old one, an identity that has
// not resolved its ID) is decided by the dictionary.
func TestMergeNamesCatalogLive(t *testing.T) {
	dict, err := v2dict.Open(t.TempDir(), v2dict.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer dict.Close()
	const name = "svc.catalog.live"
	_, raw := rollupTestPolicy(t, name, "1s:1d", sealrollup.Last, .5)
	cat := &PolicyCatalog{}
	h, err := cat.intern(7, name, raw)
	if err != nil {
		t.Fatal(err)
	}
	group, vshard := string(h.policy.Group), h.vshard
	old, _, err := dict.Append(group, vshard, []byte(name), 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := dict.Delete(group, vshard, []byte(name)); err != nil {
		t.Fatal(err)
	}
	cur, _, err := dict.Append(group, vshard, []byte(name), 0)
	if err != nil {
		t.Fatal(err)
	}
	key := func(id uint32) v2part.Key {
		return v2part.Key{Group: h.policy.Group, VShard: vshard, ByID: true, Local: id}
	}
	n := mergeNames{dict: dict, identities: cat}
	for _, c := range []struct {
		hint uint32
		id   uint32
		want bool
	}{
		{0, cur.ID, true}, {0, old.ID, false}, // no hint: the dictionary decides
		{cur.ID + 1, cur.ID, true}, {cur.ID + 1, old.ID, false}, // the current ID only
	} {
		h.dictID.Store(c.hint)
		if live, err := n.live(key(c.id), 7); err != nil || live != c.want {
			t.Fatalf("hint %d, ID %d: live %v %v, want %v", c.hint, c.id, live, err, c.want)
		}
	}
	// The hint answers without the dictionary: a dictionary that never
	// assigned the ID would fail the read.
	empty, err := v2dict.Open(t.TempDir(), v2dict.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer empty.Close()
	h.dictID.Store(cur.ID + 1)
	if live, err := (&mergeNames{dict: empty, identities: cat}).live(key(cur.ID), 7); err != nil || !live {
		t.Fatalf("catalog hint not used: %v %v", live, err)
	}
}
