package v2part

import (
	"bytes"
	"cmp"
	"context"
	"crypto/sha256"
	"fmt"
	"maps"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2dict"
)

// TestIDKeyCodec round-trips GCI2 keys, including the envelope, through the
// encoder, DecodeKey and the reusing key decoder, and checks that the
// name-only decoder rejects them, so readers without ID support fail loudly.
func TestIDKeyCodec(t *testing.T) {
	s := fixtureSpec()
	var d KeyDecoder
	for _, k := range []Key{
		{Group: s.Scope.Group, VShard: 7, Partition: -7, Part: s.Scope.Part, ByID: true, Local: 0, Archive: 0, BlockStart: 0},
		{Group: s.Scope.Group, VShard: 7, Partition: -7, Part: s.Scope.Part, ByID: true, Local: 0, Archive: 3, BlockStart: math.MaxUint32},
		{Group: s.Scope.Group, VShard: 7, Partition: -7, Part: s.Scope.Part, ByID: true, Local: 0, Archive: MetadataArchive},
		{Group: s.Scope.Group, VShard: 4095, Partition: 9, Part: s.Scope.Part, ByID: true, Local: math.MaxUint32 - 1, Archive: 254, BlockStart: 12345},
	} {
		b, err := Encode(k)
		if err != nil {
			t.Fatal(err)
		}
		if len(b) != 43+len(k.Group) {
			t.Fatalf("GCI2 key is %d bytes, want %d", len(b), 43+len(k.Group))
		}
		for _, got := range []func() (Key, error){func() (Key, error) { return DecodeKey(b) }, func() (Key, error) { return d.Decode(b) }} {
			if g, err := got(); err != nil || g != k {
				t.Fatalf("decode %x = %+v, %v; want %+v", b, g, err, k)
			}
		}
		if _, err := Decode(b); err == nil {
			t.Fatal("name-key decoder accepted a GCI2 key")
		}
	}
	bad := Key{Group: s.Scope.Group, VShard: 7, Partition: -7, Part: s.Scope.Part, ByID: true}
	for name, mutate := range map[string]func(*Key){
		"archive 255":    func(k *Key) { k.Archive = 255 },
		"slot 2^32":      func(k *Key) { k.BlockStart = math.MaxUint32 + 1 },
		"negative slot":  func(k *Key) { k.BlockStart = -1 },
		"ordinal":        func(k *Key) { k.Ordinal = 1 },
		"name in ID key": func(k *Key) { k.Metric = "example.metric" },
		"vshard":         func(k *Key) { k.VShard = VirtualShards },
	} {
		k := bad
		mutate(&k)
		if _, err := Encode(k); err == nil {
			t.Errorf("%s: encoded", name)
		}
	}
	b, _ := Encode(bad)
	if _, err := DecodeKey(b[:len(b)-1]); err == nil {
		t.Error("short GCI2 key decoded")
	}
}

type idFixtureMetric struct {
	name   ID
	vshard uint16
	id     uint32
}

func idFixtureSpec() Spec {
	spec := fixtureSpec()
	spec.Scope.VShard = GroupVShard
	spec.Encoding = DenseEncoding
	policy := []byte(`{"step":600,"count":144,"method":"average","xff":0.5}`)
	spec.Policies = append(spec.Policies, ArchivePolicy{Archive: 1, Digest: sha256.Sum256(policy), Definition: policy})
	return spec
}

// idFixture admits names over eight vshards to a dictionary in a scrambled
// (admission) order, so ID order differs from name order.
func idFixture(t *testing.T, group Group) (*v2dict.Dict, []idFixtureMetric) {
	t.Helper()
	dict, err := v2dict.Open(t.TempDir(), v2dict.Options{NoMerge: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { dict.Close() })
	var ms []idFixtureMetric
	for n := 0; len(ms) < 600; n++ {
		name := ID(fmt.Sprintf("svc.%d.m%d", n%97, n))
		if v := Route(name); v < 8 {
			ms = append(ms, idFixtureMetric{name: name, vshard: v})
		}
	}
	slices.SortFunc(ms, func(a, b idFixtureMetric) int {
		return cmp.Compare(fmt.Sprintf("%x", sha256.Sum256([]byte(a.name))), fmt.Sprintf("%x", sha256.Sum256([]byte(b.name))))
	})
	for i := range ms {
		e, created, err := dict.Append(string(group), ms[i].vshard, []byte(ms[i].name), 0)
		if err != nil || !created {
			t.Fatal(err, created)
		}
		ms[i].id = e.ID
	}
	if err := dict.Sync(); err != nil {
		t.Fatal(err)
	}
	return dict, ms
}

// idFixtureRecords is a metric's records: archive 0 slots plus a few archive
// 1 slots, including a slot with two versions.
func idFixtureRecords(i int) []sealrollup.Record {
	var rs []sealrollup.Record
	for s := 0; s < 3+i%5; s++ {
		rs = append(rs, sealrollup.Record{Delta: sealrollup.Delta{Archive: 0, Slot: int64(100 + s), Order: 8, Sample: sealrollup.Sample{Time: 1700000000 + int64(60*s), Bits: math.Float64bits(float64(i*10 + s))}}})
	}
	for s := 0; s < 1+i%2; s++ {
		rs = append(rs, sealrollup.Record{Delta: sealrollup.Delta{Archive: 1, Slot: int64(7 + s), Order: 9, Sample: sealrollup.Sample{Time: 1700000000 + int64(600*s), Bits: math.Float64bits(float64(i) / 3)}}})
	}
	return rs
}

// writeIDFixture writes ms (in its order) as a name-keyed part, or as an
// ID-keyed part when generation is nonzero.
func writeIDFixture(path string, spec Spec, ms []idFixtureMetric, generation uint64) (Descriptor, error) {
	w, err := NewDenseWriter(path, spec)
	if err != nil {
		return Descriptor{}, err
	}
	defer w.Abort()
	if generation != 0 {
		if err := w.IDKeyed(generation); err != nil {
			return Descriptor{}, err
		}
	}
	for _, m := range ms {
		i := int(m.id)
		for _, r := range idFixtureRecords(i) {
			if generation != 0 {
				err = w.SetPlannedIDRecord(m.vshard, m.id, &r)
			} else {
				err = w.SetPlannedRecord(m.vshard, 0, m.name, &r)
			}
			if err != nil {
				return Descriptor{}, err
			}
		}
		envelope := []byte("envelope-" + string(m.name))
		if generation != 0 {
			err = w.SetPlannedIDMetadata(m.vshard, m.id, envelope)
		} else {
			err = w.SetPlannedMetadata(m.vshard, 0, m.name, envelope)
		}
		if err != nil {
			return Descriptor{}, err
		}
	}
	return w.Close()
}

// logicalStream reads a part's expanded singleton records keyed by their
// GCP3 key; ID keys resolve through dict.
func logicalStream(t *testing.T, path string, d Descriptor, dict *v2dict.Dict) map[string]string {
	t.Helper()
	out := map[string]string{}
	err := ReadExpanded(context.Background(), path, &d, func(k Key, v []byte) error {
		if k.ByID {
			name, e, ok := dict.LookupID(string(k.Group), k.VShard, k.Local, nil)
			if !ok || e.Deleted() {
				return fmt.Errorf("unknown ID %d in vshard %d", k.Local, k.VShard)
			}
			k.ByID, k.Local, k.Metric = false, 0, ID(name)
		}
		b, err := Encode(k)
		if err != nil {
			return err
		}
		out[string(b)] = string(v)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

// TestIDKeyedPartRoundTrip writes the same records as a GCP3 part (name
// order) and a GCI2 part (vshard, ID order). Through the dictionary, the ID
// part's logical stream equals the name part's exactly; its descriptor is
// schema 3 with the dictionary generation; a merge-style copy reproduces the
// ID part byte for byte. Mismatched schemas, swapped IDs and bad input fail.
func TestIDKeyedPartRoundTrip(t *testing.T) {
	spec := idFixtureSpec()
	dict, ms := idFixture(t, spec.Scope.Group)
	if _, err := writeIDFixture(filepath.Join(t.TempDir(), "off.sst"), spec, ms[:1], 1); err == nil {
		t.Fatal("ID-keyed part written with IDKeys off")
	}
	IDKeys.Store(true)
	t.Cleanup(func() { IDKeys.Store(false) })
	gen := dict.Generation()
	if gen == 0 {
		t.Fatal("zero dictionary generation after Sync")
	}
	byName := slices.Clone(ms)
	slices.SortFunc(byName, func(a, b idFixtureMetric) int {
		return cmp.Or(cmp.Compare(a.vshard, b.vshard), cmp.Compare(len(a.name), len(b.name)), cmp.Compare(a.name, b.name))
	})
	byID := slices.Clone(ms)
	slices.SortFunc(byID, func(a, b idFixtureMetric) int {
		return cmp.Or(cmp.Compare(a.vshard, b.vshard), cmp.Compare(a.id, b.id))
	})
	if slices.EqualFunc(byName, byID, func(a, b idFixtureMetric) bool { return a.name == b.name }) {
		t.Fatal("fixture needs ID order to differ from name order")
	}
	dir := t.TempDir()
	namePath, idPath := filepath.Join(dir, "names.sst"), filepath.Join(dir, "ids.sst")
	nd, err := writeIDFixture(namePath, spec, byName, 0)
	if err != nil {
		t.Fatal(err)
	}
	id, err := writeIDFixture(idPath, spec, byID, gen)
	if err != nil {
		t.Fatal(err)
	}
	if id.Version != IDFileVersion || id.KeyVersion != IDKeyVersion || id.DictGeneration != gen || nd.Version != GroupFileVersion || nd.DictGeneration != 0 {
		t.Fatalf("descriptors: name %d/%d/%d, ID %d/%d/%d", nd.Version, nd.KeyVersion, nd.DictGeneration, id.Version, id.KeyVersion, id.DictGeneration)
	}
	if id.Coverage.Names != uint64(len(ms)) || id.Coverage.Names != nd.Coverage.Names || id.Coverage.Records != nd.Coverage.Records {
		t.Fatalf("coverage: name %+v, ID %+v", nd.Coverage, id.Coverage)
	}
	for _, d := range []Descriptor{nd, id} {
		b, err := d.MarshalBinary()
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(b), "DictGeneration") != (d.DictGeneration != 0) {
			t.Fatalf("descriptor JSON: %s", b)
		}
		if p, err := ParseDescriptor(b); err != nil || p.DictGeneration != d.DictGeneration {
			t.Fatal(p.DictGeneration, err)
		}
	}
	want := logicalStream(t, namePath, nd, nil)
	got := logicalStream(t, idPath, id, dict)
	if len(want) == 0 || !maps.Equal(got, want) {
		t.Fatalf("ID part logical stream differs: %d vs %d records", len(got), len(want))
	}
	nb, _ := os.Stat(namePath)
	ib, _ := os.Stat(idPath)
	t.Logf("part bytes: names %d, IDs %d (%.1f%%)", nb.Size(), ib.Size(), 100*float64(ib.Size()-nb.Size())/float64(nb.Size()))
	if ib.Size() >= nb.Size() {
		t.Errorf("ID part (%d B) not smaller than name part (%d B)", ib.Size(), nb.Size())
	}

	// Merge-style copy: runs through SetSlotsEncoded, envelopes as KVs.
	copyPart := func(src string, d Descriptor, dst string) Descriptor {
		w, err := NewDenseWriter(dst, spec)
		if err != nil {
			t.Fatal(err)
		}
		defer w.Abort()
		if d.DictGeneration != 0 {
			if err := w.IDKeyed(d.DictGeneration); err != nil {
				t.Fatal(err)
			}
		}
		err = Read(context.Background(), src, &d, func(k Key, v []byte) error {
			b, err := Encode(k)
			if err != nil {
				return err
			}
			if k.Archive == MetadataArchive {
				return w.SetEncoded(b, v)
			}
			return w.SetSlotsEncoded(b, v)
		})
		if err != nil {
			t.Fatal(err)
		}
		out, err := w.Close()
		if err != nil {
			t.Fatal(err)
		}
		return out
	}
	for _, c := range []struct {
		path string
		d    Descriptor
	}{{namePath, nd}, {idPath, id}} {
		out := copyPart(c.path, c.d, c.path+".copy")
		a, _ := os.ReadFile(c.path)
		b, _ := os.ReadFile(c.path + ".copy")
		if !bytes.Equal(a, b) || out.SHA256 != c.d.SHA256 {
			t.Errorf("copy of %s differs (%d vs %d bytes)", filepath.Base(c.path), len(b), len(a))
		}
	}

	// Mutations.
	shifted := map[string]string{}
	err = ReadExpanded(context.Background(), idPath, &id, func(k Key, v []byte) error {
		name, _, ok := dict.LookupID(string(k.Group), k.VShard, k.Local^1, nil)
		if ok {
			k.ByID, k.Local, k.Metric = false, 0, ID(name)
			b, _ := Encode(k)
			shifted[string(b)] = string(v)
		}
		return nil
	})
	if err == nil && maps.Equal(shifted, want) {
		t.Error("swapped dictionary IDs not detected")
	}
	for name, mutate := range map[string]func(*Descriptor){
		"schema 3 without generation": func(d *Descriptor) { d.DictGeneration = 0 },
		"key version 3 on ID part":    func(d *Descriptor) { d.KeyVersion = KeyVersion },
		"schema 2 with generation":    func(d *Descriptor) { d.Version = GroupFileVersion },
		"name first key":              func(d *Descriptor) { d.FirstKey = nd.FirstKey },
	} {
		d := id
		d.Coverage.Policies = slices.Clone(d.Coverage.Policies)
		mutate(&d)
		if err := d.Validate(); err == nil {
			t.Errorf("%s: descriptor validated", name)
		}
	}
	// Switched off, this build accepts exactly what the previous one did.
	idJSON, err := id.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	IDKeys.Store(false)
	if _, err := ParseDescriptor(idJSON); err == nil {
		t.Error("schema 3 descriptor parsed with IDKeys off")
	}
	if err := Read(context.Background(), idPath, nil, nil); err == nil {
		t.Error("ID-keyed part read with IDKeys off")
	}
	IDKeys.Store(true)
	nameWithGen := nd
	nameWithGen.DictGeneration = gen
	if err := nameWithGen.Validate(); err == nil {
		t.Error("GCP3 descriptor with a generation validated")
	}
	if err := Read(context.Background(), idPath, &nd, nil); err == nil {
		t.Error("ID part read against a name-part descriptor")
	}
	// Bad writer input.
	for name, write := range map[string]func(*DenseWriter) error{
		"unordered IDs": func(w *DenseWriter) error {
			r := idFixtureRecords(0)[0]
			if err := w.SetPlannedIDRecord(1, 5, &r); err != nil {
				return err
			}
			return w.SetPlannedIDRecord(1, 4, &r)
		},
		"name input": func(w *DenseWriter) error {
			r := idFixtureRecords(0)[0]
			return w.SetPlannedRecord(byName[0].vshard, 0, byName[0].name, &r)
		},
		"archive 255": func(w *DenseWriter) error {
			r := idFixtureRecords(0)[0]
			r.Archive = 255
			return w.SetPlannedIDRecord(1, 5, &r)
		},
		"slot 2^32": func(w *DenseWriter) error {
			r := idFixtureRecords(0)[0]
			r.Slot = math.MaxUint32 + 1
			return w.SetPlannedIDRecord(1, 5, &r)
		},
	} {
		w, err := NewDenseWriter(filepath.Join(dir, "bad.sst"), spec)
		if err != nil {
			t.Fatal(err)
		}
		if err := w.IDKeyed(gen); err != nil {
			t.Fatal(err)
		}
		err = write(w)
		if err == nil {
			_, err = w.Close()
		} else {
			_ = w.Abort()
		}
		if err == nil {
			t.Errorf("%s: written", name)
		}
		_ = os.Remove(filepath.Join(dir, "bad.sst"))
	}
	w, err := NewDenseWriter(filepath.Join(dir, "bad.sst"), spec)
	if err != nil {
		t.Fatal(err)
	}
	r := idFixtureRecords(0)[0]
	if err := w.SetPlannedIDRecord(1, 5, &r); err == nil {
		t.Error("ID input accepted by a name-keyed writer")
	}
	_ = w.Abort()
}

// TestRekeyDense copies a donor's ID-keyed part into a live owner whose
// dictionary admitted the names in another order and already holds some of
// them. Through v2dict Rekey, the rewritten part equals, byte for byte, the
// part the owner writes for the same records under its own IDs, minus the
// names the donor deleted.
func TestRekeyDense(t *testing.T) {
	IDKeys.Store(true)
	t.Cleanup(func() { IDKeys.Store(false) })
	spec := idFixtureSpec()
	group := string(spec.Scope.Group)
	donor, ms := idFixture(t, spec.Scope.Group)
	deleted := map[ID]bool{}
	for i := 0; i < len(ms); i += 7 {
		if ok, err := donor.Delete(group, ms[i].vshard, []byte(ms[i].name)); err != nil || !ok {
			t.Fatal(ok, err)
		}
		deleted[ms[i].name] = true
	}
	if err := donor.Sync(); err != nil {
		t.Fatal(err)
	}
	// The owner already admitted every third name, in reverse order.
	owner, err := v2dict.Open(t.TempDir(), v2dict.Options{NoMerge: true})
	if err != nil {
		t.Fatal(err)
	}
	defer owner.Close()
	for i := len(ms) - 1; i >= 0; i -= 3 {
		if _, _, err := owner.Append(group, ms[i].vshard, []byte(ms[i].name), 0); err != nil {
			t.Fatal(err)
		}
	}
	remaps := map[uint16][]uint32{}
	for v := uint16(0); v < 8; v++ {
		if remaps[v], err = owner.Rekey(group, v, donor.Entries(group, v)); err != nil {
			t.Fatal(err)
		}
	}
	if err := owner.Sync(); err != nil {
		t.Fatal(err)
	}
	remap := func(vshard uint16, id uint32) (uint32, bool) {
		m := remaps[vshard]
		if int(id) >= len(m) || m[id] == v2dict.NoID {
			return 0, false
		}
		return m[id], true
	}
	sorted := func(ms []idFixtureMetric) []idFixtureMetric {
		ms = slices.Clone(ms)
		slices.SortFunc(ms, func(a, b idFixtureMetric) int {
			return cmp.Or(cmp.Compare(a.vshard, b.vshard), cmp.Compare(a.id, b.id))
		})
		return ms
	}
	dir := t.TempDir()
	donorPath := filepath.Join(dir, "donor.sst")
	dd, err := writeIDFixture(donorPath, spec, sorted(ms), donor.Generation())
	if err != nil {
		t.Fatal(err)
	}
	// The owner's own part for the kept names: same records (fixture values
	// follow the donor ID), keyed by the owner's IDs.
	ownerSpec := spec
	ownerSpec.Scope.Part = PartID{7, 7, 7}
	var kept, names []idFixtureMetric
	moved := 0
	for _, m := range ms {
		if deleted[m.name] {
			continue
		}
		e, ok := owner.Lookup(group, m.vshard, []byte(m.name))
		if !ok {
			t.Fatalf("owner lacks %s", m.name)
		}
		if e.ID != m.id {
			moved++
		}
		names = append(names, m)
		kept = append(kept, idFixtureMetric{name: m.name, vshard: m.vshard, id: e.ID})
	}
	if moved == 0 {
		t.Fatal("fixture needs owner IDs to differ from donor IDs")
	}
	wantPath := filepath.Join(dir, "owner.sst")
	if _, err := writeRekeyedFixture(wantPath, ownerSpec, sorted(kept), names, owner.Generation()); err != nil {
		t.Fatal(err)
	}
	gotPath := filepath.Join(dir, "rekeyed.sst")
	rd, err := RekeyDense(context.Background(), donorPath, dd, gotPath, ownerSpec, owner.Generation(), remap)
	if err != nil {
		t.Fatal(err)
	}
	got, _ := os.ReadFile(gotPath)
	want, _ := os.ReadFile(wantPath)
	if len(want) == 0 || !bytes.Equal(got, want) {
		t.Fatalf("rekeyed part differs from the owner's own part (%d vs %d bytes)", len(got), len(want))
	}
	if rd.Coverage.Names != uint64(len(kept)) || rd.DictGeneration != owner.Generation() {
		t.Fatalf("rekeyed descriptor: %d names (want %d), generation %d", rd.Coverage.Names, len(kept), rd.DictGeneration)
	}
	// Mutation: copying without the re-key resolves to other names.
	identity := func(vshard uint16, id uint32) (uint32, bool) { _, ok := remap(vshard, id); return id, ok }
	bad, err := RekeyDense(context.Background(), donorPath, dd, filepath.Join(dir, "identity.sst"), ownerSpec, owner.Generation(), identity)
	if err == nil {
		if b, _ := os.ReadFile(filepath.Join(dir, "identity.sst")); bytes.Equal(b, want) || bad.SHA256 == rd.SHA256 {
			t.Error("copy without re-key not detected")
		}
	}
}

// writeRekeyedFixture writes kept (owner IDs) with the records of names[i]'s
// donor ID, matched by name.
func writeRekeyedFixture(path string, spec Spec, kept, names []idFixtureMetric, generation uint64) (Descriptor, error) {
	donorID := map[ID]uint32{}
	for _, m := range names {
		donorID[m.name] = m.id
	}
	w, err := NewDenseWriter(path, spec)
	if err != nil {
		return Descriptor{}, err
	}
	defer w.Abort()
	if err := w.IDKeyed(generation); err != nil {
		return Descriptor{}, err
	}
	for _, m := range kept {
		for _, r := range idFixtureRecords(int(donorID[m.name])) {
			if err = w.SetPlannedIDRecord(m.vshard, m.id, &r); err != nil {
				return Descriptor{}, err
			}
		}
		if err = w.SetPlannedIDMetadata(m.vshard, m.id, []byte("envelope-"+string(m.name))); err != nil {
			return Descriptor{}, err
		}
	}
	return w.Close()
}
