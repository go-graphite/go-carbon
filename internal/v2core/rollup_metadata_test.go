package v2core

import (
	"bytes"
	"encoding/json"
	"fmt"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2part"
)

func TestRollupMetadataBinaryLegacyAndFraming(t *testing.T) {
	for _, version := range []uint8{1, 2, 3, 4} {
		m := RollupMetadata{Version: version, ID: 8, Policy: []byte("canonical\x00policy"), LastUpdate: -123, Revision: 9, Frontier: sealrollup.Frontier{Order: 7, MaxTime: 99, Policy: [32]byte{1, 255}}}
		if version == 3 {
			m.Version = 2
			m.BaselineOrder = 3
		}
		if version == 4 {
			m.Version = 3
			m.ClosedThrough = 120
			m.BaselineOrder = 2
		}
		if version >= 2 {
			m.Expired = true
			m.ResetOrder = 7
		}
		legacy, e := json.Marshal(m)
		if e != nil {
			t.Fatal(e)
		}
		encoded := AppendRollupMetadata(nil, m)
		for _, data := range [][]byte{legacy, encoded} {
			got, e := DecodeRollupMetadata(data)
			if e != nil || !reflect.DeepEqual(got, m) {
				t.Fatalf("roundtrip %+v %v", got, e)
			}
		}
		for n := 0; n < len(encoded); n++ {
			if _, e := DecodeRollupMetadata(encoded[:n]); e == nil {
				t.Fatalf("accepted truncated row %d", n)
			}
		}
		for _, bad := range [][]byte{append(bytes.Clone(encoded), 0), append(bytes.Clone(encoded[:5]), append([]byte{2}, encoded[6:]...)...)} {
			if _, e := DecodeRollupMetadata(bad); e == nil {
				t.Fatal("accepted corrupt row")
			}
		}
		got, e := DecodeRollupMetadata(encoded)
		if e != nil {
			t.Fatal(e)
		}
		clear(encoded)
		if !bytes.Equal(got.Policy, m.Policy) {
			t.Fatal("policy aliases iterator")
		}
	}
}

func TestRollupGroupPackingArenaAndAbort(t *testing.T) {
	r := newCoreRollups()
	var batches []Batch
	for i := 0; i < 512; i++ {
		p, _ := rollupTestPolicy(t, fmt.Sprintf("metric.%d", i), "1s:1d,1m:30d", sealrollup.Last, .5)
		p.Group = v2part.Group(fmt.Sprintf("group.%d", i%2))
		policy, e := EncodeRollupPolicy(p)
		if e != nil {
			t.Fatal(e)
		}
		batches = append(batches, Batch{ID: uint64(i + 1), Order: 1, Clock: 1000, Policy: policy, Points: []Row{{ID: uint64(i + 1), Time: 999, Bits: uint64(i), Seq: 1}}})
	}
	in := SealInput{Batches: batches}
	plan, e := r.plan(in)
	if e != nil {
		t.Fatal(e)
	}
	if len(plan.Files) != 2 {
		t.Fatalf("files %d want one month part per placement group", len(plan.Files))
	}
	if e = validateCoreSealPlan(plan); e != nil {
		t.Fatal(e)
	}
	duplicate := plan
	duplicate.Files = append(append([][]coreKV(nil), plan.Files...), plan.Files[0])
	duplicate.PartSpecs = append(append([]v2part.Spec(nil), plan.PartSpecs...), plan.PartSpecs[0])
	if validateCoreSealPlan(duplicate) == nil {
		t.Fatal("accepted duplicate archive scope")
	}
	var saved [][]byte
	for i, file := range plan.Files {
		source := v2part.Provenance{Incarnation: v2part.PartID{9}, Stream: "original"}
		spec := plan.PartSpecs[i]
		spec.Source.Incarnation = source.Incarnation
		spec.Source.Stream = source.Stream
		dir := t.TempDir()
		fast := filepath.Join(dir, "fast.sst")
		slow := filepath.Join(dir, "slow.sst")
		fd, err := writePlannedRollupPart(fast, file, spec)
		if err != nil {
			t.Fatal(err)
		}
		sd, err := WriteDenseRollupPart(slow, file, source)
		if err != nil {
			t.Fatal(err)
		}
		// Planned sealing adds the exact inventory and expiry bounds to the
		// descriptor; the generic portable writer (no expiry: never retired by
		// age) stays backward compatible. Seals use the fast Zstd level and
		// portable parts the rest level, so only size and hash differ, and
		// both decode to the same records.
		if fd.Coverage.MaxExpiry == 0 {
			t.Fatal("planned seal omitted expiry bounds")
		}
		same := reflect.DeepEqual(readSealCompressionPart(t, fast, fd), readSealCompressionPart(t, slow, sd))
		sd.VShards = fd.VShards
		sd.Coverage.MinExpiry, sd.Coverage.MaxExpiry = fd.Coverage.MinExpiry, fd.Coverage.MaxExpiry
		sd.SHA256, sd.Bytes = fd.SHA256, fd.Bytes
		if !same || !reflect.DeepEqual(fd, sd) {
			t.Fatal("planned spec changed portable part records/descriptor")
		}
		first, _ := v2part.Decode(file[0].Key)
		shards := map[uint16]bool{}
		for _, kv := range file {
			k, e := v2part.Decode(kv.Key)
			if e != nil || k.Group != first.Group {
				t.Fatalf("mixed group %v", e)
			}
			shards[k.VShard] = true
			saved = append(saved, bytes.Clone(kv.Key), bytes.Clone(kv.encodedValue()))
		}
		if len(shards) < 32 {
			t.Fatal("fixture did not span vshards")
		}
		if len(fd.VShards) != len(shards) {
			t.Fatal("seal inventory omitted routed shards")
		}
		for _, v := range fd.VShards {
			if !shards[v] {
				t.Fatal("seal inventory added an absent shard")
			}
		}
	}
	if _, e = r.plan(in); e == nil {
		t.Fatal("reused pending plan arenas")
	}
	n := 0
	for _, file := range plan.Files {
		for _, kv := range file {
			if !bytes.Equal(saved[n], kv.Key) || !bytes.Equal(saved[n+1], kv.encodedValue()) {
				t.Fatal("pending bytes changed")
			}
			n += 2
		}
	}
	plan.Abort()
	if len(r.metrics) != 0 {
		t.Fatal("abort published metrics")
	}
	plan, e = r.plan(in)
	if e != nil {
		t.Fatal(e)
	}
	plan.Commit()
	if len(r.metrics) != 512 || r.pending {
		t.Fatal("commit lost reducer state or arena lease")
	}
	for i := range batches {
		batches[i].Order = 2
		batches[i].Points[0].Time = 998
	}
	plan, e = r.plan(in)
	if e != nil {
		t.Fatal(e)
	}
	plan.Commit()
	for _, m := range r.metrics {
		if m.meta.Frontier.Order != 2 {
			t.Fatal("second seal lost frontier")
		}
	}
}

func BenchmarkRollupMetadataBinary(b *testing.B) {
	m := RollupMetadata{Version: 1, ID: 1, Policy: bytes.Repeat([]byte("x"), 256), Revision: 1, Frontier: sealrollup.Frontier{Order: 1}}
	buf := make([]byte, 0, 512)
	b.ReportAllocs()
	for b.Loop() {
		buf = AppendRollupMetadata(buf[:0], m)
	}
}

// Registered identities must emit the identical on-disk envelope, including
// escaped names and the optional baseline header, without a plain-name copy.
func TestRollupIdentityMetadataEncoding(t *testing.T) {
	for _, name := range []string{"namespace.metric", "quoted.\"metric\\name", "html.<metric>&", "unicode.метрика"} {
		_, raw := rollupTestPolicy(t, name, "1s:1h,1m:1d", sealrollup.Last, .5)
		h, err := (&PolicyCatalog{}).intern(7, name, raw)
		if err != nil {
			t.Fatal(err)
		}
		for _, baseline := range []uint64{0, 1} {
			meta := RollupMetadata{Version: 2, ID: 7, Policy: raw, BaselineOrder: baseline, ResetOrder: 3, Expired: true, LastUpdate: -1, Revision: 19, Frontier: sealrollup.Frontier{Order: 5, MaxTime: -123, Policy: h.policy.prepared.Digest()}}
			prefix := []byte("existing arena prefix")
			want := AppendRollupMetadata(bytes.Clone(prefix), meta)
			buf := make([]byte, len(prefix), len(want)+64)
			copy(buf, prefix)
			got := h.appendMetadata(buf, meta)
			if !bytes.Equal(got, want) {
				t.Fatalf("metadata encoding changed for %q baseline%d", name, baseline)
			}
			if h.plain {
				allocs := testing.AllocsPerRun(100, func() { buf = h.appendMetadata(buf[:len(prefix)], meta) })
				if allocs != 0 {
					t.Fatalf("plain metadata allocates %.0f times", allocs)
				}
			}
		}
	}
}
