package v2core

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"math/rand/v2"
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
			got, e := DecodeRollupMetadata("", "", data)
			if e != nil || !reflect.DeepEqual(got, m) {
				t.Fatalf("roundtrip %+v %v", got, e)
			}
		}
		for n := 0; n < len(encoded); n++ {
			if _, e := DecodeRollupMetadata("", "", encoded[:n]); e == nil {
				t.Fatalf("accepted truncated row %d", n)
			}
		}
		for _, bad := range [][]byte{append(bytes.Clone(encoded), 0), append(bytes.Clone(encoded[:5]), append([]byte{2}, encoded[6:]...)...)} {
			if _, e := DecodeRollupMetadata("", "", bad); e == nil {
				t.Fatal("accepted corrupt row")
			}
		}
		got, e := DecodeRollupMetadata("", "", encoded)
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
	for id := range r.metrics {
		if r.metaOf(id).Frontier.Order != 2 {
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

// Registered identities emit slim envelopes that decode, through the part
// policy table, to exactly what the full envelope decodes to, including the
// definition bytes, for escaped names, every method, XFF and ladder shape.
func TestRollupIdentityMetadataEncoding(t *testing.T) {
	names := []string{"namespace.metric", "quoted.\"metric\\name", "html.<metric>&", "unicode.метрика", "ctl.\x01\x7f", "line\u2028sep", "a"}
	rng := rand.New(rand.NewPCG(1, 2))
	for i := 0; i < 64; i++ {
		b := make([]byte, 1+rng.IntN(120))
		for j := range b {
			b[j] = byte(0x21 + rng.IntN(0x5e))
		}
		names = append(names, string(b))
	}
	ladders := []string{"1s:1h,1m:1d", "10s:3d", "10s:2d,1m:14d,30m:2y", "1m:30d,1h:1y,1d:10y", "1s:1d,1m:30d,1h:1y,1d:10y"}
	methods := []sealrollup.Method{sealrollup.Average, sealrollup.Sum, sealrollup.Last, sealrollup.Max, sealrollup.Min}
	xffs := []float32{0, .1, .5, 1}
	for i, name := range names {
		ladder, method, xff := ladders[i%len(ladders)], methods[i%len(methods)], xffs[i%len(xffs)]
		policy, raw := rollupTestPolicy(t, name, ladder, method, xff)
		h, err := (&PolicyCatalog{}).intern(7, name, raw)
		if err != nil {
			t.Fatal(err)
		}
		config, _ := json.Marshal(policy.Config)
		v2part.RegisterPolicies([]v2part.ArchivePolicy{{Digest: sha256.Sum256(config), Definition: config}})
		for _, version := range []uint8{1, 2, 3, 4} {
			for _, baseline := range []uint64{0, 1} {
				meta := RollupMetadata{Version: version, ID: 7, Policy: raw, BaselineOrder: baseline, LastUpdate: -1, Revision: 19, Frontier: sealrollup.Frontier{Order: 5, MaxTime: -123, Policy: h.policy.prepared.Digest()}}
				if version > 1 {
					meta.ResetOrder, meta.Expired = 5, true
				}
				full, err := DecodeRollupMetadata(policy.Name, policy.Group, AppendRollupMetadata(nil, meta))
				if err != nil {
					t.Fatal(err)
				}
				prefix := []byte("existing arena prefix")
				buf := append(bytes.Clone(prefix), make([]byte, 0, 256)...)
				got := h.appendMetadata(buf, meta)
				if !bytes.Equal(got[:len(prefix)], prefix) || bytes.Contains(got, []byte(`"Config"`)) {
					t.Fatalf("slim envelope stored the definition for %q", name)
				}
				slim, err := DecodeRollupMetadata(policy.Name, policy.Group, got[len(prefix):])
				if err != nil || !reflect.DeepEqual(slim, full) || !bytes.Equal(slim.Policy, raw) {
					t.Fatalf("slim decode differs for %q v%d baseline%d: %v\n%+v\n%+v", name, version, baseline, err, slim, full)
				}
				view, err := decodeRollupMetadataView(policy.Name, policy.Group, got[len(prefix):])
				if err != nil || !reflect.DeepEqual(view, full) {
					t.Fatalf("slim view differs for %q", name)
				}
				for n := 0; n < len(got)-len(prefix); n++ {
					if _, e := DecodeRollupMetadata(policy.Name, policy.Group, got[len(prefix):len(prefix)+n]); e == nil {
						t.Fatalf("accepted truncated slim row %d", n)
					}
				}
				if _, e := DecodeRollupMetadata(policy.Name, policy.Group, append(bytes.Clone(got[len(prefix):]), 0)); e == nil {
					t.Fatal("accepted slim row with a stored definition")
				}
				allocs := testing.AllocsPerRun(100, func() { buf = h.appendMetadata(buf[:len(prefix)], meta) })
				if allocs != 0 {
					t.Fatalf("slim metadata allocates %.0f times", allocs)
				}
			}
		}
	}
}
