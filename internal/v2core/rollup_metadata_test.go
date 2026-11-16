package v2core

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2part"
)

func TestRollupMetadataBinaryLegacyAndFraming(t *testing.T) {
	for _, version := range []uint8{1, 2, 3} {
		m := RollupMetadata{Version: version, ID: 8, Policy: []byte("canonical\x00policy"), LastUpdate: -123, Revision: 9, Frontier: sealrollup.Frontier{Order: 7, MaxTime: 99, Policy: [32]byte{1, 255}}}
		if version == 3 {
			m.Version = 2
			m.BaselineOrder = 3
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
		t.Fatalf("files %d want one per placement group", len(plan.Files))
	}
	if e = validateCoreSealPlan(plan); e != nil {
		t.Fatal(e)
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
		fb, _ := os.ReadFile(fast)
		sb, _ := os.ReadFile(slow)
		if !reflect.DeepEqual(fd, sd) || !bytes.Equal(fb, sb) {
			t.Fatal("planned spec changed portable part bytes/descriptor")
		}
		first, _ := v2part.Decode(file[0].Key)
		shards := map[uint16]bool{}
		for _, kv := range file {
			k, e := v2part.Decode(kv.Key)
			if e != nil || k.Group != first.Group {
				t.Fatalf("mixed group %v", e)
			}
			shards[k.VShard] = true
			saved = append(saved, bytes.Clone(kv.Key), bytes.Clone(kv.Value))
		}
		if len(shards) < 32 {
			t.Fatal("fixture did not span vshards")
		}
	}
	if _, e = r.plan(in); e == nil {
		t.Fatal("reused pending plan arenas")
	}
	n := 0
	for _, file := range plan.Files {
		for _, kv := range file {
			if !bytes.Equal(saved[n], kv.Key) || !bytes.Equal(saved[n+1], kv.Value) {
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
