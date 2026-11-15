package partspack

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"math"
	"testing"

	"github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/partsformat"
)

func dictionaryFixture(t *testing.T) (Policy, []Record) {
	t.Helper()
	p := Policy{Method: 1, XFilesFactorBits: math.Float32bits(.5), Archives: []Retention{{60, 10080}, {3600, 8760}}}
	first := partsformat.ID("dictionary.metric.0")
	names := []partsformat.ID{first}
	for i := 1; len(names) < 2; i++ {
		name := partsformat.ID(fmt.Sprintf("dictionary.metric.%d", i))
		if partsformat.Route(name) == partsformat.Route(first) {
			names = append(names, name)
		}
	}
	var records []Record
	bits := []uint64{0, 1 << 63, 0x7ff8000000000012, 0xfff0000000000000, 1}
	for _, name := range names {
		for archive := range p.Archives {
			for block := range 5 {
				var points []chunkstore.Point
				for i, value := range bits {
					points = append(points, chunkstore.Point{Timestamp: int64(block*10000 + i*int(p.Archives[archive].Step)), Value: math.Float64frombits(value)})
				}
				data, err := EncodeBlock(points, p.Archives[archive].Step)
				if err != nil {
					t.Fatal(err)
				}
				records = append(records, Record{partsformat.Key{Group: "namespace", Metric: name, VShard: partsformat.Route(name), Part: partsformat.PartID{9}, Partition: -123, Archive: uint32(archive), BlockStart: math.MinInt64 + int64(block), Ordinal: uint32(block)}, data})
			}
		}
	}
	return p, records
}

func TestDictionaryPartRestoresPortableKeysAndBits(t *testing.T) {
	p, records := dictionaryFixture(t)
	b, err := EncodePart(p, records)
	if err != nil {
		t.Fatal(err)
	}
	opened, err := OpenPart(b)
	if err != nil {
		t.Fatal(err)
	}
	want := map[partsformat.Key][]byte{}
	for _, r := range records {
		want[r.Key] = r.Data
	}
	if len(opened.Records) != len(want) {
		t.Fatal("record count differs")
	}
	for _, r := range opened.Records {
		if !bytes.Equal(want[r.Key], r.Data) {
			t.Fatal("portable identity or exact payload changed")
		}
		before, err := DecodeBlock(want[r.Key])
		if err != nil {
			t.Fatal(err)
		}
		after, err := DecodeBlock(r.Data)
		if err != nil || len(before) != len(after) {
			t.Fatal("invalid decoded points", err)
		}
		for i := range before {
			if before[i].Timestamp != after[i].Timestamp || math.Float64bits(before[i].Value) != math.Float64bits(after[i].Value) {
				t.Fatal("changed timestamp or IEEE bits")
			}
		}
	}
	// Shuffling input must not change either the dictionary or its authenticated bytes.
	for i, j := 0, len(records)-1; i < j; i, j = i+1, j-1 {
		records[i], records[j] = records[j], records[i]
	}
	again, err := EncodePart(p, records)
	if err != nil || !bytes.Equal(b, again) {
		t.Fatal("nondeterministic packed bytes", err)
	}
}

func TestDictionaryRejectsCrossScopeAndDuplicateKeys(t *testing.T) {
	p, records := dictionaryFixture(t)
	for _, mutate := range []func(*partsformat.Key){
		func(k *partsformat.Key) { k.Group = "other" },
		func(k *partsformat.Key) { k.Partition++ },
		func(k *partsformat.Key) { k.Part[0]++ },
		func(k *partsformat.Key) { k.VShard++ },
		func(k *partsformat.Key) { k.Archive = 2 },
	} {
		copyRows := append([]Record(nil), records...)
		mutate(&copyRows[1].Key)
		if _, err := EncodePart(p, copyRows); err == nil {
			t.Fatal("accepted invalid ownership or archive")
		}
	}
	if _, err := EncodePart(p, append(records, records[0])); err == nil {
		t.Fatal("accepted duplicate key")
	}
}

func TestDictionaryRejectsCorruptAndMalformed(t *testing.T) {
	p, records := dictionaryFixture(t)
	b, err := EncodePart(p, records)
	if err != nil {
		t.Fatal(err)
	}
	for n := range len(b) {
		if _, err := OpenPart(b[:n]); err == nil {
			t.Fatalf("accepted truncation %d", n)
		}
	}
	for i := range b {
		bad := bytes.Clone(b)
		bad[i] ^= 0xff
		if _, err := OpenPart(bad); err == nil {
			t.Fatalf("accepted corruption %d", i)
		}
	}
	// Re-sign hostile lengths/references so validation is tested beyond the digest.
	for i := 4; i < len(b)-sha256.Size; i++ {
		bad := bytes.Clone(b)
		bad[i] = 0xff
		sum := sha256.Sum256(bad[:len(bad)-sha256.Size])
		copy(bad[len(bad)-sha256.Size:], sum[:])
		_, _ = OpenPart(bad) // must never panic or allocate using unbounded counts
	}
}

func TestDictionaryRestoresCompressedAndOpaquePayloadBytes(t *testing.T) {
	p, records := dictionaryFixture(t)
	var points []chunkstore.Point
	for i := range 4096 {
		points = append(points, chunkstore.Point{Timestamp: 1700000040 + int64(i*60), Value: float64(i%7) / 10})
	}
	cold, err := EncodeBlock(points, 60)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := EncodeBlock([]chunkstore.Point{{Timestamp: math.MinInt64, Value: math.Float64frombits(1 << 63)}}, 60)
	if err != nil {
		t.Fatal(err)
	}
	for _, data := range [][]byte{cold, raw, []byte("arbitrary opaque value without a cold-codec CRC")} {
		pair := []Record{records[0], records[0]}
		pair[0].Data = data
		pair[1].Data = bytes.Clone(data)
		pair[1].Key.Ordinal++
		b, err := EncodePart(p, pair)
		if err != nil {
			t.Fatal(err)
		}
		opened, err := OpenPart(b)
		if err != nil {
			t.Fatal(err)
		}
		for _, r := range opened.Records {
			if !bytes.Equal(r.Data, data) {
				t.Fatal("reconstructed payload bytes differ")
			}
		}
	}
}
