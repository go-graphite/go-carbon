package v2part

import (
	"bytes"
	"encoding/binary"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"unsafe"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
)

func TestKeyDecoderOwnsAndValidatesPrefix(t *testing.T) {
	spec := fixtureSpec()
	key, err := Encode(fixtureKey(spec, 0))
	if err != nil {
		t.Fatal(err)
	}
	original := bytes.Clone(key)
	var decoder keyDecoder
	if _, err := decoder.decode(key); err != nil {
		t.Fatal(err)
	}
	// Every tail field remains variable and is decoded exactly as the reference.
	for _, slot := range []int64{0, 1, math.MaxInt64, math.MinInt64} {
		candidate := bytes.Clone(original)
		tail := candidate[len(candidate)-16:]
		binary.BigEndian.PutUint32(tail, MetadataArchive)
		binary.BigEndian.PutUint64(tail[4:], uint64(slot)^(1<<63))
		binary.BigEndian.PutUint32(tail[12:], 31)
		got, err := decoder.decode(candidate)
		want, referenceErr := Decode(candidate)
		if err != nil || referenceErr != nil || got != want {
			t.Fatalf("tail decode: got=%+v want=%+v err=%v/%v", got, want, err, referenceErr)
		}
	}
	// Mutating the caller's buffer cannot mutate the cached identity. Every
	// changed prefix is revalidated, including a forged virtual-shard route.
	for i := 0; i < len(original)-16; i++ {
		candidate := bytes.Clone(original)
		candidate[i] ^= 1
		got, err := decoder.decode(candidate)
		want, referenceErr := Decode(candidate)
		if (err == nil) != (referenceErr == nil) || (err == nil && got != want) {
			t.Fatalf("prefix byte %d bypassed validation", i)
		}
		if _, err := decoder.decode(original); err != nil {
			t.Fatal(err)
		}
	}
	copy(key, bytes.Repeat([]byte{255}, len(key)))
	if got, err := decoder.decode(original); err != nil || got.Metric != fixtureKey(spec, 0).Metric {
		t.Fatal("borrowed input changed cached identity", got, err)
	}
}

func BenchmarkKeyDecoderRepeatedIdentity(b *testing.B) {
	spec := fixtureSpec()
	key, err := Encode(fixtureKey(spec, 0))
	if err != nil {
		b.Fatal(err)
	}
	var decoder keyDecoder
	if _, err := decoder.decode(key); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		binary.BigEndian.PutUint64(key[len(key)-12:], uint64(i)^(1<<63))
		if _, err := decoder.decode(key); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkKeyDecodeReference(b *testing.B) {
	spec := fixtureSpec()
	key, err := Encode(fixtureKey(spec, 0))
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		binary.BigEndian.PutUint64(key[len(key)-12:], uint64(i)^(1<<63))
		if _, err := Decode(key); err != nil {
			b.Fatal(err)
		}
	}
}

func TestDenseScalarValidationMatchesSingletonCodec(t *testing.T) {
	valid := sealrollup.Record{Delta: sealrollup.Delta{Archive: 0, Slot: 0, Order: 1, Sample: sealrollup.Sample{Time: 1, Bits: 0x7ff8000000000042}}}
	cases := []sealrollup.Record{valid}
	for _, archive := range []int{-1, 0, 1, math.MaxInt32} {
		r := valid
		r.Archive = archive
		cases = append(cases, r)
	}
	for _, slot := range []int64{math.MinInt64, -1, 0, math.MaxInt64} {
		r := valid
		r.Slot = slot
		cases = append(cases, r)
	}
	for _, stamp := range []int64{math.MinInt64, -1, 0, 1, math.MaxUint32, math.MaxUint32 + 1, math.MaxInt64} {
		r := valid
		r.Sample.Time = stamp
		cases = append(cases, r)
	}
	for _, order := range []uint64{0, 1, math.MaxUint64} {
		r := valid
		r.Order = order
		cases = append(cases, r)
	}
	for i, r := range cases {
		_, referenceErr := sealrollup.DecodeRecord(r.Archive, r.Slot, sealrollup.EncodeRecord(r))
		_, err := EncodeSlots([]sealrollup.Record{r})
		if (err == nil) != (referenceErr == nil) {
			t.Fatalf("case %d validation differs: %v/%v", i, err, referenceErr)
		}
	}
}

func TestDenseTypedWriterMatchesSingletonBytes(t *testing.T) {
	spec, input := denseGoldenInput(t)
	var outputs [][]byte
	for _, typed := range []bool{false, true} {
		path := filepath.Join(t.TempDir(), "typed.sst")
		writer, err := NewDenseWriter(path, spec)
		if err != nil {
			t.Fatal(err)
		}
		for _, kv := range input {
			key, err := Decode(kv[0])
			if err != nil {
				t.Fatal(err)
			}
			if typed && key.Archive != MetadataArchive {
				var record sealrollup.Record
				record, err = sealrollup.DecodeRecord(int(key.Archive), key.BlockStart, kv[1])
				if err != nil {
					t.Fatal(err)
				}
				err = writer.SetRecordEncoded(kv[0], record)
			} else {
				err = writer.SetEncoded(kv[0], kv[1])
			}
			if err != nil {
				t.Fatal(err)
			}
		}
		descriptor, err := writer.Close()
		if err != nil {
			t.Fatal(err)
		}
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		encoded, err := descriptor.MarshalBinary()
		if err != nil {
			t.Fatal(err)
		}
		outputs = append(outputs, append(data, encoded...))
	}
	if !bytes.Equal(outputs[0], outputs[1]) {
		t.Fatal("typed planner records changed SST or descriptor bytes")
	}
}

func TestDenseTypedWriterRejectsMismatch(t *testing.T) {
	spec := fixtureSpec()
	key := fixtureKey(spec, 0)
	encoded, err := Encode(key)
	if err != nil {
		t.Fatal(err)
	}
	valid := sealrollup.Record{Delta: sealrollup.Delta{Archive: int(key.Archive), Slot: key.BlockStart, Order: 1, Sample: sealrollup.Sample{Time: 1700000000, Bits: 1 << 63}}}
	for _, kind := range []string{"archive", "slot", "order", "clock", "duplicate"} {
		t.Run(kind, func(t *testing.T) {
			writer, err := NewDenseWriter(filepath.Join(t.TempDir(), "invalid.sst"), spec)
			if err != nil {
				t.Fatal(err)
			}
			record := valid
			switch kind {
			case "archive":
				record.Archive++
			case "slot":
				record.Slot++
			case "order":
				record.Order = 0
			case "clock":
				record.Sample.Time = 0
			case "duplicate":
				if err := writer.SetRecordEncoded(encoded, record); err != nil {
					t.Fatal(err)
				}
			}
			if err := writer.SetRecordEncoded(encoded, record); err == nil {
				t.Fatal("invalid typed input accepted")
			}
			if _, err := writer.Close(); err == nil {
				t.Fatal("invalid typed writer published")
			}
		})
	}
}

// A planner that already holds the metric name shares it (and the previous
// group) instead of copying both out of each new identity. Hints never change
// validation or decoded values: stale or forged hints decode as the reference.
func TestKeyDecoderSharesHintedStrings(t *testing.T) {
	spec := fixtureSpec()
	keys := make([][]byte, 2)
	names := []ID{"example.metric", "example.metric.other"}
	for i, name := range names {
		k := fixtureKey(spec, 0)
		k.Metric, k.VShard = name, Route(name)
		var err error
		if keys[i], err = Encode(k); err != nil {
			t.Fatal(err)
		}
	}
	var decoder keyDecoder
	first, err := decoder.decode(keys[0])
	if want, _ := Decode(keys[0]); err != nil || first != want {
		t.Fatalf("unhinted decode: %+v %v", first, err)
	}
	hint := ID(strings.Clone(string(names[1])))
	decoder.name = hint
	got, err := decoder.decode(keys[1])
	if want, _ := Decode(keys[1]); err != nil || got != want {
		t.Fatalf("hinted decode: got=%+v want=%+v err=%v", got, want, err)
	}
	if unsafe.StringData(string(got.Metric)) != unsafe.StringData(string(hint)) || unsafe.StringData(string(got.Group)) != unsafe.StringData(string(first.Group)) {
		t.Fatal("matching hint or group was copied")
	}
	// A stale hint costs only the copy.
	got, err = decoder.decode(keys[0])
	if want, _ := Decode(keys[0]); err != nil || got != want {
		t.Fatalf("stale hint: got=%+v want=%+v err=%v", got, want, err)
	}
	// Forged prefixes are rejected or decoded exactly as without hints.
	for i := 0; i < len(keys[1])-16; i++ {
		candidate := bytes.Clone(keys[1])
		candidate[i] ^= 1
		decoder.name = hint
		got, err := decoder.decode(candidate)
		want, referenceErr := Decode(candidate)
		if (err == nil) != (referenceErr == nil) || (err == nil && got != want) {
			t.Fatalf("prefix byte %d: hinted decode differs from reference", i)
		}
	}
	allocs := testing.AllocsPerRun(100, func() {
		for i := range keys {
			decoder.name = names[i]
			if _, err := decoder.decode(keys[i]); err != nil {
				t.Fatal(err)
			}
		}
	})
	if allocs != 0 {
		t.Fatalf("hinted identity changes allocate %.1f times", allocs)
	}
}
