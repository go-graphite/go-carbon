package v2part

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
)

// This vector freezes both dense value representations, sparse physical slots,
// wrapped event times, original order bits and opaque metadata in one file.
func denseGoldenInput(t *testing.T) (Spec, [][2][]byte) {
	t.Helper()
	spec := fixtureSpec()
	spec.Scope.VShard = GroupVShard
	spec.Scope.Partition = 0
	spec.Source.MinOrder = 1
	spec.Source.MaxOrder = math.MaxUint64
	spec.MinTime = 1700000000
	spec.MaxTime = 1700001020
	spec.Policies = nil
	for archive := uint32(0); archive < 2; archive++ {
		definition := []byte(fmt.Sprintf(`{"step":%d,"count":32,"method":"last","xff":0.5}`, 60*(archive+1)))
		spec.Policies = append(spec.Policies, ArchivePolicy{Archive: archive, Digest: sha256.Sum256(definition), Definition: definition})
	}
	var input [][2][]byte
	for _, name := range []ID{"golden.float", "golden.integer"} {
		for archive := uint32(0); archive < 2; archive++ {
			bits := []uint64{0, 1 << 63, 1, 0x7ff8000000000042, 0xfff8000000000017, 0x7ff0000000000000, 0xfff0000000000000, math.Float64bits(1.25), math.Float64bits(-51.125)}
			if name == "golden.integer" {
				for i := range bits {
					bits[i] = math.Float64bits(float64(700 - i*119))
				}
			}
			for i, value := range bits {
				slot := int64(i*i + int(archive))
				key := Key{Group: spec.Scope.Group, VShard: Route(name), Partition: spec.Scope.Partition, Part: spec.Scope.Part, Metric: name, Archive: archive, BlockStart: slot}
				encoded, err := Encode(key)
				if err != nil {
					t.Fatal(err)
				}
				order := uint64(i + 1)
				if i == 4 {
					order = math.MaxUint64
				}
				record := sealrollup.Record{Delta: sealrollup.Delta{Archive: int(archive), Slot: slot, Order: order, Sample: sealrollup.Sample{Time: spec.MinTime + int64((i+3)%9)*120, Bits: value}}, Correction: i%3 == 0}
				input = append(input, [2][]byte{encoded, sealrollup.EncodeRecord(record)})
			}
		}
		key := Key{Group: spec.Scope.Group, VShard: Route(name), Partition: spec.Scope.Partition, Part: spec.Scope.Part, Metric: name, Archive: MetadataArchive}
		encoded, err := Encode(key)
		if err != nil {
			t.Fatal(err)
		}
		input = append(input, [2][]byte{encoded, []byte(`{"opaque":"exact metadata bytes"}`)})
	}
	slices.SortFunc(input, func(a, b [2][]byte) int { return bytes.Compare(a[0], b[0]) })
	return spec, input
}

// TestDenseGoldenBitExact pins both writer forms: dense-pooled keeps the
// separate envelope KV (LegacyEnvelopeKV), dense-header folds each envelope
// into its name's last run. Both read back to the same singleton stream.
// DENSE_GOLDEN_RECORD=1 rewrites the vectors of the running build.
func TestDenseGoldenBitExact(t *testing.T) {
	defer LegacyEnvelopeKV.Store(false)
	for _, form := range []struct {
		name    string
		legacy  bool
		records uint64
	}{{"dense-pooled", true, 6}, {"dense-header", false, 4}} {
		LegacyEnvelopeKV.Store(form.legacy)
		denseGoldenWrite(t, form.name, form.records)
	}
	_, input := denseGoldenInput(t)
	// Verify and decode both compressor variants independently of the writer.
	for _, suffix := range []string{"", "-nocgo"} {
		checkDenseGolden(t, "dense"+suffix, input)
		checkDenseGolden(t, "dense-pooled"+suffix, input)
		checkDenseGolden(t, "dense-header"+suffix, input)
	}
}

func denseGoldenWrite(t *testing.T, name string, records uint64) {
	spec, input := denseGoldenInput(t)
	path := filepath.Join(t.TempDir(), "dense.sst")
	w, err := NewDenseWriter(path, spec)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Abort()
	for _, row := range input {
		if err := w.SetEncoded(row[0], row[1]); err != nil {
			t.Fatal(err)
		}
	}
	d, err := w.Close()
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	descriptor, err := d.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	vectors := map[string][]byte{name + denseGoldenSuffix + ".sst": data, name + denseGoldenSuffix + "-descriptor.json": descriptor}
	for name, got := range vectors {
		if os.Getenv("DENSE_GOLDEN_RECORD") != "" {
			if err = os.WriteFile(filepath.Join("testdata", name), got, 0o644); err != nil {
				t.Fatal(err)
			}
		}
		want, err := os.ReadFile(filepath.Join("testdata", name))
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("dense golden %s changed: sha256=%x", name, sha256.Sum256(got))
		}
	}
	if d.Version != 2 || d.Coverage.Names != 2 || d.Coverage.Records != records {
		t.Fatalf("dense coverage: %+v", d)
	}
}

func checkDenseGolden(t *testing.T, name string, input [][2][]byte) {
	t.Helper()
	goldenDescriptor, err := os.ReadFile("testdata/" + name + "-descriptor.json")
	if err != nil {
		t.Fatal(err)
	}
	d, err := ParseDescriptor(goldenDescriptor)
	if err != nil {
		t.Fatal(err)
	}
	golden, err := os.ReadFile("testdata/" + name + ".sst")
	if err != nil {
		t.Fatal(err)
	}
	var copied bytes.Buffer
	if err := VerifyStream(context.Background(), &copied, bytes.NewReader(golden), d); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(copied.Bytes(), golden) {
		t.Fatal("verified transfer changed part bytes")
	}
	copiedPath := filepath.Join(t.TempDir(), "copied.sst")
	if err := os.WriteFile(copiedPath, copied.Bytes(), 0600); err != nil {
		t.Fatal(err)
	}
	for _, source := range []string{"testdata/" + name + ".sst", copiedPath} {
		at := 0
		err := ReadExpanded(context.Background(), source, &d, func(key Key, value []byte) error {
			encoded, err := Encode(key)
			if err != nil {
				return err
			}
			if at >= len(input) || !bytes.Equal(encoded, input[at][0]) || !bytes.Equal(value, input[at][1]) {
				return fmt.Errorf("record %d changed", at)
			}
			at++
			return nil
		})
		if err != nil || at != len(input) {
			t.Fatalf("%s: decoded %d/%d: %v", source, at, len(input), err)
		}
	}
}
