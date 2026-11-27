package v2part

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/go-graphite/go-carbon/internal/partsformat"
)

func fixtureSpec() Spec {
	policy := []byte(`{"step":60,"count":1440,"method":"average","xff":0.5}`)
	return Spec{Scope: Scope{Group: "example/cell-3", VShard: Route("example.metric"), Partition: -7, Part: PartID{1, 2, 3, 4}}, Encoding: "fixture-v1", Source: Provenance{Incarnation: PartID{9}, Stream: "input-0", MinOrder: 7, MaxOrder: 9}, MinTime: 1700000000, MaxTime: 1700000060, Policies: []ArchivePolicy{{Archive: 0, Digest: sha256.Sum256(policy), Definition: policy}}}
}
func fixtureKey(s Spec, n uint32) Key {
	return Key{Group: s.Scope.Group, VShard: s.Scope.VShard, Partition: s.Scope.Partition, Part: s.Scope.Part, Metric: "example.metric", Archive: 0, BlockStart: 1, Ordinal: n}
}
func writeFixture(t *testing.T) (string, Descriptor) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "part.sst")
	s := fixtureSpec()
	w, err := NewWriter(path, s)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Abort()
	for i := uint32(0); i < 3; i++ {
		if err := w.Set(fixtureKey(s, i), []byte{0, byte(i), 255}); err != nil {
			t.Fatal(err)
		}
	}
	d, err := w.Close()
	if err != nil {
		t.Fatal(err)
	}
	return path, d
}

func TestGCP3Golden(t *testing.T) {
	s := fixtureSpec()
	k := fixtureKey(s, 2)
	b, err := Encode(k)
	if err != nil {
		t.Fatal(err)
	}
	const golden = "474350030000000e6578616d706c652f63656c6c2d3308ac7ffffffffffffff9010203040000000000000000000000000000000e6578616d706c652e6d657472696300000000800000000000000100000002"
	if hex.EncodeToString(b) != golden {
		t.Fatalf("key golden: %x", b)
	}
	old, err := partsformat.Encode(k)
	if err != nil || !bytes.Equal(b, old) {
		t.Fatalf("published GCP3 changed: %v", err)
	}
	got, err := Decode(b)
	if err != nil || got != k {
		t.Fatalf("round trip: %v %v", got, err)
	}
	for i := 0; i < len(b); i++ {
		if _, err := Decode(b[:i]); err == nil {
			t.Fatalf("accepted truncation at %d", i)
		}
	}
	if _, err := Decode(append(b, 0)); err == nil {
		t.Fatal("accepted trailing byte")
	}
	b[3]++
	if _, err := Decode(b); err == nil {
		t.Fatal("accepted unknown key version")
	}
}

func TestSSTGoldenRoundTripAndIngest(t *testing.T) {
	path, d := writeFixture(t)
	ctx := context.Background()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if d.Bytes != uint64(len(data)) || d.SHA256 != sha256.Sum256(data) {
		t.Fatal("stream checksum mismatch")
	}
	if d.Coverage.Names != 1 || d.Coverage.Records != 3 {
		t.Fatal("wrong counts")
	}
	// A fixed stored SST vector is readable independently of writer changes.
	golden, err := os.ReadFile("testdata/gcp3.sst")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(data, golden) {
		t.Fatalf("SST golden changed: sha256=%x", d.SHA256)
	}
	b, err := d.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	goldenDescriptor, err := os.ReadFile("testdata/descriptor.json")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(b, goldenDescriptor) {
		t.Fatal("descriptor golden changed")
	}
	if got, err := ParseDescriptor(b); err != nil || !reflect.DeepEqual(got, d) {
		t.Fatalf("descriptor round trip: %v", err)
	}
	var copied bytes.Buffer
	if err := VerifyStream(ctx, &copied, bytes.NewReader(data), d); err != nil || !bytes.Equal(copied.Bytes(), data) {
		t.Fatal(err)
	}
	for _, expected := range []*Descriptor{nil, &d} {
		var i uint32
		if err := Read(ctx, path, expected, func(k Key, v []byte) error {
			if k != fixtureKey(fixtureSpec(), i) || !bytes.Equal(v, []byte{0, byte(i), 255}) {
				return errors.New("record changed")
			}
			i++
			return nil
		}); err != nil || i != 3 {
			t.Fatalf("read: %v records %d", err, i)
		}
	}
	db, err := pebble.Open(filepath.Join(t.TempDir(), "target"), &pebble.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.Ingest([]string{path}); err != nil {
		t.Fatal(err)
	}
	key, _ := Encode(fixtureKey(fixtureSpec(), 1))
	value, closer, err := db.Get(key)
	if err != nil {
		t.Fatal(err)
	}
	defer closer.Close()
	if !bytes.Equal(value, []byte{0, 1, 255}) {
		t.Fatal("ingest changed value")
	}
}

func TestDescriptorAndStreamFailures(t *testing.T) {
	path, d := writeFixture(t)
	data, _ := os.ReadFile(path)
	ctx := context.Background()
	for _, b := range [][]byte{data[:len(data)-1], append(bytes.Clone(data), 0), append([]byte{data[0] ^ 1}, data[1:]...)} {
		if err := VerifyStream(ctx, io.Discard, bytes.NewReader(b), d); err == nil {
			t.Fatal("accepted corrupt stream")
		}
	}
	ctx, cancel := context.WithCancel(ctx)
	cancel()
	if err := VerifyStream(ctx, io.Discard, bytes.NewReader(data), d); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	ctx = context.Background()
	bad := d
	bad.Coverage.Names++
	if err := Read(ctx, path, &bad, nil); err == nil {
		t.Fatal("accepted wrong name count")
	}
	bad = d
	bad.Coverage.Records++
	if err := Read(ctx, path, &bad, nil); err == nil {
		t.Fatal("accepted wrong record count")
	}
	bad = d
	bad.Scope.Part[0]++
	if err := Read(ctx, path, &bad, nil); err == nil {
		t.Fatal("accepted foreign part")
	}
	bad = d
	bad.Version++
	if err := bad.Validate(); err == nil {
		t.Fatal("accepted new schema")
	}
	b, _ := d.MarshalBinary()
	for _, malformed := range [][]byte{append(bytes.Clone(b), ' '), bytes.Replace(b, []byte(`"Version":1`), []byte(`"Version":1,"Unknown":0`), 1), bytes.Replace(b, []byte(`"Version":1`), []byte(`"Version":1,"Version":1`), 1)} {
		if _, err := ParseDescriptor(malformed); err == nil {
			t.Fatal("accepted noncanonical descriptor")
		}
	}
	bad = d
	bad.Coverage.MinTime = d.Coverage.MaxTime + 1
	if err := bad.Validate(); err == nil {
		t.Fatal("accepted inverted time range")
	}
}

func TestWriterRejectsBadAndForeignInput(t *testing.T) {
	for _, kind := range []string{"empty", "duplicate", "group", "part", "vshard", "partition", "policy", "order"} {
		t.Run(kind, func(t *testing.T) {
			s := fixtureSpec()
			path := filepath.Join(t.TempDir(), "bad.sst")
			w, err := NewWriter(path, s)
			if err != nil {
				t.Fatal(err)
			}
			defer w.Abort()
			if kind != "empty" {
				if err = w.Set(fixtureKey(s, 1), nil); err != nil {
					t.Fatal(err)
				}
				k := fixtureKey(s, 2)
				switch kind {
				case "duplicate":
					k.Ordinal = 1
				case "group":
					k.Group += "other"
				case "part":
					k.Part[0]++
				case "vshard":
					k.VShard ^= 1
				case "partition":
					k.Partition++
				case "policy":
					k.Archive = 8
				case "order":
					k.Ordinal = 0
				}
				if err := w.Set(k, nil); err == nil {
					t.Fatal("accepted invalid key")
				}
			}
			if _, err := w.Close(); err == nil {
				t.Fatal("closed invalid part")
			}
			if _, err := os.Stat(path); !os.IsNotExist(err) {
				t.Fatalf("staging file retained: %v", err)
			}
		})
	}
	path := filepath.Join(t.TempDir(), "existing")
	if err := os.WriteFile(path, []byte("keep"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := NewWriter(path, fixtureSpec()); err == nil {
		t.Fatal("overwrote existing file")
	}
	data, _ := os.ReadFile(path)
	if string(data) != "keep" {
		t.Fatal("existing data damaged")
	}
}

func TestReadRejectsInteriorForeignKeyAndMutations(t *testing.T) {
	for _, kind := range []string{"mixed", "delete", "sequence", "range-delete", "range-key"} {
		t.Run(kind, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "legacy.sst")
			f, err := vfs.Default.Create(path)
			if err != nil {
				t.Fatal(err)
			}
			w := sstable.NewWriter(objstorageprovider.NewFileWritable(f), sstable.WriterOptions{TableFormat: sstable.TableFormatPebblev4})
			k, _ := Encode(fixtureKey(fixtureSpec(), 0))
			k2, _ := Encode(fixtureKey(fixtureSpec(), 2))
			switch kind {
			case "mixed":
				if err = w.Set(k, nil); err != nil {
					t.Fatal(err)
				}
				foreign := fixtureKey(fixtureSpec(), 1)
				foreign.Part[15]++
				encoded, _ := Encode(foreign)
				err = w.Set(encoded, nil)
			case "delete":
				err = w.Delete(k)
			case "sequence":
				err = w.Add(pebble.InternalKey{UserKey: k, Trailer: (1 << 8) | uint64(pebble.InternalKeyKindSet)}, nil)
			case "range-delete":
				err = w.DeleteRange(k, k2)
			case "range-key":
				err = w.RangeKeySet(k, k2, nil, []byte("value"))
			}
			if err != nil {
				t.Fatal(err)
			}
			if err = w.Close(); err != nil {
				t.Fatal(err)
			}
			if err = Read(context.Background(), path, nil, nil); err == nil {
				t.Fatal("accepted invalid SST")
			}
		})
	}
}

func TestManifestAndCoverageReceipts(t *testing.T) {
	_, d := writeFixture(t)
	empty := sha256.Sum256(nil)
	m := Manifest{Version: SchemaVersion, Group: d.Scope.Group, Generation: 7, CatalogRoot: empty, PolicyRoot: empty, TombstoneRoot: empty, Parts: []Descriptor{d}, Frontiers: []StreamFrontier{{Incarnation: PartID{9}, Stream: "input-0", Cursor: 11}}}
	root, err := m.Digest()
	if err != nil {
		t.Fatal(err)
	}
	digest, _ := d.Digest()
	target := PartID{42}
	receipt := Receipt{Version: SchemaVersion, Manifest: root, Part: digest, TargetIncarnation: target, InstalledGeneration: 19}
	if err := receipt.Covers(m, d, target, 19); err != nil {
		t.Fatal(err)
	}
	if err := receipt.Covers(m, d, PartID{43}, 19); err == nil {
		t.Fatal("receipt survived target reinstall")
	}
	if err := receipt.Covers(m, d, target, 20); err == nil {
		t.Fatal("receipt covered wrong generation")
	}
	bad := d
	bad.Coverage.MaxTime++
	if err := receipt.Validate(root, bad); err == nil {
		t.Fatal("receipt covered changed time range")
	}
	bad = d
	bad.Coverage.Names++
	if err := receipt.Validate(root, bad); err == nil {
		t.Fatal("receipt covered changed name count")
	}
	b, err := m.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	got, err := ParseManifest(b)
	if err != nil {
		t.Fatal(err)
	}
	gotRoot, _ := got.Digest()
	if gotRoot != root {
		t.Fatal("manifest root changed")
	}
	m.Generation++
	if err := receipt.Covers(m, d, target, 19); err == nil {
		t.Fatal("receipt survived snapshot replacement")
	}
	m.Generation--
	m.Parts = append(m.Parts, d)
	if _, err := m.Digest(); err == nil {
		t.Fatal("duplicate part")
	}
	m.Parts = []Descriptor{d}
	m.Group += "foreign"
	if _, err := m.Digest(); err == nil {
		t.Fatal("mixed group")
	}
	if _, err := ParseManifest([]byte(strings.TrimSuffix(string(b), "}") + `,"extra":true}`)); err == nil {
		t.Fatal("unknown manifest field")
	}
}

func TestPartAdmissionClock(t *testing.T) {
	for _, clock := range []int64{0, 1700000000} {
		s := fixtureSpec()
		s.MinClock = clock
		w, err := NewWriter(filepath.Join(t.TempDir(), "clock.sst"), s)
		if err != nil {
			t.Fatal(err)
		}
		if err := w.Set(fixtureKey(s, 0), []byte{1}); err != nil {
			t.Fatal(err)
		}
		d, err := w.Close()
		if err != nil {
			t.Fatal(err)
		}
		b, err := d.MarshalBinary()
		if err != nil {
			t.Fatal(err)
		}
		if bytes.Contains(b, []byte(`"MinClock"`)) != (clock != 0) {
			t.Fatalf("clock omission: %s", b)
		}
		got, err := ParseDescriptor(b)
		if err != nil || got.Coverage.MinClock != clock {
			t.Fatalf("clock %d: %+v %v", clock, got.Coverage, err)
		}
	}
	s := fixtureSpec()
	s.MinClock = -1
	if err := validateSpec(s); err == nil {
		t.Fatal("accepted negative admission clock")
	}
}
