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
	"reflect"
	"slices"
	"testing"
	"testing/cryptotest"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2part"
)

// Seal parts are written at the fast Zstd level and merges at the rest level.
// Neither readers nor merges may observe the difference: every seal part
// decodes to the records of a rest-level part, and merging seal-level inputs
// yields the same bytes as merging rest-level inputs.
func TestSealCompressionDecodesAndMergesIdentically(t *testing.T) {
	if coreTestProcess(t) {
		return
	}
	root := t.TempDir()
	restDir, sealDir := filepath.Join(root, "rest"), filepath.Join(root, "seal")
	for _, dir := range []string{restDir, sealDir} {
		if err := os.MkdirAll(filepath.Join(dir, "parts"), 0700); err != nil {
			t.Fatal(err)
		}
	}
	var names []v2part.ID
	for candidate := 0; len(names) < 4096; candidate++ {
		name := v2part.ID(fmt.Sprintf("seal.service.%03d.timer.%06d", candidate%61, candidate))
		if v2part.Route(name) < 64 {
			names = append(names, name)
		}
	}
	rng := rand.New(rand.NewPCG(17, 29))
	var rest, seal []v2part.Descriptor
	for part := 0; part < 4; part++ {
		var rows []KV
		for n, name := range names {
			route := v2part.Route(name)
			p := RollupPolicy{Name: name, Group: "seal", Config: sealrollup.Config{Method: sealrollup.Average, XFilesFactor: .5}}
			p.Config.Archives = []sealrollup.Archive{{Step: 10, Count: 360}, {Step: 60, Count: 1440}}
			digest, err := sealrollup.PolicyDigest(p.Config)
			if err != nil {
				t.Fatal(err)
			}
			policy, err := json.Marshal(p)
			if err != nil {
				t.Fatal(err)
			}
			k := v2part.Key{Group: "seal", VShard: route, Part: v2part.PartID{byte(part + 1)}, Metric: name}
			order := uint64(part + 1)
			for slot := int64(0); slot < 4; slot++ {
				k.Archive, k.BlockStart = 0, int64(part)*4+slot
				enc, err := v2part.Encode(k)
				if err != nil {
					t.Fatal(err)
				}
				// Mostly smooth gauges, some counters and some full-entropy values.
				bits := math.Float64bits(float64(n%977) + float64(rng.Uint64N(1000))/8)
				switch n % 5 {
				case 0:
					bits = math.Float64bits(float64(int64(n)*1000 + k.BlockStart))
				case 1:
					bits = rng.Uint64()
				}
				r := sealrollup.Record{Delta: sealrollup.Delta{Archive: 0, Slot: k.BlockStart, Order: order, Sample: sealrollup.Sample{Time: 1000 + 10*k.BlockStart, Bits: bits, Sequence: order}}, Correction: slot == 3}
				rows = append(rows, KV{Key: enc, Value: sealrollup.EncodeRecord(r)})
			}
			k.Archive, k.BlockStart = v2part.MetadataArchive, 0
			enc, err := v2part.Encode(k)
			if err != nil {
				t.Fatal(err)
			}
			rows = append(rows, KV{Key: enc, Value: AppendRollupMetadata(nil, RollupMetadata{Version: 2, ID: uint64(n + 1), Policy: policy, Frontier: sealrollup.Frontier{Order: order, MaxTime: 1160, Policy: digest}, Revision: order})})
		}
		slices.SortFunc(rows, func(a, b KV) int { return bytes.Compare(a.Key, b.Key) })
		source := v2part.Provenance{Stream: "seal", Incarnation: v2part.PartID{42}, MinOrder: uint64(part + 1), MaxOrder: uint64(part + 1)}
		restPath := filepath.Join(root, fmt.Sprintf("rest-%d.sst", part))
		d, err := WriteDenseRollupPart(restPath, rows, source)
		if err != nil {
			t.Fatal(err)
		}
		// Live seals and dense portable parts store envelopes without the
		// definition; give the seal writer the rows a live seal emits.
		planned := make([]coreKV, len(rows))
		for i, kv := range rows {
			value := kv.Value
			k, err := v2part.Decode(kv.Key)
			if err != nil {
				t.Fatal(err)
			}
			if k.Archive == v2part.MetadataArchive {
				m, err := DecodeRollupMetadata(k.Metric, k.Group, value)
				if err != nil {
					t.Fatal(err)
				}
				value = appendRollupMetadataSlim(nil, m)
			}
			planned[i] = coreKV{Key: kv.Key, Value: value}
		}
		spec := v2part.Spec{Scope: d.Scope, Encoding: d.Encoding, Source: d.Source, MinTime: d.Coverage.MinTime, MaxTime: d.Coverage.MaxTime, Policies: d.Coverage.Policies, MinExpiry: d.Coverage.MinExpiry, MaxExpiry: d.Coverage.MaxExpiry}
		sealPath := filepath.Join(root, fmt.Sprintf("seal-%d.sst", part))
		s, err := writePlannedRollupPart(sealPath, planned, spec)
		if err != nil {
			t.Fatal(err)
		}
		if s.SHA256 == d.SHA256 {
			t.Fatal("seal part was not written at the seal level")
		}
		if !reflect.DeepEqual(s.Coverage, d.Coverage) || !bytes.Equal(s.FirstKey, d.FirstKey) || !bytes.Equal(s.LastKey, d.LastKey) {
			t.Fatal("seal level changed part coverage")
		}
		if want, got := readSealCompressionPart(t, restPath, d), readSealCompressionPart(t, sealPath, s); !reflect.DeepEqual(got, want) {
			t.Fatal("seal-level part decodes to different records")
		}
		t.Logf("part %d: %d rows, rest %d bytes, seal %d bytes", part, len(rows), d.Bytes, s.Bytes)
		for _, x := range []struct {
			dir, path string
			d         v2part.Descriptor
		}{{restDir, restPath, d}, {sealDir, sealPath, s}} {
			if err = os.Rename(x.path, filepath.Join(x.dir, "parts", hex.EncodeToString(x.d.SHA256[:])+".sst")); err != nil {
				t.Fatal(err)
			}
		}
		rest, seal = append(rest, d), append(seal, s)
	}
	merge := func(dir string, inputs []v2part.Descriptor) ([]byte, v2part.Descriptor) {
		// The merged part draws a random part identity; pin it per merge.
		cryptotest.SetGlobalRandom(t, 7)
		path := filepath.Join(dir, "merged.sst")
		d, err := mergePartFiles(context.Background(), dir, path, inputs, 0, nil)
		if err != nil {
			t.Fatal(err)
		}
		b, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		return b, d
	}
	restBytes, restMerged := merge(restDir, rest)
	sealBytes, sealMerged := merge(sealDir, seal)
	if !bytes.Equal(sealBytes, restBytes) || !reflect.DeepEqual(sealMerged, restMerged) {
		t.Fatalf("merge of seal-level inputs differs: %d vs %d bytes", len(sealBytes), len(restBytes))
	}
	if restMerged.Coverage.Records == 0 {
		t.Fatal("empty merge")
	}
}

func readSealCompressionPart(t *testing.T, path string, d v2part.Descriptor) []KV {
	t.Helper()
	var out []KV
	err := v2part.ReadExpanded(context.Background(), path, &d, func(k v2part.Key, value []byte) error {
		key, err := v2part.Encode(k)
		if err != nil {
			return err
		}
		out = append(out, KV{Key: key, Value: bytes.Clone(value)})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}
