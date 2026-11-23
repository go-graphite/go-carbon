package v2core

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"reflect"
	"slices"
	"testing"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2part"
)

func TestRollupParallelPreparationMatchesSerial(t *testing.T) {
	serial, parallel := newCoreRollups(), newCoreRollups()
	serial.parallelism, parallel.parallelism = 1, 4
	policies := make([][]byte, 512)
	for i := range policies {
		_, policies[i] = rollupTestPolicy(t, fmt.Sprintf("namespace.metric.%d", i), "1s:1h,1m:1d", sealrollup.Average, .5)
	}
	aborted := false
	for epoch := uint64(1); epoch <= 4; epoch++ {
		batches := make([]Batch, len(policies))
		for i := range batches {
			batches[i] = Batch{ID: uint64(i + 1), Policy: policies[i], Order: epoch, Clock: 1000 + int64(epoch), Points: []Row{{ID: uint64(i + 1), Time: 990 + int64(epoch), Bits: uint64(i) << 32, Seq: epoch}}}
		}
		left, e := serial.plan(SealInput{Batches: batches})
		if e != nil {
			t.Fatal(e)
		}
		right, e := parallel.plan(SealInput{Batches: batches})
		if e != nil {
			t.Fatal(e)
		}
		normalize := func(p SealPlan) map[string][]byte {
			out := map[string][]byte{}
			for _, file := range p.Files {
				for _, kv := range file {
					k, e := v2part.Decode(kv.Key)
					if e != nil {
						t.Fatal(e)
					}
					k.Part = v2part.PartID{}
					key, e := v2part.Encode(k)
					if e != nil {
						t.Fatal(e)
					}
					out[string(key)] = bytes.Clone(kv.encodedValue())
				}
			}
			return out
		}
		if !reflect.DeepEqual(normalize(left), normalize(right)) {
			t.Fatalf("parallel output differs at epoch%d", epoch)
		}
		if !aborted {
			left.Abort()
			right.Abort()
			epoch = 0
			aborted = true
			continue
		}
		left.Commit()
		right.Commit()
	}
}

func TestApplicationPolicyOwnsCanonicalDefinition(t *testing.T) {
	_, encoded := rollupTestPolicy(t, "namespace.metric", "1s:1h", sealrollup.Last, .5)
	original := bytes.Clone(encoded)
	p, e := NewApplicationPolicy("namespace.metric", encoded)
	if e != nil {
		t.Fatal(e)
	}
	clear(encoded)
	if !bytes.Equal(p.definitionBytes(), original) || v2part.Route(p.policy.Name) != v2part.Route("namespace.metric") {
		t.Fatal("registered policy aliases caller or changes route")
	}
	if _, e = NewApplicationPolicy("another.metric", original); e == nil {
		t.Fatal("accepted mismatched identity")
	}
}

func TestRollupStagesPreserveKeyOrderAfterCatalogGrowth(t *testing.T) {
	r := newCoreRollups()
	r.parallelism = 4
	policies := make([][]byte, 512)
	for i := range policies {
		p, _ := rollupTestPolicy(t, fmt.Sprintf("namespace.metric.%03d", 511-i), "1s:1h,1m:1d", sealrollup.Last, .5)
		if i%2 == 0 {
			p.Group = "another-group"
		}
		var err error
		policies[i], err = EncodeRollupPolicy(p)
		if err != nil {
			t.Fatal(err)
		}
	}
	for epoch := uint64(1); epoch <= 6; epoch++ {
		count := 256
		if epoch > 1 {
			count = 512
		}
		var batches []Batch
		for i := range count {
			// Later seals touch a sparse subset in reverse admission order,
			// including names first encountered by the aborted second seal.
			if epoch >= 4 && i%17 != 0 {
				continue
			}
			bits := [...]uint64{0, 1 << 63, 1, 0x7ff8000000000042, 0x7ff0000000000000, 0xfff0000000000000}
			batches = append(batches, Batch{ID: uint64(i + 1), Policy: policies[i], Order: epoch, Clock: 1000 + int64(epoch), Points: []Row{
				{Time: 990 + int64(epoch), Seq: epoch*3 + 1, Bits: bits[i%len(bits)]},
				{Time: 989, Seq: epoch*3 + 2, Bits: bits[(i+1)%len(bits)]},
				{Time: 989, Seq: epoch*3 + 3, Bits: bits[(i+2)%len(bits)]},
			}})
		}
		slices.Reverse(batches)
		plan, err := r.plan(SealInput{Batches: batches})
		if err != nil {
			t.Fatal(err)
		}
		for _, file := range plan.Files {
			for i := 1; i < len(file); i++ {
				if bytes.Compare(file[i-1].Key, file[i].Key) >= 0 {
					t.Fatalf("unordered output at epoch%d entry%d", epoch, i)
				}
			}
		}
		// The UUID is the only nondeterministic key field. Keep all stored
		// values, including metadata and IEEE bits, in the cross-runtime oracle.
		digest := sha256.New()
		legacyDigest := sha256.New()
		for _, file := range plan.Files {
			for _, kv := range file {
				key, err := v2part.Decode(kv.Key)
				if err != nil {
					t.Fatal(err)
				}
				key.Part = v2part.PartID{}
				encoded, err := v2part.Encode(key)
				if err != nil {
					t.Fatal(err)
				}
				digest.Write(encoded)
				digest.Write(kv.encodedValue())
				legacyDigest.Write(encoded)
				value := kv.encodedValue()
				if key.Archive == rollupMetadataArchive {
					meta, err := DecodeRollupMetadata(key.Metric, key.Group, value)
					if err != nil {
						t.Fatal(err)
					}
					meta.Version = 1
					value = AppendRollupMetadata(nil, meta)
				}
				legacyDigest.Write(value)
			}
		}
		t.Logf("epoch=%d ordered-record-sha256=%x", epoch, digest.Sum(nil))
		// This fixture never meets the coarse XFF threshold. Its six original
		// stored-record digests remain exact after removing only the new metadata
		// envelope and closure watermark, which older readers cannot understand.
		legacyWant := []string{"e11721303e3f2cf994e8a222eb226abf276be21c4a08b3089ecea66ba16ee5b1", "bfcae9042506b59d4a2e3ec0093aca20f0cfcf5f021f25bba3dd10df9e8c9632", "c52588bfee6ecb666cac8e891fac1ebaf133d1e73fe9029c7b9ef4b254d4c1f7", "ee9a265822d31d8698fccd61a2e8b7e2be93f9245ede776f56621a68980d8e64", "da551dd1f350ab6326fe6511561ecf30cff2bef3701e02c6ef2c9c7357267f42", "e46918ee8dd7b1d1ef736f1833bedc85796d0d7cf2f6bf4af5a01b689a5111a8"}[epoch-1]
		if got := fmt.Sprintf("%x", legacyDigest.Sum(nil)); got != legacyWant {
			t.Fatalf("legacy envelope digest %s want %s", got, legacyWant)
		}
		t.Logf("epoch=%d legacy-metadata-sha256=%x", epoch, legacyDigest.Sum(nil))
		if epoch == 2 {
			plan.Abort()
		} else {
			plan.Commit()
		}
	}
}
