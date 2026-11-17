package v2core

import (
	"bytes"
	"fmt"
	"reflect"
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
	if !bytes.Equal(p.definition, original) || v2part.Route(p.policy.Name) != v2part.Route("namespace.metric") {
		t.Fatal("registered policy aliases caller or changes route")
	}
	if _, e = NewApplicationPolicy("another.metric", original); e == nil {
		t.Fatal("accepted mismatched identity")
	}
}

func TestRollupOrdinalsPreserveKeyOrderAfterCatalogGrowth(t *testing.T) {
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
	for epoch := uint64(1); epoch <= 4; epoch++ {
		count := 256
		if epoch > 1 {
			count = 512
		}
		batches := make([]Batch, count)
		for i := range batches {
			batches[i] = Batch{ID: uint64(i + 1), Policy: policies[i], Order: epoch, Clock: 1000 + int64(epoch), Points: []Row{{Time: 990 + int64(epoch), Seq: epoch}}}
		}
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
		if epoch == 2 {
			plan.Abort()
		} else {
			plan.Commit()
		}
	}
}
