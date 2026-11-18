//go:build packedslots

package v2core

import (
	"context"
	"fmt"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2part"
)

type packedLogical struct {
	runs      map[string][]sealrollup.Record // name/archive -> records in slot order
	envelopes map[string]string              // name -> envelope bytes
}

func readPackedLogical(t *testing.T, path string, d *v2part.Descriptor, out packedLogical) {
	t.Helper()
	err := v2part.Read(context.Background(), path, d, func(k v2part.Key, value []byte) error {
		if k.Archive == v2part.MetadataArchive {
			out.envelopes[string(k.Metric)] = string(value)
			return nil
		}
		env, run, packed, err := v2part.SplitPacked(value)
		if err != nil {
			return err
		}
		if packed {
			if _, dup := out.envelopes[string(k.Metric)]; dup {
				return fmt.Errorf("duplicate envelope for %s", k.Metric)
			}
			out.envelopes[string(k.Metric)] = string(env)
		}
		records, err := v2part.DecodeSlots(k.Archive, k.BlockStart, run)
		if err != nil {
			return err
		}
		id := fmt.Sprintf("%s/%d", k.Metric, k.Archive)
		out.runs[id] = append(out.runs[id], records...)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

// TestPackedSlotsExpandDifferential: the packed layout carries exactly today's
// records and envelope bytes for every name, over the production planner.
func TestPackedSlotsExpandDifferential(t *testing.T) {
	shapes := []sealCPUShape{
		{"single10s", []sealrollup.Archive{{Step: 10, Count: 8640}}, sealrollup.Last, 0, 10},
		{"multi", []sealrollup.Archive{{Step: 10, Count: 8640}, {Step: 60, Count: 10080}, {Step: 600, Count: 52560}}, sealrollup.Average, 0, 10},
		{"single1s", []sealrollup.Archive{{Step: 1, Count: 86400}}, sealrollup.Last, 0, 1},
	}
	for _, shape := range shapes {
		for _, k := range []int{1, 3, 12, 70} {
			shape.points = k
			r, batches, points := sealCPUFixture(t, shape, 3000)
			plan, err := r.planParts(SealInput{Batches: batches})
			if err != nil {
				t.Fatal(err)
			}
			dir := t.TempDir()
			today := packedLogical{map[string][]sealrollup.Record{}, map[string]string{}}
			packed := packedLogical{map[string][]sealrollup.Record{}, map[string]string{}}
			var kvToday, kvPacked uint64
			for f, part := range plan.Parts {
				part.Spec.Source.Incarnation, part.Spec.Source.Stream = v2part.PartID{1}, "test"
				pt, pp := filepath.Join(dir, fmt.Sprintf("t%d.sst", f)), filepath.Join(dir, fmt.Sprintf("p%d.sst", f))
				dt, err := writePlannedRollupTuples(pt, part)
				if err != nil {
					t.Fatal(err)
				}
				dp, err := writePlannedRollupTuplesPacked(pp, part)
				if err != nil {
					t.Fatal(err)
				}
				kvToday, kvPacked = kvToday+dt.Coverage.Records, kvPacked+dp.Coverage.Records
				readPackedLogical(t, pt, &dt, today)
				readPackedLogical(t, pp, &dp, packed)
			}
			plan.Abort()
			n := 0
			for _, rs := range today.runs {
				n += len(rs)
			}
			if n < points || len(today.envelopes) != 3000 {
				t.Fatalf("%s/k%d: today has %d records for %d points, %d envelopes", shape.name, k, n, points, len(today.envelopes))
			}
			if !reflect.DeepEqual(today.runs, packed.runs) || !reflect.DeepEqual(today.envelopes, packed.envelopes) {
				t.Fatalf("%s/k%d: packed layout differs from today", shape.name, k)
			}
			if kvPacked != kvToday-3000 {
				t.Fatalf("%s/k%d: KVs today %d packed %d, want one fewer per name", shape.name, k, kvToday, kvPacked)
			}
			t.Logf("%s/k%d: %d records, KVs %d -> %d", shape.name, k, n, kvToday, kvPacked)
		}
	}
}
