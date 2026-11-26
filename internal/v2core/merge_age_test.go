package v2core

import (
	"encoding/hex"
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/v2part"
)

// A short run of old, balanced neighbours must make progress without another
// seal. Fresh runs still batch, and an oversized live Seq remains a barrier.
func TestMergeAgedAdjacentRange(t *testing.T) {
	t.Setenv("CORE_MERGE_WORKERS", "0")
	e := mergeFixtureIdle(t, t.TempDir(), nil)
	defer e.close()
	o := e.owners[0]
	for n := uint64(1); n <= sealedMergeFanIn/2; n++ {
		mergeAppend(t, e, n)
	}
	live := seqLiveParts(t, o)
	// Equal advertised sizes isolate the age gate from encoding-size noise.
	for i := range live {
		live[i].Bytes = 100
	}
	replaceDescriptors(t, o, live)
	s := o.db.NewSnapshot()
	defer s.Close()
	selectParts := func() []v2part.Descriptor {
		t.Helper()
		ds, err := o.selectMergeLimit(s, math.MaxUint64, false)
		if err != nil {
			t.Fatal(err)
		}
		return ds
	}
	if ds := selectParts(); len(ds) != 0 {
		t.Fatalf("fresh short range selected: %d", len(ds))
	}
	old := time.Now().Add(-time.Minute)
	for _, d := range live {
		if err := os.Chtimes(filepath.Join(o.dir, "parts", hex.EncodeToString(d.SHA256[:])+".sst"), old, old); err != nil {
			t.Fatal(err)
		}
	}
	ds := selectParts()
	if len(ds) < 2 {
		t.Fatalf("aged adjacent range stranded: %d", len(ds))
	}
	seqContiguous(t, live, ds)
	// No candidate may jump over an oversized live Seq, even after aging.
	live[1].Bytes = 1000
	replaceDescriptors(t, o, live)
	if ds, err := o.selectMergeLimit(s, 300, false); err != nil || len(ds) != 0 {
		t.Fatalf("skipped live Seq: %d %v", len(ds), err)
	}
}
