package whisperio

import (
	"context"
	"math"
	"reflect"
	"testing"
)

type snapshotProbeIO struct {
	mapContinuationIO
	probes int
}

func (kv *snapshotProbeIO) Probe(key []byte) ([]byte, error) {
	kv.probes++
	return kv.mapContinuationIO.Get(key)
}

// Compare the real classic fetch and digest drivers through the optional Probe
// interface against the former Get-only IO, including wrap and missing slots.
func TestClassicSnapshotProbesBitExact(t *testing.T) {
	kv, m, clock := classicFetchFixture(t)
	probed := &snapshotProbeIO{mapContinuationIO: kv}
	codec := &ContinuationCodec{}
	for _, window := range [][2]int{{0, 1000}, {751, 998}, {0, 60}, {1, 900}, {998, 1001}} {
		want, err := codec.Fetch(context.Background(), kv, m, window[0], window[1], clock)
		if err != nil {
			t.Fatal(err)
		}
		before := probed.probes
		got, err := codec.Fetch(context.Background(), probed, m, window[0], window[1], clock)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got.Metadata, want.Metadata) || got.FromTime != want.FromTime || got.UntilTime != want.UntilTime || got.Step != want.Step || len(got.Values) != len(want.Values) {
			t.Fatalf("window %v: shape changed", window)
		}
		for i, v := range want.Values {
			if math.Float64bits(v) != math.Float64bits(got.Values[i]) {
				t.Fatalf("window %v slot %d: bits differ", window, i)
			}
		}
		if probed.probes == before {
			t.Fatal("snapshot did not probe point groups")
		}
		wantSum, err := codec.SumDigest(context.Background(), kv, m, window[0], window[1], clock)
		if err != nil {
			t.Fatal(err)
		}
		gotSum, err := codec.SumDigest(context.Background(), probed, m, window[0], window[1], clock)
		if err != nil || !reflect.DeepEqual(gotSum, wantSum) {
			t.Fatalf("window %v digest: %+v/%v vs %+v", window, gotSum, err, wantSum)
		}
	}
	// Mutable main-file reads must retain their fresh-lookup policy.
	fs := newContinuationFiles(probed, false)
	f := &continuationFile{fs: fs}
	before := probed.probes
	if _, err := f.pointBlockAccess([]byte("absent"), true); err != nil {
		t.Fatal(err)
	}
	if probed.probes != before {
		t.Fatal("writer probed main-file point group")
	}
}
