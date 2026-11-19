package v2part

import (
	"bytes"
	"context"
	"path/filepath"
	"testing"
)

// Seq and the admission clock range round trip, leave the part bytes and
// legacy descriptors unchanged, and reject inverted or half-known ranges.
func TestDescriptorSeqClock(t *testing.T) {
	spec, input := denseGoldenInput(t)
	path := filepath.Join(t.TempDir(), "seq.sst")
	w, err := NewDenseWriter(path, spec)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Abort()
	for _, kv := range input {
		if err := w.SetEncoded(kv[0], kv[1]); err != nil {
			t.Fatal(err)
		}
	}
	d, err := w.Close()
	if err != nil {
		t.Fatal(err)
	}
	legacy, err := d.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(legacy, []byte(`"Seq"`)) || bytes.Contains(legacy, []byte(`Clock"`)) {
		t.Fatalf("zero fields are not omitted: %s", legacy)
	}
	stamped := d
	stamped.Seq, stamped.MinClock, stamped.MaxClock = 42, 1794701390, 1794701400
	encoded, err := stamped.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := ParseDescriptor(encoded)
	if err != nil || parsed.Seq != 42 || parsed.MinClock != 1794701390 || parsed.MaxClock != 1794701400 {
		t.Fatalf("round trip: %+v %v", parsed, err)
	}
	if parsed.SHA256 != d.SHA256 || parsed.Bytes != d.Bytes {
		t.Fatal("publication fields changed part identity")
	}
	if err := Read(context.Background(), path, &parsed, nil); err != nil {
		t.Fatal(err)
	}
	if old, err := ParseDescriptor(legacy); err != nil || old.Seq != 0 || old.MaxClock != 0 {
		t.Fatalf("legacy descriptor: %+v %v", old, err)
	}
	for _, bad := range [][2]int64{{2, 1}, {0, 1}, {-1, 1}} {
		broken := stamped
		broken.MinClock, broken.MaxClock = bad[0], bad[1]
		if _, err := broken.MarshalBinary(); err == nil {
			t.Fatalf("invalid clock range accepted: %v", bad)
		}
	}
}
