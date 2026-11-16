package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/go-graphite/go-carbon/internal/v2placement"
)

func TestPlacementSelection(t *testing.T) {
	b, e := os.ReadFile("../../internal/v2placement/testdata/map-v1.json")
	if e != nil {
		t.Fatal(e)
	}
	d, e := v2placement.Decode(bytes.NewReader(b))
	if e != nil {
		t.Fatal(e)
	}
	path := filepath.Join(t.TempDir(), "map.json")
	if e = os.WriteFile(path, b, 0600); e != nil {
		t.Fatal(e)
	}
	args := []string{"--map", path, "--name", "probe.metric.273", "--replica", "site-a", "--expect-digest", d.Digest(), "--expect-epoch", "1"}
	var out bytes.Buffer
	if e = placement(args, &out); e != nil {
		t.Fatal(e)
	}
	var selected v2placement.Selection
	if e = json.Unmarshal(out.Bytes(), &selected); e != nil {
		t.Fatal(e)
	}
	want, e := d.Select("probe.metric.273", "site-a")
	if e != nil {
		t.Fatal(e)
	}
	if selected.Group != want.Group || selected.Replica.Writer != want.Replica.Writer || selected.MapDigest != d.Digest() {
		t.Fatal("selection mismatch")
	}
	args[len(args)-1] = "2"
	out.Reset()
	if e = placement(args, &out); !errors.Is(e, v2placement.ErrStaleEpoch) {
		t.Fatal(e)
	}
	if out.Len() != 0 {
		t.Fatal("stale selection leaked output")
	}
	if e = placement(args[:len(args)-2], &out); e == nil {
		t.Fatal("missing expected epoch accepted")
	}
}
