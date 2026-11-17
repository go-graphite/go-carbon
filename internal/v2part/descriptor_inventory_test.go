package v2part

import (
	"context"
	"path/filepath"
	"slices"
	"testing"
)

func TestDescriptorVShardInventory(t *testing.T) {
	spec, input := denseGoldenInput(t)
	path := filepath.Join(t.TempDir(), "inventory.sst")
	w, err := NewDenseWriter(path, spec)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Abort()
	var shards []uint16
	for _, kv := range input {
		key, err := Decode(kv[0])
		if err != nil {
			t.Fatal(err)
		}
		if len(shards) == 0 || shards[len(shards)-1] != key.VShard {
			shards = append(shards, key.VShard)
		}
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
	d.VShards = shards
	encoded, err := d.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := ParseDescriptor(encoded)
	if err != nil || !slices.Equal(parsed.VShards, shards) {
		t.Fatalf("inventory round trip: %v %v", parsed.VShards, err)
	}
	if err := Read(context.Background(), path, &parsed, nil); err != nil {
		t.Fatal(err)
	}
	old, err := ParseDescriptor(legacy)
	if err != nil || len(old.VShards) != 0 {
		t.Fatalf("legacy descriptor rejected: %v", err)
	}
	if err := Read(context.Background(), path, &old, nil); err != nil {
		t.Fatal(err)
	}
	for _, bad := range [][]uint16{{shards[0], shards[0], shards[1]}, {shards[1], shards[0]}, {VirtualShards}, {shards[0]}} {
		broken := d
		broken.VShards = bad
		if _, err := broken.MarshalBinary(); err == nil {
			t.Fatalf("invalid inventory accepted: %v", bad)
		}
	}
	if len(shards) != 2 || shards[1]-shards[0] < 2 {
		t.Fatal("fixture needs separated routed shards")
	}
	// Correct endpoints and checksum cannot authenticate a forged interior
	// inventory: the portable reader also compares it with actual SST keys.
	forged := d
	forged.VShards = []uint16{shards[0], shards[0] + 1, shards[1]}
	if err := forged.Validate(); err != nil {
		t.Fatal(err)
	}
	if err := Read(context.Background(), path, &forged, nil); err == nil {
		t.Fatal("forged inventory accepted")
	}
}
