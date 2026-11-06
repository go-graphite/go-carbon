package chunkstore

import (
	"crypto/sha256"
	"fmt"
	"strings"
	"testing"
)

func TestNativeDeferredIdentityPreservesPhaseOne(t *testing.T) {
	r := NativeRange{Operation: strings.Repeat("1", 64), Plan: strings.Repeat("2", 64), Stream: strings.Repeat("3", 64), Source: "donor", End: 2, Names: []string{"a", "b"}}
	first, names, err := r.Identity()
	if err != nil {
		t.Fatal(err)
	}
	want := sha256.Sum256([]byte(fmt.Sprintf("native-v1/%s/%s/%s/%s/%d/%d/%s", r.Operation, r.Plan, r.Stream, r.Source, r.Start, r.End, names)))
	if first != fmt.Sprintf("%x", want) {
		t.Fatal("changed phase-1 receipt identity")
	}
	r.Combine = true
	combine, _, err := r.Identity()
	if err != nil || first == combine {
		t.Fatal("combine aliases phase-1 receipt", err)
	}
	r.DeferredFrom, r.Selection = first, []byte{1}
	a, _, err := r.Identity()
	if err != nil {
		t.Fatal(err)
	}
	r.Selection = []byte{2}
	b, _, err := r.Identity()
	if err != nil || a == b || b == combine {
		t.Fatal("selection aliases another attempt", err)
	}
	r.Candidates = []byte{0}
	c, _, err := r.Identity()
	if err != nil || c != b {
		t.Fatal("transient candidates change identity", err)
	}
	for _, invalid := range [][]byte{nil, {}, {4}, {255}, {1, 0}} {
		r.Selection = invalid
		if _, _, err := r.Identity(); err == nil {
			t.Fatal("accepted invalid selection", invalid)
		}
	}
	r.Selection, r.Combine = []byte{1}, false
	if _, _, err := r.Identity(); err == nil {
		t.Fatal("phase 1 accepted a phase-2 selection")
	}
}
