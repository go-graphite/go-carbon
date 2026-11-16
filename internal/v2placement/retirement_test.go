package v2placement

import (
	"context"
	"testing"
)

func TestPartRetirementNeedsLocalFenceAndAdoptedSuccessor(t *testing.T) {
	old := fixture(t)
	next, activation := certificate(t, old)
	root := t.TempDir()
	source, err := OpenAuthority(root, old, activation.Fence.Group, activation.Fence.Replica, activation.Fence.Store)
	if err != nil {
		t.Fatal(err)
	}
	if err = source.AuthorizePartRetirement(activation); err == nil {
		t.Fatal("unfenced source accepted")
	}
	actual, err := source.Fence(context.Background(), next.Operation, activation.Fence.Ends, func() (Frontier, Roots, error) { return activation.Fence.Frontier, activation.Fence.Roots, nil })
	if err != nil {
		t.Fatal(err)
	}
	if actual.Digest() != activation.Fence.Digest() {
		t.Fatal("fixture fence differs")
	}
	if err = source.AuthorizePartRetirement(activation); err == nil {
		t.Fatal("fence without adoption accepted")
	}
	if err = source.Adopt(next, []Activation{activation}); err != nil {
		t.Fatal(err)
	}
	if err = source.AuthorizePartRetirement(activation); err != nil {
		t.Fatal(err)
	}
	source, err = OpenAuthority(root, next, activation.Fence.Group, activation.Fence.Replica, activation.Fence.Store)
	if err != nil {
		t.Fatal(err)
	}
	if err = source.AuthorizePartRetirement(activation); err != nil {
		t.Fatalf("retirement authority lost on restart: %v", err)
	}
	target, err := OpenAuthority(t.TempDir(), old, activation.Fence.Group, activation.Fence.Replica, activation.Coverage.Store)
	if err != nil {
		t.Fatal(err)
	}
	if err = target.Adopt(next, []Activation{activation}); err != nil {
		t.Fatal(err)
	}
	if err = target.AuthorizePartRetirement(activation); err == nil {
		t.Fatal("target accepted source retirement receipt")
	}
	tampered := activation
	tampered.Coverage.FenceDigest = ""
	if err = source.AuthorizePartRetirement(tampered); err == nil {
		t.Fatal("uninstalled activation accepted")
	}
}
