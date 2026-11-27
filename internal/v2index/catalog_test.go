package v2index

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"sync"
	"testing"
)

func TestCatalogMergeCancellation(t *testing.T) {
	c := &Catalog{}
	if err := c.Publish(1, []string{"existing.name"}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := c.ApplyBeforePublishContext(ctx, 2, []string{"new.name"}, nil, func(*Snapshot) error {
		t.Fatal("canceled merge reached quota publication")
		return nil
	}); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if c.Snapshot().Revision() != 1 {
		t.Fatal("canceled merge changed the published snapshot")
	}
}

func TestCatalogPublication(t *testing.T) {
	c := &Catalog{}
	input := []string{"a.child", "a", "a", "z.cpu"}
	if err := c.Publish(1, input); err != nil {
		t.Fatal(err)
	}
	old := c.Snapshot()
	input[0] = "overwritten"
	if err := c.Apply(2, []string{"new.cpu", "z.cpu"}, []string{"a.child", "z.cpu"}); err != nil {
		t.Fatal(err)
	}
	if got := slices.Collect(c.Snapshot().Names()); !reflect.DeepEqual(got, []string{"a", "new.cpu"}) {
		t.Fatal(got)
	}
	if got := slices.Collect(old.Names()); !reflect.DeepEqual(got, []string{"a", "a.child", "z.cpu"}) {
		t.Fatal("snapshot changed", got)
	}
	for _, err := range []error{c.Publish(2, nil), c.Apply(1, nil, nil), c.Publish(3, []string{"bad/path"}), c.Apply(3, []string{"bad..component"}, nil)} {
		if err == nil {
			t.Fatal("invalid publication accepted")
		}
	}
	if c.Snapshot().Revision() != 2 {
		t.Fatal("failed publication changed revision")
	}
	if err := c.Apply(3, []string{"a.child"}, nil); err != nil {
		t.Fatal(err)
	}
	if c.Snapshot().Len() != 3 {
		t.Fatal("recreation missing")
	}
	if err := c.Publish(4, nil); err != nil {
		t.Fatal(err)
	}
	if c.Snapshot().Len() != 0 {
		t.Fatal("empty publication lost")
	}
}

func TestCatalogConcurrentReaders(t *testing.T) {
	c := &Catalog{}
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 1000 {
				s := c.Snapshot()
				if !slices.IsSorted(slices.Collect(s.Names())) {
					t.Error("not sorted")
				}
			}
		}()
	}
	for i := uint64(1); i < 100; i++ {
		if err := c.Apply(i, []string{fmt.Sprintf("service.%03d", i)}, nil); err != nil {
			t.Fatal(err)
		}
	}
	wg.Wait()
}

// The writer state has no lock: ordered writers on different goroutines are
// fine, overlapping ones panic instead of racing on the background merge job.
func TestCatalogSingleWriter(t *testing.T) {
	c := &Catalog{}
	if err := c.SetDir(t.TempDir()); err != nil {
		t.Fatal(err)
	}
	if err := c.Publish(1, []string{"a.base"}); err != nil {
		t.Fatal(err)
	}
	handoff := make(chan error)
	go func() { handoff <- c.Apply(2, []string{"b.other"}, nil) }() // starts a base merge
	if err := <-handoff; err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	go func() {
		handoff <- c.ApplyBeforePublish(3, []string{"c.blocked"}, nil, func(*Snapshot) error { close(entered); <-release; return nil })
	}()
	<-entered
	for name, write := range map[string]func(){
		"publish": func() { _ = c.Publish(4, nil) },
		"apply":   func() { _ = c.Apply(4, []string{"d.late"}, nil) },
		"close":   c.Close,
	} {
		func() {
			defer func() {
				if r := recover(); r != "v2 index: concurrent catalog writers" {
					t.Errorf("%s during a blocked Apply: recovered %v", name, r)
				}
			}()
			write()
		}()
	}
	close(release)
	if err := <-handoff; err != nil {
		t.Fatal(err)
	}
	c.Close()
	if got := slices.Collect(c.Snapshot().Names()); !reflect.DeepEqual(got, []string{"a.base", "b.other", "c.blocked"}) {
		t.Fatal(got)
	}
}

// TestPublishSortedNames checks the single validation pass: sorted unique
// input is used as is, a sorted duplicate or unsorted input is still
// deduplicated, and the caller's slice is never modified.
func TestPublishSortedNames(t *testing.T) {
	var c Catalog
	for i, tc := range []struct{ in, want []string }{
		{[]string{"a.b", "a.c", "b"}, []string{"a.b", "a.c", "b"}},
		{[]string{"a.b", "a.c", "a.c", "b"}, []string{"a.b", "a.c", "b"}},
		{[]string{"b", "a.c", "a.b", "b"}, []string{"a.b", "a.c", "b"}},
	} {
		in := slices.Clone(tc.in)
		if err := c.Publish(uint64(i+1), in); err != nil {
			t.Fatal(err)
		}
		if !slices.Equal(in, tc.in) {
			t.Fatalf("case %d: input modified: %q", i, in)
		}
		if s := c.Snapshot(); s.Len() != len(tc.want) || !s.EqualNames(tc.want) {
			t.Fatalf("case %d: got %q", i, slices.Collect(s.Names()))
		}
	}
	if err := c.Publish(9, []string{"a.b", "a..c"}); err == nil {
		t.Fatal("invalid name published")
	}
}
