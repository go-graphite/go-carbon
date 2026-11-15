package partsread

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
)

func tokenCatalog() Catalog {
	return CatalogFuncs{LookupFunc: func(n string) (Metric, bool) {
		return Metric{ID: 1, Archives: []Archive{{Step: 1, Points: 100}}, AggregationMethod: "last"}, n == "m"
	}, FindFunc: func(context.Context, string, func(string) error) error { return nil }}
}
func TestPinIntoReuseAndGeneration(t *testing.T) {
	p := new(Publication)
	p.Publish(View{Catalog: tokenCatalog()})
	defer p.Close()
	b := NewBudget(1024)
	var h Handle
	h.Close()
	if err := p.PinInto(&h, b, 512); err != nil {
		t.Fatal(err)
	}
	token, err := h.Resolve("m")
	if err != nil {
		t.Fatal(err)
	}
	if err := p.PinInto(&h, b, 512); !errors.Is(err, ErrHandleActive) {
		t.Fatalf("active reuse=%v", err)
	}
	p.Publish(View{Catalog: tokenCatalog()})
	h.Close()
	h.Close()
	if b.Used() != 0 {
		t.Fatal("budget leak")
	}
	if err := p.PinInto(&h, b, 512); err != nil {
		t.Fatal(err)
	}
	if _, err := h.RenderResolved(context.Background(), token, 80, 90, 100); !errors.Is(err, ErrMetadataGeneration) {
		t.Fatalf("old token=%v", err)
	}
	token, err = h.Resolve("m")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := h.RenderResolved(ctx, token, 80, 90, 100); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled=%v", err)
	}
	h.Close()
	if err := p.PinInto(&h, b, 2048); !errors.Is(err, ErrAdmission) {
		t.Fatalf("admission=%v", err)
	}
	if b.Used() != 0 {
		t.Fatal("failed admission leaked")
	}
	if err := p.PinInto(&h, b, 512); err != nil {
		t.Fatal(err)
	}
	h.Close()
}
func TestSnapshotBorrowConcurrentDrain(t *testing.T) {
	var closed atomic.Int64
	p := new(Publication)
	p.Publish(View{Catalog: tokenCatalog(), Close: func() { closed.Add(1) }})
	snap, err := p.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	refs := snap.v.refs.Load()
	b := NewBudget(8 << 20)
	var wg sync.WaitGroup
	start := make(chan struct{})
	release := make(chan struct{})
	ready := make(chan struct{}, 8)
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			var h Handle
			if err := snap.BorrowInto(&h, b, 1024); err != nil {
				t.Error(err)
				return
			}
			ready <- struct{}{}
			<-release
			h.Close()
		}()
	}
	close(start)
	for range 8 {
		<-ready
	}
	if snap.v.refs.Load() != refs {
		t.Fatal("borrow changed publication pins")
	}
	p.Close()
	if closed.Load() != 0 {
		t.Fatal("snapshot released before drain")
	}
	close(release)
	wg.Wait()
	if b.Used() != 0 {
		t.Fatal("budget leaked")
	}
	snap.Close()
	snap.Close()
	if closed.Load() != 1 {
		t.Fatalf("close=%d", closed.Load())
	}
	var h Handle
	if err := snap.BorrowInto(&h, b, 1024); !errors.Is(err, ErrClosed) {
		t.Fatalf("afterclose=%v", err)
	}
}
