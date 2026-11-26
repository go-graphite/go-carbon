package v2read

import (
	"context"
	"fmt"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2core"
	"github.com/go-graphite/go-carbon/internal/v2part"
)

// Exact reads use committed descriptors and dictionary lookups. A stopped
// writable-inventory recovery must neither delay them nor change their values.
// Discovery still fences on that recovery before exposing projected names.
func TestExactReadBeforeOwnerHydration(t *testing.T) {
	v2part.IDKeys.Store(true)
	defer v2part.IDKeys.Store(false)
	cfg := testConfig()
	cfg.SealInterval = time.Hour
	root := t.TempDir()
	open := func(stage func(string)) (*Reader, *v2core.Engine) {
		t.Helper()
		r, err := New(1, 1<<28, 1<<20)
		if err != nil {
			t.Fatal(err)
		}
		r.Now = func() time.Time { return time.Unix(1000, 0) }
		r.Identities = &v2core.PolicyCatalog{}
		e, err := v2core.Open(root, cfg, v2core.Hooks{Identities: r.Identities, IDKeys: true, PublishPending: r.PublishPending, Stage: stage, Now: r.Now})
		if err != nil {
			r.Close()
			t.Fatal(err)
		}
		return r, e
	}
	r, e := open(nil)
	names := []string{"cold.exact.first", "cold.exact.second"}
	for i, name := range names {
		id := uint64(i + 1)
		policy, err := v2core.EncodeRollupPolicy(v2core.RollupPolicy{Name: v2part.ID(name), Group: "test", Config: sealrollup.Config{Method: sealrollup.Last, Archives: []sealrollup.Archive{{Step: 1, Count: 100}}}})
		if err != nil {
			t.Fatal(err)
		}
		err = e.AppendBatches(0, []v2core.Batch{{ID: id, Order: id, Clock: 1000, Policy: policy, Points: []v2core.Row{{ID: id, Time: 990, Bits: math.Float64bits(float64(40 + i)), Seq: id}}}}, id, true)
		if err != nil {
			t.Fatal(err)
		}
	}
	dump := func() (string, error) {
		var out string
		err := r.FetchMany(context.Background(), names, 989, 992, 1000, func(name string, s *Series) error {
			out += fmt.Sprintf("%s %d %d %d %v %v\n", name, s.From, s.Until, s.Step, s.Values, s.Occupied)
			return nil
		})
		return out, err
	}
	want, err := dump()
	if err != nil {
		t.Fatal(err)
	}
	r.Close()
	if err = e.Close(); err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}, 1), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	r, e = open(func(stage string) {
		if stage == "owner-hydration" {
			entered <- struct{}{}
			<-release
		}
	})
	defer func() {
		unblock()
		r.Close()
		if err := e.Close(); err != nil {
			t.Error(err)
		}
	}()
	// Start the existing fence explicitly: no dependence on a maintenance tick.
	fenced := make(chan error, 1)
	go func() { fenced <- r.AwaitHydration(context.Background(), names...) }()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("owner recovery was not deferred")
	}
	read := make(chan error, 1)
	go func() {
		got, err := dump()
		if err == nil && got != want {
			err = fmt.Errorf("cold read differs: got %s want %s", got, want)
		}
		read <- err
	}()
	select {
	case err := <-read:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("exact read waited for writable inventory recovery")
	}
	discovered := make(chan error, 1)
	go func() { _, err := r.owners[0].Load().discover(context.Background()); discovered <- err }()
	select {
	case err := <-discovered:
		t.Fatalf("discovery crossed recovery fence: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	unblock()
	if err := <-fenced; err != nil {
		t.Fatal(err)
	}
	if err := <-discovered; err != nil {
		t.Fatal(err)
	}
	if got, err := dump(); err != nil || got != want {
		t.Fatalf("recovered read: %s %v; want %s", got, err, want)
	}
}
