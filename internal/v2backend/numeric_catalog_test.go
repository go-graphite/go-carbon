package v2backend

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/v2core"
)

func TestGlobalNumericCatalogStableAcrossStreamsAndRestart(t *testing.T) {
	opts := testOptions(t.TempDir(), simplePolicy())
	opts.WriteEngine, opts.Owners, opts.Shards = "core", 7, 3
	s, err := openAt(opts, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	want := make(map[string]uint64)
	for i := 0; i < 128; i++ {
		name := fmt.Sprintf("catalog.numeric.%d;tag=stable", i)
		r, err := s.resolve(name)
		if err != nil {
			t.Fatal(err)
		}
		if r.globalID == 0 || v2core.Shard(r.globalID, opts.Shards) != r.owner%opts.Shards {
			t.Fatal("invalid global route", r)
		}
		want[name] = r.globalID
	}
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for name, id := range want {
				r, e := s.resolve(name)
				if e != nil || r.globalID != id {
					t.Errorf("concurrent identity changed: %v %v", r, e)
				}
			}
		}()
	}
	wg.Wait()
	requireClose(t, s)
	s, err = openAt(opts, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	defer requireClose(t, s)
	for name, id := range want {
		r, e := s.resolve(name)
		if e != nil || r.globalID != id {
			t.Fatalf("restart identity changed: %v %v", r, e)
		}
	}
}
