package v2core

import (
	"slices"
	"testing"
)

// Wrap every dispatch lease many times with changing batch sizes and overwrite
// the caller's buffer immediately. Also exercise multiple DBs per execution owner.
func TestCoreRoutingLeaseWrapAndIndependentOwners(t *testing.T) {
	t.Parallel()
	for _, owners := range []int{1, 2, 4} {
		t.Run(string(rune('0'+owners)), func(t *testing.T) {
			t.Parallel()
			cfg := coreTestConfig()
			cfg.Owners = owners
			e, err := openCore(t.TempDir(), cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := e.close(); err != nil {
					t.Error(err)
				}
			}()
			var want []row
			for b := 1; b <= 40; b++ {
				rs := coreRows(0, b)
				rs = rs[:1+b%len(rs)]
				want = append(want, rs...)
				if err := e.submit(0, rs, uint64(b), false); err != nil {
					t.Fatal(err)
				}
				clear(rs)
			}
			if err := e.submit(0, nil, 41, true); err != nil {
				t.Fatal(err)
			}
			for id := uint64(0); id < 11; id++ {
				var expected []row
				for _, r := range want {
					if r.ID == id {
						expected = append(expected, r)
					}
				}
				slices.SortFunc(expected, less)
				snap := e.owners[coreShard(id, cfg.Shards)].db.NewSnapshot()
				got, err := coreRead(snap, id)
				_ = snap.Close()
				if err != nil || !slices.Equal(got, expected) {
					t.Fatalf("ID%d bits/order mismatch: %v", id, err)
				}
			}
		})
	}
}

func TestCoreOwnerGeometryMayChangeOnReopen(t *testing.T) {
	t.Parallel()
	cfg := coreTestConfig()
	root := t.TempDir()
	e, err := openCore(root, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err = e.submit(0, coreRows(0, 1), 1, true); err != nil {
		t.Fatal(err)
	}
	if err = e.close(); err != nil {
		t.Fatal(err)
	}
	cfg.Owners = 1
	e, err = openCore(root, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if got := e.globalFrontier(); got[0] != 1 {
		t.Fatal(got)
	}
	if err = e.close(); err != nil {
		t.Fatal(err)
	}
}
