package downsize

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type restartCatalogWatch struct {
	*readyFixture
	disallowList bool
}

func (f *restartCatalogWatch) List(ctx context.Context, host, prefix, after string, limit int) ([]string, error) {
	if f.disallowList {
		return nil, errors.New("restart repeated discovery")
	}
	return f.fakeSync.List(ctx, host, prefix, after, limit)
}

func fixedRestartFixture(t *testing.T) (*restartCatalogWatch, syncOptions, syncReadyOptions) {
	t.Helper()
	f, o, r := newReadyFixture(t, 100)
	o.namesFile = filepath.Join(t.TempDir(), "names")
	var names strings.Builder
	for i := 0; i < 100; i++ {
		fmt.Fprintf(&names, "metric.%03d\n", i)
	}
	names.WriteString("new.during.restart\n")
	if err := os.WriteFile(o.namesFile, []byte(names.String()), 0644); err != nil {
		t.Fatal(err)
	}
	o.namesHash = "fixed-test-inventory"
	return &restartCatalogWatch{readyFixture: f}, o, r
}

func TestReadyFixedRestartReusesCatalog(t *testing.T) {
	t.Parallel()
	f, o, r := fixedRestartFixture(t)
	clock := syncTestNow - 7200
	wall := time.Now()
	o.now = func() time.Time { return time.Unix(clock, 0).Add(time.Since(wall)) }
	r.period[0].Until = syncTestNow - 10800
	err := runSyncReady(context.Background(), &o, r, f, func() error {
		if err := runSync(context.Background(), o, f); err != nil {
			return err
		}
		f.disallowList = true
		clock = syncTestNow
		f.starts[syncT]++
		// A donor can gain a previously absent explicit name. Old source masks
		// must not hide its points in the restart window.
		f.put(syncD1, "new.during.restart", []syncArchive{{Step: 3600, Points: 240}})
		for ts := range f.store(syncD1).points["new.during.restart"][3600] {
			if ts < clock-7200 || ts >= clock+1 {
				delete(f.store(syncD1).points["new.during.restart"][3600], ts)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	paths, err := filepath.Glob(filepath.Join(o.state, "ready-recovery-*", "union.tsv"))
	if err != nil || len(paths) != 1 {
		t.Fatalf("recovery catalogs: %v %v", paths, err)
	}
	a, err := os.Stat(filepath.Join(o.state, "union.tsv"))
	if err != nil {
		t.Fatal(err)
	}
	b, err := os.Stat(paths[0])
	if err != nil {
		t.Fatal(err)
	}
	if !os.SameFile(a, b) {
		t.Fatal("catalog bytes copied instead of reused")
	}
}

func TestReadyRestartCatalogRejectsUnprovenState(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"scope", "truncated", "missing-catalog", "truncated-catalog", "cancelled"} {
		t.Run(mode, func(t *testing.T) {
			f, o, _ := fixedRestartFixture(t)
			if err := runSync(context.Background(), o, f); err != nil {
				t.Fatal(err)
			}
			recovery := o
			recovery.restartWindow = true
			recovery.fullHistory = false
			recovery.windows = syncRanges{{"gap", syncTestNow - 7200, syncTestNow - 3600}}
			recovery.state = filepath.Join(o.state, "ready-recovery-test")
			ctx := context.Background()
			switch mode {
			case "scope":
				o.namesHash = "changed"
			case "truncated":
				if err := os.WriteFile(filepath.Join(o.state, "sync.jsonl"), []byte("{\"kind\":\"header\"}"), 0644); err != nil {
					t.Fatal(err)
				}
			case "missing-catalog":
				if err := os.Remove(filepath.Join(o.state, "union.tsv")); err != nil {
					t.Fatal(err)
				}
			case "truncated-catalog":
				if err := os.Truncate(filepath.Join(o.state, "union.tsv"), 0); err != nil {
					t.Fatal(err)
				}
			case "cancelled":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}
			if err := seedReadyRecoveryCatalog(ctx, o, recovery); err == nil {
				t.Fatal("unproven catalog accepted")
			}
		})
	}
}

func TestReadyRestartCatalogPreservesExistingRecovery(t *testing.T) {
	t.Parallel()
	f, o, _ := fixedRestartFixture(t)
	if err := runSync(context.Background(), o, f); err != nil {
		t.Fatal(err)
	}
	recovery := o
	recovery.restartWindow = true
	recovery.state = filepath.Join(o.state, "ready-recovery-test")
	recovery.fullHistory = false
	recovery.windows = syncRanges{{"gap", syncTestNow - 7200, syncTestNow - 3600}}
	if err := seedReadyRecoveryCatalog(context.Background(), o, recovery); err != nil {
		t.Fatal(err)
	}
	if err := runSync(context.Background(), recovery, f); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(filepath.Join(recovery.state, "sync.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	// Re-entering after a store/client interruption must retain every receipt.
	if err := seedReadyRecoveryCatalog(context.Background(), o, recovery); err != nil {
		t.Fatal(err)
	}
	after, err := os.ReadFile(filepath.Join(recovery.state, "sync.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	if string(before) != string(after) {
		t.Fatal("existing recovery overwritten")
	}
}

type fixedRecoveryDigestWatch struct {
	*recoveryDigestWatch
	disallowList bool
}

func (b *fixedRecoveryDigestWatch) List(ctx context.Context, store, prefix, after string, limit int) ([]string, error) {
	if b.disallowList {
		return nil, errors.New("interrupted recovery repeated discovery")
	}
	return b.fakeSync.List(ctx, store, prefix, after, limit)
}

func TestReadyFixedRestartMidVerify(t *testing.T) {
	t.Parallel()
	for _, restartAgain := range []bool{false, true} {
		t.Run(map[bool]string{false: "same-process", true: "another-store-restart"}[restartAgain], func(t *testing.T) {
			f, o, r := fixedRestartFixture(t)
			o.batch, o.streams, o.verifyStreams = 10, 1, 1
			clock := syncTestNow - 7200
			wall := time.Now()
			o.now = func() time.Time { return time.Unix(clock, 0).Add(time.Since(wall)) }
			r.period[0].Until = syncTestNow - 10800
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			b := &fixedRecoveryDigestWatch{recoveryDigestWatch: &recoveryDigestWatch{readyFixture: f.readyFixture, cancel: cancel}}
			err := runSyncReady(ctx, &o, r, b, func() error {
				if err := runSync(ctx, o, b); err != nil {
					return err
				}
				clock = syncTestNow
				f.starts[syncT]++
				b.armed, b.disallowList = true, true
				return nil
			})
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("interruption: %v", err)
			}
			if len(b.names) != 10 {
				t.Fatalf("verified prefix = %v", b.names)
			}
			prefix := append([]string(nil), b.names...)
			b.names, b.calls, b.cancel = nil, 0, nil
			clock += 60
			if restartAgain {
				clock += 3600
				f.starts[syncT]++
			}
			if err := runSyncReady(context.Background(), &o, r, b, func() error { return errors.New("reentered initial copy") }); err != nil {
				t.Fatal(err)
			}
			want := 91
			if restartAgain {
				want += 101
			}
			if len(b.names) != want {
				t.Fatalf("got %d gap comparisons, want %d", len(b.names), want)
			}
			for _, n := range b.names[:91] {
				for _, done := range prefix {
					if n == done {
						t.Fatalf("repeated verified prefix %s", n)
					}
				}
			}
		})
	}
}
