package whisperio

import (
	"context"
	"fmt"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/inputlog"
	"github.com/go-graphite/go-carbon/points"
)

// This harness deliberately uses synchronous durable acceptance. It proves the
// adapter cannot promote uncommitted state or make acknowledged points depend
// on retained RAM. It does not certify asynchronous production log retirement,
// power loss inside fsync, SST/manifest failure, or checkpoint retirement.
func TestOwnerHeadsCrashReplay(t *testing.T) {
	const env = "GO_CARBON_OWNER_HEAD_CRASH_ROOT"
	if root := os.Getenv(env); root != "" {
		phase := os.Getenv("GO_CARBON_OWNER_HEAD_CRASH_PHASE")
		ctx := context.Background()
		codec := &ownerCrashCodec{ContinuationCodec: &ContinuationCodec{OwnerHeads: NewOwnerHeads(2, 128<<10), ReuseBlockKeys: true}, phase: phase}
		opts := coldOptions(codec)
		opts.Now = classicTestClock
		db, err := store.Open(filepath.Join(root, "db"), opts)
		if err != nil {
			t.Fatal(err)
		}
		db.StopBackground()
		journal, err := inputlog.Open(filepath.Join(root, "input"), inputlog.Options{SyncInterval: time.Hour, SegmentSize: 1 << 20, Workers: 1})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := journal.Replay(func(*points.Points) {}); err != nil {
			t.Fatal(err)
		}
		src := &ownerLogSource{}
		ref := inputlog.NewStreamRef("durable")
		for _, p := range ownerFaultPoints() {
			src.entries = append(src.entries, ref.Entry(p.Value, p.Timestamp))
		}
		journal.Start(src, 16)
		if phase == "before-log-sync" {
			os.Exit(73)
		}
		if err := journal.Sync(); err != nil {
			t.Fatal(err)
		}
		writeOwnerReceipt(t, filepath.Join(root, "accepted"))
		if phase == "after-log-sync" {
			os.Exit(73)
		}
		if err := db.UpdateMany(ctx, "durable", ownerFaultPoints()); err != nil {
			t.Fatal(err)
		}
		if phase == "before-ack" {
			os.Exit(73)
		}
		writeOwnerReceipt(t, filepath.Join(root, "acked"))
		os.Exit(73) // bypass every graceful close and sync
	}
	for _, phase := range []string{"before-log-sync", "after-log-sync", "before-db-commit", "before-publish", "after-publish", "before-ack", "after-ack"} {
		t.Run(phase, func(t *testing.T) {
			root := t.TempDir()
			opts := coldOptions(&ContinuationCodec{})
			opts.Now = classicTestClock
			db, err := store.Open(filepath.Join(root, "db"), opts)
			if err != nil {
				t.Fatal(err)
			}
			cfg := store.MetricConfig{Name: "durable", Retentions: []store.Retention{{Step: 1, Count: 3600}, {Step: 60, Count: 1440}}, AggregationMethod: store.Last}
			if _, err := db.Create(t.Context(), cfg); err != nil {
				t.Fatal(err)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			cmd := exec.Command(os.Args[0], "-test.run=^TestOwnerHeadsCrashReplay$", "-test.timeout=30s")
			cmd.Env = append(os.Environ(), env+"="+root, "GO_CARBON_OWNER_HEAD_CRASH_PHASE="+phase)
			output, err := cmd.CombinedOutput()
			if exit, ok := err.(*exec.ExitError); !ok || exit.ExitCode() != 73 {
				t.Fatalf("fault not reached: %v\n%s", err, output)
			}
			journal, err := inputlog.Open(filepath.Join(root, "input"), inputlog.Options{SegmentSize: 1 << 20, Workers: 1})
			if err != nil {
				t.Fatal(err)
			}
			defer journal.Close()
			var replayed []store.Point
			stats, err := journal.Replay(func(p *points.Points) { replayed = append(replayed, p.Data...) })
			if err != nil || stats.Corrupt != 0 {
				t.Fatalf("journal replay: %+v %v", stats, err)
			}
			_, accepted := os.Stat(filepath.Join(root, "accepted"))
			_, acked := os.Stat(filepath.Join(root, "acked"))
			if accepted == nil {
				want := ownerFaultPoints()
				if len(replayed) != len(want) {
					t.Fatal("durably accepted input missing from journal")
				}
				for i, p := range want {
					if p.Timestamp != replayed[i].Timestamp || math.Float64bits(p.Value) != math.Float64bits(replayed[i].Value) {
						t.Fatalf("input journal value bits/order differ at %d", i)
					}
				}
			} else if len(replayed) != 0 {
				t.Fatal("unexpected unsynchronized journal point")
			}
			opts = coldOptions(&ContinuationCodec{OwnerHeads: NewOwnerHeads(3, 128<<10), ReuseBlockKeys: true})
			opts.Now = classicTestClock
			db, err = store.Open(filepath.Join(root, "db"), opts)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			db.StopBackground()
			oracleOpts := coldOptions(&ContinuationCodec{ReuseBlockKeys: true})
			oracleOpts.Now = classicTestClock
			oracle, err := store.Open(filepath.Join(root, "oracle"), oracleOpts)
			if err != nil {
				t.Fatal(err)
			}
			defer oracle.Close()
			oracle.StopBackground()
			if _, err := oracle.Create(t.Context(), cfg); err != nil {
				t.Fatal(err)
			}
			committed := phase == "before-publish" || phase == "after-publish" || phase == "before-ack" || phase == "after-ack"
			if committed {
				if err := oracle.UpdateMany(t.Context(), "durable", ownerFaultPoints()); err != nil {
					t.Fatal(err)
				}
			}
			// A durable ack requires the original database update, before replay.
			if acked == nil && !committed {
				t.Fatal("acknowledgement preceded commit")
			}
			compareOwnerStores(t, oracle, db, "durable", classicTestClock())
			if len(replayed) > 0 {
				for _, target := range []*store.Store{db, oracle} {
					if err := target.UpdateMany(t.Context(), "durable", replayed); err != nil {
						t.Fatal(err)
					}
				}
			}
			// Every archive and encoded output must also match after replay,
			// including duplicate replay of an already committed correction.
			compareOwnerStores(t, oracle, db, "durable", classicTestClock())
		})
	}
}

type ownerCrashCodec struct {
	*ContinuationCodec
	phase string
}

func (c *ownerCrashCodec) Update(ctx context.Context, kv store.ContinuationIO, m store.Metadata, p []store.Point, r int, clock func() time.Time, maintenance *store.ContinuationMaintenance) error {
	err := c.ContinuationCodec.Update(ctx, ownerCrashIO{ContinuationIO: kv, phase: c.phase}, m, p, r, clock, maintenance)
	if err == nil && c.phase == "before-db-commit" {
		os.Exit(73)
	}
	return err
}

type ownerCrashIO struct {
	store.ContinuationIO
	phase string
}

func (o ownerCrashIO) AfterContinuationCommit(fn func(store.Metadata)) {
	o.ContinuationIO.(store.ContinuationCommitObserver).AfterContinuationCommit(func(m store.Metadata) {
		if o.phase == "before-publish" {
			os.Exit(73)
		}
		fn(m)
		if o.phase == "after-publish" {
			os.Exit(73)
		}
	})
}
func (o ownerCrashIO) TimeBuckets() bool {
	v, ok := o.ContinuationIO.(store.ContinuationTimeBuckets)
	return ok && v.TimeBuckets()
}
func (o ownerCrashIO) ContinuationRevision() uint64 {
	return o.ContinuationIO.(store.ContinuationRevision).ContinuationRevision()
}
func (o ownerCrashIO) Probe(k []byte) ([]byte, error) {
	return o.ContinuationIO.(store.ContinuationProber).Probe(k)
}
func writeOwnerReceipt(t *testing.T, path string) {
	t.Helper()
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = fmt.Fprintln(f, "durable acceptance boundary"); err != nil {
		t.Fatal(err)
	}
	if err = f.Sync(); err != nil {
		t.Fatal(err)
	}
	if err = f.Close(); err != nil {
		t.Fatal(err)
	}
}

type ownerLogSource struct {
	sync.Mutex
	entries []inputlog.Entry
}

func (s *ownerLogSource) TakeLogEntries(shard int, _ uint64, spare []inputlog.Entry) []inputlog.Entry {
	s.Lock()
	defer s.Unlock()
	if shard != 0 {
		return spare[:0]
	}
	out := s.entries
	s.entries = spare[:0]
	return out
}
func (*ownerLogSource) LogLowWater() uint64         { return 0 }
func (*ownerLogSource) RelogBefore(uint64, int) int { return 0 }
func (*ownerLogSource) LogDropped() uint64          { return 0 }

func (o ownerCrashIO) ContinuationCacheScope() *byte {
	return o.ContinuationIO.(store.ContinuationCommitObserver).ContinuationCacheScope()
}

func ownerFaultPoints() []store.Point {
	bits := []uint64{0, 0x8000000000000000, 1, 0x8000000000000001, 0x7ff0000000000000, 0xfff0000000000000, 0x7ff8000000000042, 0x7ff0000000000001, 0xfff8000000001234, 0x0010000000000000, 0x7fefffffffffffff, 0x4045000000000000, 0x400921fb54442d18, 0xc05ec00000000000}
	out := make([]store.Point, len(bits))
	for i, v := range bits {
		out[i] = store.Point{Timestamp: 999970 + int64(i), Value: math.Float64frombits(v)}
	}
	return out
}
