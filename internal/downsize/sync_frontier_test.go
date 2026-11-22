package downsize

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"sync"
	"testing"
	"time"
)

type proofFixture struct {
	*fakeSync
	epoch       string
	unknown     map[string]bool
	unsupported bool
	seenMu      sync.Mutex
	seen        map[string]bool
	mutate      func(string, int64)
}

func (f *proofFixture) Proofs(_ context.Context, store string, names []string) ([]string, error) {
	if f.unsupported {
		return nil, errSyncNoProof
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]string, len(names))
	for i, n := range names {
		if f.unknown[n] {
			continue
		}
		h := sha256.New()
		fmt.Fprintln(h, f.epoch, store, n, f.store(store).archives[n])
		var steps []int64
		for step := range f.store(store).points[n] {
			steps = append(steps, step)
		}
		sort.Slice(steps, func(i, j int) bool { return steps[i] < steps[j] })
		for _, step := range steps {
			var times []int64
			for ts := range f.store(store).points[n][step] {
				times = append(times, ts)
			}
			sort.Slice(times, func(i, j int) bool { return times[i] < times[j] })
			for _, ts := range times {
				fmt.Fprintln(h, step, ts, math.Float64bits(f.store(store).points[n][step][ts]))
			}
		}
		out[i] = hex.EncodeToString(h.Sum(nil))
	}
	return out, nil
}
func (f *proofFixture) Digest(ctx context.Context, store string, names []string, from, until int64) (map[string][]syncDigest, error) {
	f.seenMu.Lock()
	for _, n := range names {
		f.seen[n] = true
	}
	f.seenMu.Unlock()
	got, err := f.fakeSync.Digest(ctx, store, names, from, until)
	if f.mutate != nil {
		f.mutate(store, from)
	}
	return got, err
}
func frontierFixture(t *testing.T) (*proofFixture, syncOptions) {
	t.Helper()
	f := &proofFixture{fakeSync: newFakeSync(), epoch: "one", unknown: map[string]bool{}, seen: map[string]bool{}}
	f.listPageSize = 11 // two pages, distinct from the seven-name verification batches
	for _, host := range []string{syncT, syncD1, syncD2} {
		for i := range 20 {
			f.put(host, fmt.Sprintf("metric.%02d", i), []syncArchive{{3600, 240}})
		}
	}
	delete(f.store(syncT).points["metric.19"][3600], syncTestNow-5*3600)
	// Frontier fixtures exercise the opt-in continuous convergence behavior.
	o := syncOptions{rescanAll: true, to: syncT, from: []string{syncD1, syncD2}, state: t.TempDir(), batch: 7, streams: 2, maxRounds: 1, noCopy: true, now: func() time.Time { return time.Unix(syncTestNow, 0) }}
	return f, o
}
func seenProofNames(f *proofFixture) []string {
	var names []string
	for n := range f.seen {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}
func TestSyncTerminalRetryMutationFrontier(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		change func(*proofFixture)
		extra  []string
		all    bool
	}{
		{name: "residual only"},
		{name: "target delete", change: func(f *proofFixture) {
			delete(f.store(syncT).archives, "metric.02")
			delete(f.store(syncT).points, "metric.02")
		}, extra: []string{"metric.02"}},
		{name: "target recreate", change: func(f *proofFixture) { f.put(syncT, "metric.02", []syncArchive{{3600, 240}}, syncTestNow-5*3600) }, extra: []string{"metric.02"}},
		{name: "historical write", change: func(f *proofFixture) { f.store(syncT).points["metric.02"][3600][syncTestNow-5*3600] = 42 }, extra: []string{"metric.02"}},
		{name: "donor write", change: func(f *proofFixture) { f.store(syncD2).points["metric.02"][3600][syncTestNow-5*3600] = 42 }, extra: []string{"metric.02"}},
		{name: "serving index", change: func(f *proofFixture) { f.hide["metric.02"] = true }, extra: []string{"metric.02"}},
		{name: "policy", change: func(f *proofFixture) { f.store(syncT).archives["metric.02"] = []syncArchive{{3600, 241}} }, extra: []string{"metric.02"}},
		{name: "unknown", change: func(f *proofFixture) { f.unknown["metric.02"] = true }, extra: []string{"metric.02"}},
		{name: "epoch", change: func(f *proofFixture) { f.epoch = "two" }, all: true},
		{name: "old endpoint", change: func(f *proofFixture) { f.unsupported = true }, all: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f, o := frontierFixture(t)
			if err := runSync(context.Background(), o, f); !errors.Is(err, ErrSyncResidual) {
				t.Fatal(err)
			}
			f.seen = map[string]bool{}
			if tc.change != nil {
				tc.change(f)
			}
			// Batch boundaries and client wall time are not a new verification scope.
			o.batch = 3
			o.now = func() time.Time { return time.Unix(syncTestNow+3600, 0) }
			if err := runSync(context.Background(), o, f); !errors.Is(err, ErrSyncResidual) {
				t.Fatal(err)
			}
			want := append([]string{"metric.19"}, tc.extra...)
			if tc.all {
				want = nil
				for i := range 20 {
					want = append(want, fmt.Sprintf("metric.%02d", i))
				}
			}
			sort.Strings(want)
			if got := seenProofNames(f); !reflect.DeepEqual(got, want) {
				t.Fatalf("hashed %v, want %v", got, want)
			}
			if stat, err := os.Stat(filepath.Join(o.state, "verification-frontier.bin")); err != nil || stat.Size() != 20*32 {
				t.Fatalf("compact index: %v %v", stat, err)
			}
		})
	}
}
func TestSyncFrontierTornRecordAndChangedScope(t *testing.T) {
	t.Parallel()
	f, o := frontierFixture(t)
	if err := runSync(context.Background(), o, f); !errors.Is(err, ErrSyncResidual) {
		t.Fatal(err)
	}
	file, err := os.OpenFile(filepath.Join(o.state, "verification-frontier.bin"), os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = file.WriteAt([]byte{0}, 2*32+16); err != nil {
		t.Fatal(err)
	}
	file.Close()
	f.seen = map[string]bool{}
	if err = runSync(context.Background(), o, f); !errors.Is(err, ErrSyncResidual) {
		t.Fatal(err)
	}
	if got := seenProofNames(f); !reflect.DeepEqual(got, []string{"metric.02", "metric.19"}) {
		t.Fatal(got)
	}
	o.settle = time.Hour
	if err = runSync(context.Background(), o, f); err == nil {
		t.Fatal("changed verification policy reused clean proof")
	}
}
func TestSyncFrontierWriteDuringVerification(t *testing.T) {
	t.Parallel()
	f, o := frontierFixture(t)
	o.noCopy = false
	o.maxRounds = 0
	var once sync.Once
	f.mutate = func(host string, from int64) {
		if host == syncT && from < syncTestNow-86400 {
			once.Do(func() {
				f.mu.Lock()
				defer f.mu.Unlock()
				delete(f.store(syncT).points["metric.02"][3600], syncTestNow-6*3600)
			})
		}
	}
	if err := runSync(context.Background(), o, f); err != nil {
		t.Fatal(err)
	}
	if _, ok := f.store(syncT).points["metric.02"][3600][syncTestNow-6*3600]; !ok {
		t.Fatal("concurrent mutation was certified clean")
	}
}

type readyProofFixture struct {
	*readyFixture
	proof *proofFixture
}

func (f readyProofFixture) Proofs(ctx context.Context, host string, names []string) ([]string, error) {
	return f.proof.Proofs(ctx, host, names)
}
func TestSyncFrontierReadyVerdicts(t *testing.T) {
	t.Parallel()
	for _, hole := range []bool{false, true} {
		for _, copy := range []bool{false, true} {
			var verdicts []string
			for _, proof := range []bool{false, true} {
				f, o, r := newReadyFixture(t, 100)
				o.noCopy = !copy
				if hole {
					delete(f.store(syncT).points["metric.002"][3600], syncTestNow-5*3600)
				}
				var b syncReadyBackend = f
				if proof {
					b = readyProofFixture{f, &proofFixture{fakeSync: f.fakeSync, epoch: "one"}}
				}
				_ = runSyncReady(context.Background(), &o, r, b, func() error { return runSync(context.Background(), o, b) })
				verdicts = append(verdicts, readReadyReport(t, o.state).Verdict)
			}
			want := "READY"
			if hole && !copy {
				want = "NOT_READY"
			}
			if verdicts[0] != verdicts[1] || verdicts[0] != want {
				t.Fatalf("hole=%v copy=%v verdicts=%v", hole, copy, verdicts)
			}
		}
	}
}

func TestSyncFrontierCannotCertifyExpandedPeriod(t *testing.T) {
	t.Parallel()
	f, o, r := newReadyFixture(t, 100)
	b := readyProofFixture{f, &proofFixture{fakeSync: f.fakeSync, epoch: "one"}}
	if err := runSyncReady(context.Background(), &o, r, b, func() error { return runSync(context.Background(), o, b) }); err != nil {
		t.Fatal(err)
	}
	o.now = func() time.Time { return time.Unix(syncTestNow+7200, 0) }
	r.period[0].Until = syncTestNow + 3600
	if err := runSyncReady(context.Background(), &o, r, b, func() error { return runSync(context.Background(), o, b) }); !errors.Is(err, ErrSyncNotReady) {
		t.Fatalf("expanded period certified: %v", err)
	}
	if report := readReadyReport(t, o.state); report.Verdict != "NOT_READY" || report.HistoryVerifiedUntil != syncTestNow {
		t.Fatalf("missing scope evidence: %+v", report)
	}
}

func TestSyncFrontierNewDonorAfterDiscovery(t *testing.T) {
	t.Parallel()
	f, o := frontierFixture(t)
	ts := int64(syncTestNow - 6*3600)
	delete(f.store(syncT).points["metric.02"][3600], ts)
	delete(f.store(syncD1).points["metric.02"][3600], ts)
	delete(f.store(syncD2).archives, "metric.02")
	delete(f.store(syncD2).points, "metric.02")
	if err := runSync(context.Background(), o, f); !errors.Is(err, ErrSyncResidual) {
		t.Fatal(err)
	}
	f.put(syncD2, "metric.02", []syncArchive{{3600, 240}})
	if err := runSync(context.Background(), o, f); !errors.Is(err, ErrSyncResidual) {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(o.state, "residual.json"))
	if err != nil {
		t.Fatal(err)
	}
	var residual []syncResidual
	if err = json.Unmarshal(data, &residual); err != nil {
		t.Fatal(err)
	}
	for _, r := range residual {
		if r.Name == "metric.02" && r.Source == syncD2 {
			return
		}
	}
	t.Fatal("a donor absent from the original catalog mask was not compared")
}

// Repairing one residual must not hide a subsequent write to a clean name.
type mutateCleanDuringCopy struct {
	*proofFixture
	once sync.Once
}

func (f *mutateCleanDuringCopy) Copy(ctx context.Context, source, target string, names []string, journal string) (syncCopyResult, error) {
	r, err := f.fakeSync.Copy(ctx, source, target, names, journal)
	f.once.Do(func() {
		f.mu.Lock()
		defer f.mu.Unlock()
		delete(f.store(syncT).points["metric.02"][3600], syncTestNow-6*3600)
	})
	return r, err
}
func TestSyncFrontierRechecksCleanNamesBetweenRounds(t *testing.T) {
	t.Parallel()
	f, o := frontierFixture(t)
	o.noCopy = false
	o.maxRounds = 0
	b := &mutateCleanDuringCopy{proofFixture: f}
	if err := runSync(context.Background(), o, b); err != nil {
		t.Fatal(err)
	}
	if _, ok := f.store(syncT).points["metric.02"][3600][syncTestNow-6*3600]; !ok {
		t.Fatal("write between rounds escaped mutation frontier")
	}
}

func TestSyncFrontierArchiveSelectionAgesWithoutWrite(t *testing.T) {
	t.Parallel()
	f, o := frontierFixture(t)
	for _, host := range []string{syncT, syncD1, syncD2} {
		f.store(host).archives["metric.02"] = []syncArchive{{60, 20}, {3600, 240}}
	}
	o.windows = syncRanges{{"gap", syncTestNow - 1000, syncTestNow - 100}}
	s := &syncer{o: o, b: f, frontier: &syncFrontier{key: "scope"}}
	s.frontier.scope.Now = syncTestNow
	before, err := s.proofStamps(context.Background(), []string{"metric.02"})
	if err != nil {
		t.Fatal(err)
	}
	s.o.now = func() time.Time { return time.Unix(syncTestNow+1000, 0) }
	after, err := s.proofStamps(context.Background(), []string{"metric.02"})
	if err != nil {
		t.Fatal(err)
	}
	if before[0] == (syncProof{}) || before[0] == after[0] {
		t.Fatal("serving archive rollover reused old clean proof")
	}
}
