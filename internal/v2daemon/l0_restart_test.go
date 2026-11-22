package v2daemon

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2core"
)

func l0Config() Config {
	// One owner: with several, a cut owner blocks its execution group, and the
	// rows of owners that had not published are legitimately lost.
	return Config{Layer: 3, Producers: 1, Shards: 1, GroupInterval: time.Hour}
}

// TestL0SIGKILL kills a producer-journal-free store with a real SIGKILL at
// every L0 and seal fence and requires the classic whisper image on restart,
// twice. Only a kill before the L0 rename may lose the second group: its
// points were never visible.
func TestL0SIGKILL(t *testing.T) {
	if root := os.Getenv("V2_L0_CHILD_ROOT"); root != "" {
		stage := os.Getenv("V2_L0_CHILD_STAGE")
		var armed atomic.Bool
		var once sync.Once
		cut := func(s string) {
			if armed.Load() && s == stage {
				once.Do(func() {
					fmt.Println("CUT")
					for {
						time.Sleep(time.Second)
					}
				})
			}
		}
		s, err := openWithHooks(root, l0Config(), v2core.Hooks{Stage: cut})
		if err != nil {
			t.Fatal(err)
		}
		armed.Store(true)
		if err = s.Producers[0].AppendApplications(restartApps(t, 6, 12)); err != nil {
			t.Fatal(err)
		}
		if err = s.Producers[0].Drain(); err != nil {
			t.Fatal(err)
		}
		if err = s.Core.AppendBatches(0, nil, s.Producers[0].cursor, true); err != nil {
			t.Fatal(err)
		}
		t.Fatal("missed crash stage", stage)
	}
	for _, stage := range []string{"l0-written", "l0-renamed", "seal", "commit"} {
		t.Run(stage, func(t *testing.T) {
			root := t.TempDir()
			cfg := l0Config()
			s, err := Open(root, cfg)
			if err != nil {
				t.Fatal(err)
			}
			if err = s.Producers[0].AppendApplications(restartApps(t, 0, 6)); err != nil {
				t.Fatal(err)
			}
			if err = s.Close(); err != nil {
				t.Fatal(err)
			}
			cmd := exec.Command(os.Args[0], "-test.run=^TestL0SIGKILL$", "-test.timeout=30s")
			cmd.Env = append(os.Environ(), "V2_L0_CHILD_ROOT="+root, "V2_L0_CHILD_STAGE="+stage)
			pipe, err := cmd.StdoutPipe()
			if err != nil {
				t.Fatal(err)
			}
			var stderr synchronizedBuffer
			cmd.Stderr = &stderr
			if err = cmd.Start(); err != nil {
				t.Fatal(err)
			}
			defer func() { _ = cmd.Process.Kill() }()
			ready := make(chan bool, 1)
			go func() {
				scan := bufio.NewScanner(pipe)
				for scan.Scan() {
					if scan.Text() == "CUT" {
						ready <- true
						return
					}
				}
				ready <- false
			}()
			select {
			case ok := <-ready:
				if !ok {
					_ = cmd.Wait()
					t.Fatal("no crash barrier", stderr.String())
				}
			case <-time.After(20 * time.Second):
				_ = cmd.Process.Kill()
				_ = cmd.Wait()
				t.Fatal("crash barrier timeout", stderr.String())
			}
			if err = cmd.Process.Signal(syscall.SIGKILL); err != nil {
				t.Fatal(err)
			}
			err = cmd.Wait()
			exit := &exec.ExitError{}
			if !errors.As(err, &exit) || !exit.Sys().(syscall.WaitStatus).Signaled() {
				t.Fatalf("not SIGKILL: %v", err)
			}
			count := 12
			if stage == "l0-written" {
				count = 6
			}
			for restart := range 2 {
				s, err = Open(root, cfg)
				if err != nil {
					t.Fatal(err)
				}
				if order := s.Producers[0].applyOrder; order < uint64(count) {
					t.Fatalf("restart %d continues at order %d, below loaded %d", restart, order, count)
				}
				checkRestartOracle(t, s, restartApps(t, 0, count), 3)
				if err = s.Close(); err != nil {
					t.Fatal(err)
				}
				if files, _ := filepath.Glob(filepath.Join(root, "*", "core", "*", "l0", "l0-*")); len(files) != 0 {
					t.Fatalf("restart %d left L0 files %v", restart, files)
				}
			}
			// The store keeps accepting input above the restored order.
			s, err = Open(root, cfg)
			if err != nil {
				t.Fatal(err)
			}
			if err = s.Producers[0].AppendApplications(restartApps(t, 12, 16)); err != nil {
				t.Fatal(err)
			}
			if err = s.Producers[0].Drain(); err != nil {
				t.Fatal(err)
			}
			if count == 12 {
				checkRestartOracle(t, s, restartApps(t, 0, 16), 3)
			}
			if err = s.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// TestL0RefusesUnsealedJournal: an L0 build never replays a journal tail.
// The journal layer writes the same producer journal the old rollup build
// wrote; with no seal covering it, opening the root refuses.
func TestL0RefusesUnsealedJournal(t *testing.T) {
	legacy := t.TempDir()
	cfg := l0Config()
	cfg.Layer = 2
	s, err := Open(legacy, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Producers[0].AppendApplications(restartApps(t, 0, 6)); err != nil {
		t.Fatal(err)
	}
	if err = s.Producers[0].Drain(); err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	journal := filepath.Join("placement-group-00000001", "journal")
	if err = os.CopyFS(filepath.Join(root, journal), os.DirFS(filepath.Join(legacy, journal))); err != nil {
		t.Fatal(err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err = Open(root, l0Config()); !errors.Is(err, ErrLegacyJournal) {
		t.Fatalf("unsealed journal: %v, want ErrLegacyJournal", err)
	}
}

// TestL0KillFences is the production port of the B3-L0 oracle's SIGKILL
// fences: rounds groups, each published then sealed, and a real SIGKILL at the
// k-th L0 write, L0 rename, seal and seal commit for every k, plus one kill of
// an empty store (4*rounds+1 kills). After each kill the store must hold
// exactly the classic image of an application prefix that contains every
// application a reader saw. V2_L0_KILL_ROUNDS sets rounds (default 3; the
// oracle used 100).
func TestL0KillFences(t *testing.T) {
	if root := os.Getenv("V2_L0_FENCE_ROOT"); root != "" {
		stage := os.Getenv("V2_L0_FENCE_STAGE")
		k, _ := strconv.Atoi(os.Getenv("V2_L0_FENCE_K"))
		rounds, _ := strconv.Atoi(os.Getenv("V2_L0_FENCE_ROUNDS"))
		var hits atomic.Int64
		cut := func(s string) {
			if s == stage && hits.Add(1) == int64(k) {
				fmt.Println("CUT")
				for {
					time.Sleep(time.Second)
				}
			}
		}
		hooks := v2core.Hooks{
			Stage: cut,
			PublishPending: func(_ int, pin *pebble.Snapshot, _ v2core.PendingView, _ uint64, _ *v2core.RollupInventory) error {
				if pin != nil {
					return pin.Close()
				}
				return nil
			},
			VisiblePending: func(_ int, v v2core.PendingView) {
				var top uint64
				v.Each(func(a *v2core.PendingApplication) bool { top = max(top, a.Order); return true })
				fmt.Println("VIS", top)
			},
		}
		s, err := openWithHooks(root, l0Config(), hooks)
		if err != nil {
			t.Fatal(err)
		}
		if stage == "empty" {
			cut("empty")
		}
		for g := range rounds {
			if err = s.Producers[0].AppendApplications(restartApps(t, 2+2*g, 4+2*g)); err != nil {
				t.Fatal(err)
			}
			if err = s.Producers[0].Drain(); err != nil {
				t.Fatal(err)
			}
			if err = s.Core.AppendBatches(0, nil, s.Producers[0].cursor, true); err != nil {
				t.Fatal(err)
			}
		}
		t.Fatal("missed crash stage", stage, k)
	}
	rounds := 3
	if v := os.Getenv("V2_L0_KILL_ROUNDS"); v != "" {
		rounds, _ = strconv.Atoi(v)
	}
	type kill struct {
		stage string
		k     int
	}
	kills := []kill{{"empty", 1}}
	for _, stage := range []string{"l0-written", "l0-renamed", "seal", "commit"} {
		for k := 1; k <= rounds; k++ {
			kills = append(kills, kill{stage, k})
		}
	}
	exact := 0
	parent := t.TempDir()
	for i, c := range kills {
		// One store per kill, removed after its check: 401 stores fill a disk.
		root := filepath.Join(parent, strconv.Itoa(i))
		base := 2
		if c.stage == "empty" {
			base = 0
		} else {
			s, err := Open(root, l0Config())
			if err != nil {
				t.Fatal(err)
			}
			if err = s.Producers[0].AppendApplications(restartApps(t, 0, base)); err != nil {
				t.Fatal(err)
			}
			if err = s.Close(); err != nil {
				t.Fatal(err)
			}
		}
		cmd := exec.Command(os.Args[0], "-test.run=^TestL0KillFences$", "-test.timeout=60s")
		cmd.Env = append(os.Environ(), "V2_L0_FENCE_ROOT="+root, "V2_L0_FENCE_STAGE="+c.stage, fmt.Sprintf("V2_L0_FENCE_K=%d", c.k), fmt.Sprintf("V2_L0_FENCE_ROUNDS=%d", rounds))
		pipe, err := cmd.StdoutPipe()
		if err != nil {
			t.Fatal(err)
		}
		var stderr synchronizedBuffer
		cmd.Stderr = &stderr
		if err = cmd.Start(); err != nil {
			t.Fatal(err)
		}
		visible := uint64(base)
		var output synchronizedBuffer
		ready := make(chan bool, 1)
		go func() {
			scan := bufio.NewScanner(pipe)
			for scan.Scan() {
				line := scan.Text()
				if line == "CUT" {
					ready <- true
					return
				}
				if v, ok := strings.CutPrefix(line, "VIS "); ok {
					n, _ := strconv.ParseUint(v, 10, 64)
					visible = max(visible, n)
				} else {
					_, _ = output.Write([]byte(line + "\n"))
				}
			}
			ready <- false
		}()
		select {
		case ok := <-ready:
			if !ok {
				_ = cmd.Wait()
				t.Fatalf("%v: no crash barrier: %s", c, output.String())
			}
		case <-time.After(30 * time.Second):
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
			t.Fatalf("%v: crash barrier timeout: %s", c, stderr.String())
		}
		if err = cmd.Process.Signal(syscall.SIGKILL); err != nil {
			t.Fatal(err)
		}
		if err = cmd.Wait(); err == nil {
			t.Fatalf("%v: child exited cleanly", c)
		}
		s, err := Open(root, l0Config())
		if err != nil {
			t.Fatalf("%v: %v", c, err)
		}
		// The store's last order per name tells which prefix it holds.
		var held uint64
		for _, db := range s.Core.DBs() {
			snap := db.NewSnapshot()
			err := v2core.ScanRollups(snap, func(_ v2core.RollupPolicy, m v2core.RollupMetadata, _ []sealrollup.Record) error {
				held = max(held, m.Frontier.Order)
				return nil
			})
			_ = snap.Close()
			if err != nil {
				t.Fatal(err)
			}
		}
		// Loaded L0 rows are sealed by the oracle's own seal below; before it
		// the producer order already covers them.
		held = max(held, s.Producers[0].applyOrder)
		if held < visible {
			t.Fatalf("%v: store continues at order %d, below visible %d", c, held, visible)
		}
		if held > uint64(base+2*rounds) {
			t.Fatalf("%v: store holds order %d beyond input", c, held)
		}
		if held > 0 {
			checkRestartOracle(t, s, restartApps(t, 0, int(held)), 3)
		}
		if err = s.Close(); err != nil {
			t.Fatal(err)
		}
		if err = os.RemoveAll(root); err != nil {
			t.Fatal(err)
		}
		exact++
	}
	t.Logf("exact %d/%d kills (rounds %d)", exact, len(kills), rounds)
}
