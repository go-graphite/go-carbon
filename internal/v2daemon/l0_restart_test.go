package v2daemon

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
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
	"github.com/go-graphite/go-carbon/internal/partsformat"
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
		cfg := l0Config()
		cfg.L0Dir = os.Getenv("V2_L0_CHILD_L0DIR")
		s, err := openWithHooks(root, cfg, v2core.Hooks{Stage: cut})
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
	for _, stage := range []string{"l0-written", "l0-renamed", "seal", "commit", "l0-renamed/moved", "commit/moved"} {
		t.Run(stage, func(t *testing.T) {
			root := t.TempDir()
			cfg := l0Config()
			stage, moved := strings.CutSuffix(stage, "/moved")
			if moved {
				// L0 on its own directory, as v2.l0-dir puts it on tmpfs.
				cfg.L0Dir = filepath.Join(t.TempDir(), "shm", "l0")
			}
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
			cmd.Env = append(os.Environ(), "V2_L0_CHILD_ROOT="+root, "V2_L0_CHILD_STAGE="+stage, "V2_L0_CHILD_L0DIR="+cfg.L0Dir)
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
			if stage == "l0-renamed" {
				// The killed child's run file is where the config puts it.
				inRoot, _ := filepath.Glob(filepath.Join(root, "*", "core", "*", "l0", "l0-*"))
				inDir, _ := filepath.Glob(filepath.Join(cfg.L0Dir, "owner-*", "l0-*"))
				if moved && (len(inRoot) != 0 || len(inDir) == 0) || !moved && len(inRoot) == 0 {
					t.Fatalf("L0 files under root %v, under L0Dir %v (moved %v)", inRoot, inDir, moved)
				}
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
				if _, err := os.Stat(cfg.L0Dir); moved && !os.IsNotExist(err) {
					t.Fatalf("restart %d: clean close kept L0 dir: %v", restart, err)
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

// writeLegacySegment writes the journal segment an old journal build left: one
// V2A2 lean record whose header says cursor and orders 1..count. Its coded
// payload is never decoded.
func writeLegacySegment(t *testing.T, root string, cursor, count uint64) {
	t.Helper()
	record := []byte{'V', '2', 'A', 2}
	record = binary.AppendUvarint(record, cursor)
	record = binary.AppendUvarint(record, count)
	record = binary.AppendUvarint(record, 1)
	record = binary.AppendUvarint(record, 3)
	record = append(record, 1, 2, 3)
	record = binary.LittleEndian.AppendUint32(record, crc32.Checksum(record, applicationCRC))
	dir := filepath.Join(root, "placement-group-00000001", "journal", "producer-000")
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	segment := binary.LittleEndian.AppendUint32(nil, uint32(len(record)))
	if err := os.WriteFile(filepath.Join(dir, "segment.pdc4"), append(segment, record...), 0600); err != nil {
		t.Fatal(err)
	}
}

// TestL0RefusesUnsealedJournal: an L0 build never replays a journal tail. A
// journal whose newest record no seal covers refuses to open; a cleanly
// stopped journal build (every record sealed) upgrades in place.
func TestL0RefusesUnsealedJournal(t *testing.T) {
	root := t.TempDir()
	writeLegacySegment(t, root, 12, 6)
	if _, err := Open(root, l0Config()); !errors.Is(err, ErrLegacyJournal) {
		t.Fatalf("unsealed journal: %v, want ErrLegacyJournal", err)
	}
	// The old build's clean stop sealed through its cursor, which counted
	// points (12) and so ran above its last order (6).
	root = t.TempDir()
	s, err := Open(root, l0Config())
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Core.AppendBatches(0, nil, 12, true); err != nil {
		t.Fatal(err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	writeLegacySegment(t, root, 12, 6)
	s, err = Open(root, l0Config())
	if err != nil {
		t.Fatal(err)
	}
	if s.Producers[0].applyOrder != 12 {
		t.Fatalf("order after upgrade %d, want max(order 6, frontier 12)", s.Producers[0].applyOrder)
	}
	next := restartApps(t, 0, 6)
	for i := range next {
		next[i].Order += 12
	}
	if err = s.Producers[0].AppendApplications(next); err != nil {
		t.Fatal(err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestL0KillFences is the production port of the B3-L0 oracle's SIGKILL
// fences: rounds groups, each published then sealed, and a real SIGKILL at the
// k-th L0 write, L0 rename, seal and seal commit for every k, plus one kill of
// an empty store (4*rounds+1 kills). After each kill the store must hold
// exactly the classic image of an application prefix that contains every
// application a reader saw. V2_L0_KILL_ROUNDS sets rounds (default 3; the
// oracle used 100).
func TestL0KillFences(t *testing.T) { testL0KillFences(t, "TestL0KillFences", 1) }

// TestL0KillFencesMultiOwner runs the same kills with the two names on two
// owners. A cut owner blocks only itself, so at the kill each owner may hold
// a different prefix: each must hold every application a reader saw from it
// and exactly the classic image of its own prefix.
func TestL0KillFencesMultiOwner(t *testing.T) { testL0KillFences(t, "TestL0KillFencesMultiOwner", 2) }

func testL0KillFences(t *testing.T, name string, shards int) {
	config := func() Config { c := l0Config(); c.Shards = shards; return c }
	owner := func(app Application) int {
		return int(partsformat.Route(partsformat.ID(app.Name))) % shards
	}
	if shards > 1 && owner(restartApps(t, 0, 1)[0]) == owner(restartApps(t, 1, 2)[0]) {
		t.Fatal("both names route to one owner")
	}
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
			VisiblePending: func(o int, v v2core.PendingView) {
				var top uint64
				v.Each(func(a *v2core.PendingApplication) bool { top = max(top, a.Order); return true })
				fmt.Println("VIS", o, top)
			},
		}
		s, err := openWithHooks(root, config(), hooks)
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
			s, err := Open(root, config())
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
		cmd := exec.Command(os.Args[0], "-test.run=^"+name+"$", "-test.timeout=60s")
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
		// Order of the last application each owner showed a reader.
		visible := make([]uint64, shards)
		for _, app := range restartApps(t, 0, base) {
			visible[owner(app)] = max(visible[owner(app)], app.Order)
		}
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
					var o int
					var n uint64
					if _, err := fmt.Sscan(v, &o, &n); err == nil && o < shards {
						visible[o] = max(visible[o], n)
					}
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
		s, err := Open(root, config())
		if err != nil {
			t.Fatalf("%v: %v", c, err)
		}
		// Seal the loaded L0 rows, then each owner's last order per name
		// tells which prefix of its own applications it holds.
		if p := s.Producers[0]; p.cursor > 0 {
			if err = s.Core.AppendBatches(0, nil, p.cursor, true); err != nil {
				t.Fatal(err)
			}
		}
		held := make([]uint64, shards)
		for o, db := range s.Core.DBs() {
			snap := db.NewSnapshot()
			err := v2core.ScanRollups(snap, func(_ v2core.RollupPolicy, m v2core.RollupMetadata, _ []sealrollup.Record) error {
				held[o] = max(held[o], m.Frontier.Order)
				return nil
			})
			_ = snap.Close()
			if err != nil {
				t.Fatal(err)
			}
		}
		var prefix []Application
		for _, app := range restartApps(t, 0, base+2*rounds) {
			if app.Order <= held[owner(app)] {
				prefix = append(prefix, app)
			}
		}
		for o := range shards {
			if held[o] < visible[o] {
				t.Fatalf("%v: owner %d holds order %d, below visible %d", c, o, held[o], visible[o])
			}
			if held[o] > uint64(base+2*rounds) {
				t.Fatalf("%v: owner %d holds order %d beyond input", c, o, held[o])
			}
		}
		if len(prefix) > 0 {
			checkRestartOracle(t, s, prefix, 3)
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
