package v2daemon

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/partsformat"
	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2core"
	"github.com/go-graphite/go-carbon/points"
	whisper "github.com/go-graphite/go-whisper"
)

func restartConfig(layer int) Config {
	return Config{Layer: layer, Producers: 1, Shards: 2, GroupInterval: time.Hour, KeepBaseRows: layer == 3}
}
func restartApps(t *testing.T, from, to int) []Application {
	t.Helper()
	var out []Application
	for j := from; j < to; j++ {
		name := fmt.Sprintf("replay.%d", j%2)
		policy, err := v2core.EncodeRollupPolicy(v2core.RollupPolicy{Name: partsformat.ID(name), Group: "test", Config: sealrollup.Config{Method: sealrollup.Sum, XFilesFactor: .5, Archives: []sealrollup.Archive{{Step: 1, Count: 120}, {Step: 10, Count: 120}}}})
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, Application{Name: name, Policy: policy, Order: uint64(j + 1), Clock: 10020 + int64(j/4), Points: []points.Point{{Timestamp: 10000 + int64(j), Value: float64(j) / 4}, {Timestamp: 10000 + int64(j), Value: float64(j+1) / 4}}, Sequence: []uint64{uint64(j*3 + 1), uint64(j*3 + 2)}})
	}
	return out
}

// TestRestartSIGKILL ports replay12's parent-held receipt + child barrier + real
// SIGKILL pattern, with an exhaustive local oracle instead of digest sampling.
func TestRestartSIGKILL(t *testing.T) {
	if root := os.Getenv("V2_REPLAY_CHILD_ROOT"); root != "" {
		stage := os.Getenv("V2_REPLAY_CHILD_STAGE")
		layer, _ := strconv.Atoi(os.Getenv("V2_REPLAY_CHILD_LAYER"))
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
		s, err := openWithHooks(root, restartConfig(layer), v2core.Hooks{Stage: cut})
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
		// Drained rows are durable once a publication wrote them to L0 run
		// files (one per owner holding them); only then cut the seal.
		owners := map[int]bool{}
		for _, app := range restartApps(t, 6, 12) {
			owners[int(partsformat.Route(partsformat.ID(app.Name)))%s.cfg.Shards] = true
		}
		for deadline := time.Now().Add(10 * time.Second); ; time.Sleep(10 * time.Millisecond) {
			files, _ := filepath.Glob(filepath.Join(root, "placement-group-00000001", "core", "owner-*", "l0", "l0-*[0-9]"))
			if len(files) >= len(owners) {
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("no L0 run files")
			}
		}
		if err = s.Core.AppendBatches(0, nil, s.Producers[0].cursor, true); err != nil {
			t.Fatal(err)
		}
		t.Fatal("missed crash stage", stage)
	}
	for _, layer := range []int{3} {
		for _, stage := range []string{"seal", "sort", "part-write", "ingest", "commit", "rollup"} {
			t.Run(fmt.Sprintf("L%d/%s", layer, stage), func(t *testing.T) {
				root := t.TempDir()
				cfg := restartConfig(layer)
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
				cmd := exec.Command(os.Args[0], "-test.run=^TestRestartSIGKILL$", "-test.timeout=30s")
				cmd.Env = append(os.Environ(), "V2_REPLAY_CHILD_ROOT="+root, "V2_REPLAY_CHILD_STAGE="+stage, fmt.Sprintf("V2_REPLAY_CHILD_LAYER=%d", layer))
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
				ok := errors.As(err, &exit)
				if !ok || !exit.Sys().(syscall.WaitStatus).Signaled() || exit.Sys().(syscall.WaitStatus).Signal() != syscall.SIGKILL {
					t.Fatalf("not SIGKILL: %v", err)
				}
				count := 12
				if stage == "append" {
					count = 6
				}
				// Reopen twice: first rebuilds the uncommitted suffix, second resubmits
				// committed packets and must produce exactly the same rows and metadata.
				for replay := 0; replay < 2; replay++ {
					start := time.Now()
					s, err = Open(root, cfg)
					elapsed := time.Since(start)
					if err != nil {
						t.Fatal(err)
					}
					checkRestartOracle(t, s, restartApps(t, 0, count), layer)
					t.Logf("restart=%d first_accept_ms=%.3f exact_points=%d", replay, float64(elapsed)/float64(time.Millisecond), count*2)
					if err = s.Close(); err != nil {
						t.Fatal(err)
					}
				}
			})
		}
	}
}

type synchronizedBuffer struct {
	mu sync.Mutex
	b  []byte
}

func (b *synchronizedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.b = append(b.b, p...)
	return len(p), nil
}
func (b *synchronizedBuffer) String() string { b.mu.Lock(); defer b.mu.Unlock(); return string(b.b) }

func checkRestartOracle(t *testing.T, s *Store, apps []Application, layer int) {
	t.Helper()
	if s.Producers[0].cursor > 0 {
		if err := s.Core.AppendBatches(0, nil, s.Producers[0].cursor, true); err != nil {
			t.Fatal(err)
		}
	}
	want := map[uint64][]v2core.Row{}
	lastOrder := map[uint64]uint64{}
	revisions := map[uint64]uint64{}
	clocks := map[uint64]int64{}
	paths := map[uint64]string{}
	for id := uint64(1); id <= 2; id++ {
		path := filepath.Join(t.TempDir(), "classic.wsp")
		paths[id] = path
		clock := int64(10020)
		w, err := whisper.CreateWithOptions(path, whisper.MustParseRetentionDefs("1s:120s,10s:1200s"), whisper.Sum, .5, &whisper.Options{Sparse: true, Now: func() time.Time { return time.Unix(clock, 0) }})
		if err != nil {
			t.Fatal(err)
		}
		for _, app := range apps {
			appID := uint64(1)
			if app.Name == "replay.1" {
				appID = 2
			}
			if appID != id {
				continue
			}
			clock = app.Clock
			var ps []*whisper.TimeSeriesPoint
			for i, p := range app.Points {
				want[id] = append(want[id], v2core.Row{ID: id, Time: p.Timestamp, Bits: math.Float64bits(p.Value), Seq: app.Sequence[i]})
				ps = append(ps, &whisper.TimeSeriesPoint{Time: int(p.Timestamp), Value: p.Value})
			}
			if err = w.UpdateMany(ps); err != nil {
				t.Fatal(err)
			}
			lastOrder[id] = app.Order
			revisions[id]++
			clocks[id] = max(clocks[id], app.Clock)
		}
		if err = w.Close(); err != nil {
			t.Fatal(err)
		}
	}
	for id, rows := range want {
		if layer == 3 && !s.cfg.KeepBaseRows {
			break // archive records only, as in production
		}
		owner := v2core.Shard(id, s.cfg.Shards)
		if layer == 3 {
			owner = int(partsformat.Route(partsformat.ID(fmt.Sprintf("replay.%d", id-1)))) % s.cfg.Shards
		}
		snap := s.Core.DBs()[owner].NewSnapshot()
		got, err := v2core.Read(snap, id)
		_ = snap.Close()
		if err != nil || !slices.Equal(got, rows) {
			t.Fatalf("raw id%d: got%+v want%+v err%v", id, got, rows, err)
		}
	}
	if layer != 3 {
		return
	}
	seen := 0
	for _, db := range s.Core.DBs() {
		snap := db.NewSnapshot()
		err := v2core.ScanRollups(snap, func(p v2core.RollupPolicy, m v2core.RollupMetadata, records []sealrollup.Record) error {
			seen++
			id := m.ID
			if m.Frontier.Order != lastOrder[id] || m.Revision != revisions[id]+1 || m.LastUpdate != clocks[id]*1e9 {
				t.Fatalf("metadata id%d: %+v", id, m)
			}
			data, err := os.ReadFile(paths[id])
			if err != nil {
				return err
			}
			type slot struct {
				archive  int
				physical int64
			}
			stored := map[slot]sealrollup.Record{}
			for _, r := range records {
				k := slot{r.Archive, r.Slot}
				if _, ok := stored[k]; ok {
					t.Fatal("duplicate rollup slot")
				}
				stored[k] = r
			}
			n := 0
			for ai, a := range p.Config.Archives {
				offset := int(binary.BigEndian.Uint32(data[16+ai*12:]))
				raw := data[offset : offset+int(a.Count)*12]
				for i := 0; i < len(raw); i += 12 {
					stamp := int64(binary.BigEndian.Uint32(raw[i:]))
					if stamp == 0 {
						continue
					}
					n++
					r, ok := stored[slot{ai, (stamp / a.Step) % a.Count}]
					if !ok || r.Sample.Time != stamp || r.Sample.Bits != binary.BigEndian.Uint64(raw[i+4:]) {
						t.Fatalf("classic mismatch archive%d stamp%d got%+v", ai, stamp, r)
					}
				}
			}
			if n != len(stored) {
				t.Fatalf("extra rollup slots got%d want%d", len(stored), n)
			}
			return nil
		})
		_ = snap.Close()
		if err != nil {
			t.Fatal(err)
		}
	}
	if seen != len(want) {
		t.Fatalf("metric count %d want%d", seen, len(want))
	}
}

func TestRestartExclusionGeometryAndAppend(t *testing.T) {
	root := t.TempDir()
	cfg := restartConfig(3)
	s, err := Open(root, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if other, err := Open(root, cfg); err == nil {
		_ = other.Close()
		t.Fatal("concurrent open allowed")
	}
	if err = s.Producers[0].AppendApplications(restartApps(t, 0, 6)); err != nil {
		t.Fatal(err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	wrong := cfg
	wrong.Producers++
	if other, err := Open(root, wrong); err == nil {
		_ = other.Close()
		t.Fatal("changed geometry accepted")
	}
	s, err = Open(root, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Producers[0].AppendApplications(restartApps(t, 6, 12)); err != nil {
		t.Fatal(err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(root, cfg)
	if err != nil {
		t.Fatal(err)
	}
	checkRestartOracle(t, s, restartApps(t, 0, 12), 3)
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
}
