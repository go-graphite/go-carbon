package inputlog

import (
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

func stageBatch(name string, bits uint64, ts int64) [partitions]*Batch {
	var b Batch
	b.Append(NewStreamRef(name), math.Float64frombits(bits), ts)
	var group [partitions]*Batch
	group[0] = &b
	return group
}

func TestStageReleasesOwnershipBeforeSyncAndPublishesOnlyDurable(t *testing.T) {
	l, err := Open(t.TempDir(), withTestDefaults(Options{SegmentSize: 16 << 20}))
	if err != nil {
		t.Fatal(err)
	}
	l.StartAppend()
	defer l.Close()
	var pending []*AppendCompletion
	bits := []uint64{0, 1 << 63, 1, 0x7ff8000000000042, 0x7ff0000000000001, 0x7ff0000000000000, 0xfff0000000000000}
	for i, bit := range bits {
		group := stageBatch("same", bit, int64(i/2))
		completion, err := l.StageGroup(group)
		if err != nil {
			t.Fatal(err)
		}
		// Borrow has ended even though nothing is durable yet.
		group[0].Reset()
		group[0].Append(NewStreamRef("overwritten"), 99, 99)
		pending = append(pending, completion)
	}
	state, err := l.AppendState()
	if err != nil || len(state.Files) != 0 {
		t.Fatal("unsynced tail advertised", state, err)
	}
	if l.Metrics().Syncs != 0 {
		t.Fatal("Stage performed per-call sync")
	}
	if err := l.Sync(); err != nil {
		t.Fatal(err)
	}
	var previous AppendReceipt
	for _, completion := range pending {
		receipt, err := completion.Wait()
		if err != nil || receipt.Salt == 0 || receipt.Tail.Offset < previous.Tail.Offset {
			t.Fatal(receipt, err)
		}
		previous = receipt
	}
	if got := l.Metrics().Syncs; got != 1 {
		t.Fatalf("syncs=%d want 1", got)
	}
	dir := l.dir
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	recovered, err := Open(dir, withTestDefaults(Options{}))
	if err != nil {
		t.Fatal(err)
	}
	defer recovered.Close()
	got, _ := replayAll(t, recovered)
	if len(got) != 1 || len(got["same"]) != len(bits) {
		t.Fatal(got)
	}
	for i, p := range got["same"] {
		if math.Float64bits(p.Value) != bits[i] || p.Timestamp != int64(i/2) {
			t.Fatal("bits/order", i, p)
		}
	}
}

func TestStageFaultsNeverPublishFalseReceipts(t *testing.T) {
	for _, phase := range []string{"before-encode", "after-encode", "before-write", "after-write", "before-sync", "after-sync", "before-publish", "after-publish"} {
		t.Run(phase, func(t *testing.T) {
			dir := t.TempDir()
			l, err := Open(dir, withTestDefaults(Options{SegmentSize: 16 << 20}))
			if err != nil {
				t.Fatal(err)
			}
			stable := t.TempDir()
			var armed atomic.Bool
			injected := errors.New("injected " + phase)
			l.testAppendHook = func(at string) error {
				if armed.Load() && at == "after-sync" {
					for _, seg := range l.live {
						data, e := os.ReadFile(seg.path)
						if e != nil {
							return e
						}
						if e = os.WriteFile(filepath.Join(stable, filepath.Base(seg.path)), data[:seg.end], 0600); e != nil {
							return e
						}
					}
				}
				if armed.Load() && at == phase {
					return injected
				}
				return nil
			}
			l.StartAppend()
			if _, err := l.AppendGroup(stageBatch("durable", 0x8000000000000000, 1)); err != nil {
				t.Fatal(err)
			}
			// This is the stable storage image at the last successful sync.
			state, err := l.AppendState()
			if err != nil {
				t.Fatal(err)
			}
			for _, f := range state.Files {
				bytes, err := os.ReadFile(filepath.Join(dir, f.Name))
				if err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(stable, f.Name), bytes[:f.Size], 0600); err != nil {
					t.Fatal(err)
				}
			}
			armed.Store(true)
			completion, stageErr := l.StageGroup(stageBatch("unsynced", 1, 2))
			syncErr := l.Sync()
			if !errors.Is(syncErr, injected) {
				t.Fatal("sync did not propagate sticky error", syncErr)
			}
			if stageErr == nil {
				receipt, err := completion.Wait()
				if phase == "after-publish" {
					if err != nil || receipt.Salt == 0 {
						t.Fatal("durable receipt lost after publication", receipt, err)
					}
				} else if !errors.Is(err, injected) || receipt != (AppendReceipt{}) {
					t.Fatal("false receipt", receipt, err)
				}
			} else if !errors.Is(stageErr, injected) {
				t.Fatal(stageErr)
			}
			if _, err := l.StageGroup(stageBatch("later", 2, 3)); !errors.Is(err, injected) {
				t.Fatal("later writer missed sticky error", err)
			}
			l.Kill()
			// Power loss before sync retains exactly the stable image. A
			// process crash may preserve additional complete page-cache frames.
			recovered, err := Open(stable, withTestDefaults(Options{}))
			if err != nil {
				t.Fatal(err)
			}
			defer recovered.Close()
			got, _ := replayAll(t, recovered)
			wantNames := 1
			if phase == "after-sync" || phase == "before-publish" || phase == "after-publish" {
				wantNames = 2
			}
			if len(got) != wantNames || len(got["durable"]) != 1 || math.Float64bits(got["durable"][0].Value) != 1<<63 {
				t.Fatal(got)
			}
		})
	}
}

func TestStageTimerAndCloseForceSync(t *testing.T) {
	for _, closeNow := range []bool{false, true} {
		t.Run(fmt.Sprint(closeNow), func(t *testing.T) {
			l, err := Open(t.TempDir(), withTestDefaults(Options{SegmentSize: 16 << 20}))
			if err != nil {
				t.Fatal(err)
			}
			l.StartAppend()
			defer l.Close()
			c, err := l.StageGroup(stageBatch("timer", 1, 1))
			if err != nil {
				t.Fatal(err)
			}
			if closeNow {
				if err := l.Close(); err != nil {
					t.Fatal(err)
				}
			}
			done := make(chan error, 1)
			go func() { _, err := c.Wait(); done <- err }()
			select {
			case err := <-done:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("durability completion stalled")
			}
		})
	}
}

func TestStageRecoveredFrontierRequiresNewSync(t *testing.T) {
	dir := t.TempDir()
	l, err := Open(dir, withTestDefaults(Options{SegmentSize: 16 << 20}))
	if err != nil {
		t.Fatal(err)
	}
	l.StartAppend()
	if _, err := l.StageGroup(stageBatch("recovered", 1, 2)); err != nil {
		t.Fatal(err)
	}
	// Serialize behind the positioned write without forcing durability.
	if _, err := l.AppendState(); err != nil {
		t.Fatal(err)
	}
	l.Kill()
	l, err = Open(dir, withTestDefaults(Options{}))
	if err != nil {
		t.Fatal(err)
	}
	l.StartAppend()
	defer l.Close()
	state, err := l.AppendState()
	if err != nil || len(state.Files) != 0 {
		t.Fatal("recovered page-cache tail advertised before sync", state, err)
	}
	if err := l.Sync(); err != nil {
		t.Fatal(err)
	}
	state, err = l.AppendState()
	if err != nil || len(state.Files) != 1 || state.Files[0].Size <= segmentHeaderSize || state.Files[0].Size >= 16<<20 {
		t.Fatal("expected validated, newly synced prefix", state, err)
	}
}
