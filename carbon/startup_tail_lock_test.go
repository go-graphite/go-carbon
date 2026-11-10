//go:build unix

package carbon

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/handoff"
	"github.com/go-graphite/go-carbon/internal/recovery"
	"github.com/go-graphite/go-carbon/points"
	"go.uber.org/zap"
)

func TestStartupCapturePreparesBeforeLegacyTailSeal(t *testing.T) {
	path := filepath.Join(t.TempDir(), "input.1.2.tail.bin")
	writer, err := recovery.NewWriter(path, 1, recovery.InputFileBuffer, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	unlock, err := handoff.LockExclusive(path)
	if err != nil {
		t.Fatal(err)
	}
	release := sync.OnceFunc(func() {
		if err := unlock(); err != nil {
			t.Error(err)
		}
	})
	defer release()
	s, err := openStartupInputWithLegacyLock(path, startupPolicy(path), false)
	if err != nil {
		t.Fatal(err)
	}
	defer s.close()
	done := make(chan error, 1)
	go func() { done <- s.lockLegacy() }()
	joined := false
	defer func() {
		release()
		if !joined {
			<-done
		}
	}()
	select {
	case err := <-done:
		joined = true
		t.Fatalf("unsealed tail admitted for replay: %v", err)
	case <-time.After(25 * time.Millisecond):
	}
	live := points.OnePoint("live", 2.25, 1700000001)
	if err := s.shards[0].Write(live.AppendBinary(nil), 1); err != nil {
		t.Fatal(err)
	}
	if s.points.Load() != 1 {
		t.Fatal("capture waited for legacy tail seal")
	}
	// These bytes did not exist when the successor prepared its capture epoch.
	old := points.OnePoint("older", 1.125, 1700000000)
	if err := writer.WritePoints(old); err != nil {
		t.Fatal(err)
	}
	descriptor, err := writer.Close()
	if err != nil {
		t.Fatal(err)
	}
	release()
	select {
	case err := <-done:
		joined = true
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("legacy lock did not follow completed seal")
	}
	if s.legacyLockPending || s.legacy.current.Load().size.Load() != descriptor.Size || s.legacy.size.Load() != descriptor.Size {
		t.Fatal("prepared capture lost the predecessor's final extent")
	}
	seen := 0
	if err := points.ReadFromFile(path, func(p *points.Points) {
		seen++
		if p.Metric != old.Metric || len(p.Data) != 1 || p.Data[0] != old.Data[0] {
			t.Error("legacy prefix changed")
		}
	}); err != nil {
		t.Fatal(err)
	}
	if seen != 1 {
		t.Fatal("legacy prefix missing", seen)
	}
}

const tailSealCrashRoot = "GO_CARBON_TEST_TAIL_SEAL_CRASH_ROOT"

func TestAsyncTailSealCrashProcess(t *testing.T) {
	root := os.Getenv(tailSealCrashRoot)
	if root == "" {
		return
	}
	path := filepath.Join(root, "input.1.2.tail.bin")
	writer, err := recovery.NewWriter(path, 1, recovery.InputFileBuffer, nil)
	if err != nil {
		t.Fatal(err)
	}
	unlock, err := handoff.LockExclusive(path)
	if err != nil {
		t.Fatal(err)
	}
	h := &inputHandover{tail: writer, path: path, unlock: unlock, logger: zap.NewNop(), timer: time.NewTimer(time.Hour), asyncSeal: true}
	for i := range 128 {
		if err := h.write(points.OnePoint("older", float64(i)+.125, 1700000000+int64(i))); err != nil {
			t.Fatal(err)
		}
	}
	handoverSealHook = func() {
		if err := os.WriteFile(filepath.Join(root, "seal-entered"), []byte("flushed before transfer\n"), 0600); err != nil {
			panic(err)
		}
		select {} // parent kills only this private process at the durability fence
	}
	if _, err := h.Detach(); err != nil {
		t.Fatal(err)
	}
	if err := h.waitSeal(); err != nil {
		t.Fatal(err)
	}
}

func TestAsyncTailSealProcessCrashPreservesAcceptedPrefix(t *testing.T) {
	root := t.TempDir()
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	child := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestAsyncTailSealCrashProcess$")
	child.Env = append(os.Environ(), tailSealCrashRoot+"="+root)
	if err := child.Start(); err != nil {
		t.Fatal(err)
	}
	joined := false
	defer func() {
		if !joined {
			_ = child.Process.Kill()
			_ = child.Wait()
		}
	}()
	awaitCapture(t, func() bool { _, err := os.Stat(filepath.Join(root, "seal-entered")); return err == nil })
	path := filepath.Join(root, "input.1.2.tail.bin")
	s, err := openStartupInputWithLegacyLock(path, startupPolicy(path), false)
	if err != nil {
		t.Fatal(err)
	}
	defer s.close()
	newer := points.OnePoint("newer", 2.25, 1700000128)
	if err := s.shards[0].Write(newer.AppendBinary(nil), 1); err != nil {
		t.Fatal(err)
	}
	if err := child.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	if err := child.Wait(); err == nil {
		t.Fatal("predecessor was not killed at the seal fence")
	}
	joined = true
	if err := s.lockLegacy(); err != nil {
		t.Fatal(err)
	}
	if err := s.close(); err != nil {
		t.Fatal(err)
	}
	seen := 0
	if err := points.ReadFromFile(path, func(p *points.Points) {
		for _, point := range p.Data {
			if p.Metric != "older" || point.Timestamp != 1700000000+int64(seen) || point.Value != float64(seen)+.125 {
				t.Error("process crash changed accepted prefix", p)
			}
			seen++
		}
	}); err != nil || seen != 128 {
		t.Fatal("process crash lost accepted prefix", seen, err)
	}
	seen = 0
	paths, err := filepath.Glob(s.shards[0].path + ".*.tail.bin")
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range paths {
		if _, err := recovery.ReadCaptureFile(path, func(p *points.Points) {
			if p.Metric != newer.Metric || len(p.Data) != 1 || p.Data[0] != newer.Data[0] {
				t.Error("predecessor crash changed successor capture", p)
			}
			seen++
		}); err != nil {
			t.Fatal(err)
		}
	}
	if seen != 1 {
		t.Fatal("predecessor crash lost successor capture", seen)
	}
}

func TestStartupLegacyLockWaitCanBeCancelled(t *testing.T) {
	path := filepath.Join(t.TempDir(), "input.1.2.tail.bin")
	if err := os.WriteFile(path, nil, 0600); err != nil {
		t.Fatal(err)
	}
	unlock, err := handoff.LockExclusive(path)
	if err != nil {
		t.Fatal(err)
	}
	defer unlock()
	s, err := openStartupInputWithLegacyLock(path, startupPolicy(path), false)
	if err != nil {
		t.Fatal(err)
	}
	defer s.close()
	stop := make(chan struct{})
	done := make(chan error, 1)
	go func() { _, err := handoff.LockExclusiveUntil(path, stop); done <- err }()
	close(stop)
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("replay cancellation waited for predecessor IO")
	}
	// Closing prepared capture does not synchronize or lock an incumbent's tail.
	closed := make(chan error, 1)
	go func() { closed <- s.close() }()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("capture close waited for predecessor IO")
	}
}
