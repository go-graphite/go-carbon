package partlog

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestAdmissionBeforeHistoricalValidation(t *testing.T) {
	dir := t.TempDir()
	opts := Options{Streams: 4, SegmentBytes: 96}
	l, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	bits := []uint64{0x7ff8000000000042, 0x8000000000000000, 1, 0xfff0000000000000}
	for p := range 4 {
		if _, err = l.Append(p, -10, binary.LittleEndian.AppendUint64(nil, bits[p])); err != nil {
			t.Fatal(err)
		}
	}
	identity := l.Identity()
	if err = l.Close(); err != nil {
		t.Fatal(err)
	}
	blocked, release := make(chan struct{}, 4), make(chan struct{})
	var unblock sync.Once
	defer unblock.Do(func() { close(release) })
	opts.Hook = func(_ int, phase string) {
		if phase == "recover-scan" {
			blocked <- struct{}{}
			<-release
		}
	}
	l, err = OpenAdmission(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	for range 4 {
		<-blocked
	}
	for p := range 4 {
		cursor, e := l.Append(p, 20, []byte("live"))
		if e != nil || cursor != epochStride+1 {
			t.Fatal(cursor, e)
		}
	}
	if err = l.Sync(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if e := l.Follow(ctx, 0, 0, func(Record) error { return nil }, nil); !errors.Is(e, context.Canceled) {
		t.Fatal("canceled historical wait", e)
	}
	select {
	case <-l.recovered:
		t.Fatal("historical validation bypassed")
	default:
	}
	unblock.Do(func() { close(release) })
	if err = l.WaitReady(context.Background()); err != nil {
		t.Fatal(err)
	}
	for p := range 4 {
		var got []Record
		err = l.Replay(p, 0, func(r Record) error {
			r.Payload = slices.Clone(r.Payload)
			got = append(got, r)
			return nil
		})
		if err != nil || len(got) != 2 || got[0].Cursor != 1 || got[0].Clock != -10 || binary.LittleEndian.Uint64(got[0].Payload) != bits[p] || got[1].Clock != 20 || string(got[1].Payload) != "live" {
			t.Fatal(got, err)
		}
	}
	if err = l.Truncate([][]uint64{{epochStride + 1, epochStride + 1, epochStride + 1, epochStride + 1}}); err != nil {
		t.Fatal(err)
	}
	if err = l.Close(); err != nil {
		t.Fatal(err)
	}
	// Synchronous Open must recognize the same fences and retired boundary.
	opts.Hook = nil
	l, err = Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	if l.Identity() != identity {
		t.Fatal("identity changed")
	}
	if err = l.Replay(0, 0, func(Record) error { return nil }); err == nil {
		t.Fatal("retired historical prefix accepted")
	}
	var count int
	if err = l.Replay(0, 1, func(Record) error { count++; return nil }); err != nil || count != 1 {
		t.Fatal(count, err)
	}
}

func TestAdmissionRecoveryFailureStopsAppends(t *testing.T) {
	dir := t.TempDir()
	l, err := Open(dir, Options{Streams: 1})
	if err != nil {
		t.Fatal(err)
	}
	_, _ = l.Append(0, 1, []byte("original"))
	if err = l.Close(); err != nil {
		t.Fatal(err)
	}
	files, _ := filepath.Glob(filepath.Join(dir, "*.plog"))
	f, err := os.OpenFile(files[0], os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	_, err = f.WriteAt([]byte{0xff}, headerSize+headerSize)
	if err = errors.Join(err, f.Close()); err != nil {
		t.Fatal(err)
	}
	release := make(chan struct{})
	var synced atomic.Bool
	l, err = OpenAdmission(dir, Options{Streams: 1, Hook: func(_ int, phase string) {
		if phase == "recover-scan" {
			<-release
		}
		if phase == "after-sync" {
			synced.Store(true)
		}
	}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = l.Append(0, 20, []byte("accepted before failure")); err != nil {
		close(release)
		t.Fatal(err)
	}
	close(release)
	if err = l.WaitReady(context.Background()); err == nil {
		t.Fatal("corrupt historical data accepted")
	}
	if !synced.Load() {
		t.Fatal("historical failure prevented sync of the accepted live suffix")
	}
	if _, err = l.Append(0, 1, []byte("must fail")); err == nil {
		t.Fatal("admission continued after recovery failure")
	}
	select {
	case <-l.Errors():
	default:
		t.Fatal("failure not surfaced")
	}
	if err = l.Close(); err == nil {
		t.Fatal("close hid historical failure")
	}
}

func TestAdmissionRepeatedEpochsAndTornTails(t *testing.T) {
	dir := t.TempDir()
	var want []string
	for epoch := uint64(1); epoch <= 4; epoch++ {
		l, err := OpenAdmission(dir, Options{Streams: 1, SegmentBytes: 100})
		if err != nil {
			t.Fatal(err)
		}
		if epoch%2 == 0 {
			if err = l.WaitReady(context.Background()); err != nil {
				t.Fatal(err)
			}
		}
		payload := fmt.Sprintf("epoch-%d", epoch)
		cursor, err := l.Append(0, -int64(epoch), []byte(payload))
		if err != nil || cursor != epoch*epochStride+1 {
			t.Fatal(cursor, err)
		}
		want = append(want, payload)
		if err = l.Close(); err != nil {
			t.Fatal(err)
		}
		files, _ := filepath.Glob(filepath.Join(dir, "*.plog"))
		f, err := os.OpenFile(files[len(files)-1], os.O_APPEND|os.O_WRONLY, 0600)
		if err != nil {
			t.Fatal(err)
		}
		_, err = f.Write([]byte("torn"))
		if err = errors.Join(err, f.Close()); err != nil {
			t.Fatal(err)
		}
	}
	l, err := Open(dir, Options{Streams: 1})
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	err = l.Replay(0, 0, func(r Record) error { got = append(got, string(r.Payload)); return nil })
	if err != nil || !slices.Equal(want, got) {
		t.Fatal(want, got, err)
	}
	if err = l.Close(); err != nil {
		t.Fatal(err)
	}
	if err = os.Remove(filepath.Join(dir, "epoch-00000000000000000002.fence")); err != nil {
		t.Fatal(err)
	}
	if l, err = OpenAdmission(dir, Options{Streams: 1}); err == nil {
		_ = l.Close()
		t.Fatal("missing epoch fence accepted")
	}
}

func TestAdmissionEmptyEpochAndIdleStreams(t *testing.T) {
	dir := t.TempDir()
	for range 3 {
		l, err := OpenAdmission(dir, Options{Streams: 2})
		if err != nil {
			t.Fatal(err)
		}
		if err = l.Close(); err != nil {
			t.Fatal(err)
		}
	}
	l, err := Open(dir, Options{Streams: 2})
	if err != nil {
		t.Fatal(err)
	}
	if cursor, e := l.Append(0, -1, []byte("first")); e != nil || cursor != 3*epochStride+1 {
		t.Fatal(cursor, e)
	}
	if err = l.Close(); err != nil {
		t.Fatal(err)
	}
	l, err = OpenAdmission(dir, Options{Streams: 2})
	if err != nil {
		t.Fatal(err)
	}
	if err = l.WaitReady(context.Background()); err != nil {
		t.Fatal(err)
	}
	if cursor, e := l.Append(1, -2, []byte("second")); e != nil || cursor != 4*epochStride+1 {
		t.Fatal(cursor, e)
	}
	if err = l.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestAdmissionMissingLatestFenceRejectedBeforeReuse(t *testing.T) {
	dir := t.TempDir()
	l, err := OpenAdmission(dir, Options{Streams: 1})
	if err != nil {
		t.Fatal(err)
	}
	_, err = l.Append(0, 1, []byte("must retain"))
	if err = errors.Join(err, l.Close()); err != nil {
		t.Fatal(err)
	}
	fence := filepath.Join(dir, "epoch-00000000000000000001.fence")
	if err = os.Remove(fence); err != nil {
		t.Fatal(err)
	}
	if l, err = OpenAdmission(dir, Options{Streams: 1}); err == nil {
		_ = l.Close()
		t.Fatal("missing latest fence reused an existing cursor range")
	}
	if _, err = os.Stat(fence); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("invalid epoch fence republished", err)
	}
}

// Kill after a newly synced live append while old-tail validation is blocked.
// On the next start both epochs must survive in their original order.
func TestAdmissionSIGKILLDuringRecovery(t *testing.T) {
	if dir := os.Getenv("PARTLOG_ADMISSION_CRASH_ROOT"); dir != "" {
		l, err := OpenAdmission(dir, Options{Streams: 1, Hook: func(_ int, phase string) {
			if phase == "recover-scan" {
				select {} // Historical I/O is deliberately unavailable.
			}
		}})
		if err == nil {
			_, err = l.Append(0, 20, []byte("new-live"))
		}
		if err == nil {
			err = l.Sync()
		}
		if err != nil {
			panic(err)
		}
		if err = os.WriteFile(filepath.Join(dir, "accepted"), []byte("synced"), 0600); err != nil {
			panic(err)
		}
		select {}
	}
	for trial := range 4 {
		t.Run(fmt.Sprint(trial), func(t *testing.T) {
			dir := t.TempDir()
			l, err := Open(dir, Options{Streams: 1})
			if err != nil {
				t.Fatal(err)
			}
			_, err = l.Append(0, 10, bytes.Repeat([]byte("old"), 100))
			if err = errors.Join(err, l.Close()); err != nil {
				t.Fatal(err)
			}
			cmd := exec.Command(os.Args[0], "-test.run=^TestAdmissionSIGKILLDuringRecovery$")
			cmd.Env = append(os.Environ(), "PARTLOG_ADMISSION_CRASH_ROOT="+dir)
			if err = cmd.Start(); err != nil {
				t.Fatal(err)
			}
			defer func() { _ = cmd.Process.Kill(); _ = cmd.Wait() }()
			deadline := time.Now().Add(10 * time.Second)
			for {
				if _, err = os.Stat(filepath.Join(dir, "accepted")); err == nil {
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("child failed to accept while history was blocked")
				}
				time.Sleep(time.Millisecond)
			}
			if err = cmd.Process.Kill(); err != nil {
				t.Fatal(err)
			}
			_ = cmd.Wait()
			l, err = OpenAdmission(dir, Options{Streams: 1})
			if err != nil {
				t.Fatal(err)
			}
			var got []string
			err = l.Replay(0, 0, func(r Record) error { got = append(got, string(r.Payload)); return nil })
			if err != nil || !slices.Equal(got, []string{string(bytes.Repeat([]byte("old"), 100)), "new-live"}) {
				t.Fatal(got, err)
			}
			if err = l.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
