//go:build linux

package carbon

import (
	"context"
	"errors"
	"io"
	"math"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"reflect"
	"syscall"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/recovery"
	"github.com/go-graphite/go-carbon/points"
)

// Real kernel write errors must never acknowledge a failed frame or hide an
// earlier complete prefix. These fixtures use private files and process limits;
// they neither fill a filesystem nor change a host-wide resource limit.
func TestStartupCaptureKernelWriteFailures(t *testing.T) {
	for _, fault := range []string{"enospc", "partial"} {
		t.Run(fault, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestStartupCaptureKernelWriteFailureProcess$", "-test.timeout=30s", "-test.v")
			cmd.Env = append(os.Environ(), "GO_CARBON_CAPTURE_WRITE_FAULT="+fault)
			if out, err := cmd.CombinedOutput(); err != nil {
				t.Fatalf("%s: %v\n%s", fault, err, out)
			} else {
				t.Logf("%s: %s", fault, out)
			}
		})
	}
}

func TestStartupCaptureKernelWriteFailureProcess(t *testing.T) {
	fault := os.Getenv("GO_CARBON_CAPTURE_WRITE_FAULT")
	if fault == "" {
		t.Skip("subprocess fixture")
	}
	path := filepath.Join(t.TempDir(), "input.tail.bin")
	s, err := openStartupInputWithPolicy(path, chooseStartupPolicy(1, 256<<20, 1<<30))
	if err != nil {
		t.Fatal(err)
	}
	defer s.close()
	sh := s.shards[0]
	want := points.OnePoint("accepted.before.fault", math.Float64frombits(1<<63), 1700000000)
	if err := sh.Write(want.AppendBinary(nil), 1); err != nil {
		t.Fatal(err)
	}
	seg := sh.current.Load()
	if err := seg.file.Sync(); err != nil {
		t.Fatal(err)
	}
	prefix := seg.size.Load()
	var expected error
	switch fault {
	case "enospc":
		full, err := os.OpenFile("/dev/full", os.O_WRONLY, 0)
		if err != nil {
			t.Fatal(err)
		}
		defer full.Close()
		// The append owner is idle after its acknowledgement. Keep its os.File
		// object unchanged while replacing only this child's descriptor target.
		if err := syscall.Dup2(int(full.Fd()), int(seg.file.Fd())); err != nil {
			t.Fatal(err)
		}
		expected = syscall.ENOSPC
	case "partial":
		var limit syscall.Rlimit
		if err := syscall.Getrlimit(syscall.RLIMIT_FSIZE, &limit); err != nil {
			t.Fatal(err)
		}
		limit.Cur = uint64(prefix + 7) // less than a complete capture header
		signal.Ignore(syscall.SIGXFSZ)
		if err := syscall.Setrlimit(syscall.RLIMIT_FSIZE, &limit); err != nil {
			t.Fatal(err)
		}
		expected = syscall.EFBIG
	default:
		t.Fatal("unknown fixture", fault)
	}
	if err := sh.Write(points.OnePoint("failed.frame", 2, 1700000001).AppendBinary(nil), 1); !errors.Is(err, expected) {
		t.Fatalf("kernel failure: got %v, want %v", err, expected)
	}
	if s.points.Load() != 1 || seg.size.Load() != prefix || sh.pending.Load() != 0 {
		t.Fatal("failed frame published or remained pending", s.points.Load(), seg.size.Load(), sh.pending.Load())
	}
	// Poisoned capture must refuse subsequent batches promptly, rather than
	// appending behind an incomplete frame or returning a success receipt.
	done := make(chan error, 1)
	sh.SubmitCapture(points.OnePoint("after.failure", 3, 1700000002).AppendBinary(nil), 1, done)
	select {
	case err := <-done:
		if !errors.Is(err, expected) {
			t.Fatal("lost original failure", err)
		}
	case <-time.After(time.Second):
		t.Fatal("poisoned capture blocked a subsequent acknowledgement")
	}
	if err := s.close(); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(seg.path)
	if err != nil {
		t.Fatal(err)
	}
	physical := prefix
	if fault == "partial" {
		physical += 7
	}
	if info.Size() != physical {
		t.Fatal("fault did not reach the real file", info.Size(), physical)
	}
	var got []*points.Points
	n, err := recovery.ReadCaptureFile(seg.path, func(p *points.Points) { got = append(got, p) })
	if fault == "partial" {
		if !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Fatal("partial tail was silently accepted", err)
		}
	} else if err != nil {
		t.Fatal(err)
	}
	if n != 1 || !reflect.DeepEqual(got, []*points.Points{want}) || math.Float64bits(got[0].Data[0].Value) != 1<<63 {
		t.Fatal("accepted prefix lost or altered", n, got)
	}
	t.Logf("fault=%s accepted_points=%d published_bytes=%d physical_bytes=%d", fault, n, prefix, info.Size())
}
