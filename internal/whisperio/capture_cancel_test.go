package whisperio

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	whisper "github.com/go-graphite/go-whisper"
)

func TestCaptureLockWaitCancellation(t *testing.T) {
	for _, kind := range []string{"path", "inode"} {
		t.Run(kind, func(t *testing.T) {
			root := t.TempDir()
			source := filepath.Join(root, "metric.wsp")
			w, err := whisper.Create(source, whisper.MustParseRetentionDefs("1s:1h"), whisper.Average, 0)
			if err != nil {
				t.Fatal(err)
			}
			if err = w.Close(); err != nil {
				t.Fatal(err)
			}
			lockPath := source
			if kind == "path" {
				lockPath = whisper.PathLockPath(source)
			}
			f, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0600)
			if err != nil {
				t.Fatal(err)
			}
			defer f.Close()
			if err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != nil {
				t.Fatal(err)
			}
			defer syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
			destination := filepath.Join(root, "private", "metric.wsp")
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- Capture(ctx, source, destination, false) }()
			select {
			case err = <-done:
				if !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("capture cancellation: %v", err)
				}
			case <-time.After(time.Second):
				syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
				<-done
				t.Fatal("capture ignored cancellation while source lock remained held")
			}
			if _, err = os.Stat(destination); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("cancelled capture published destination: %v", err)
			}
		})
	}
}
