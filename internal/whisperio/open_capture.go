package whisperio

import (
	"context"
	"errors"
	"os"
	"syscall"
	"time"

	whisper "github.com/go-graphite/go-whisper"
)

// openCapture observes cancellation while waiting for stable-path/inode locks.
// Blocking flock cannot be interrupted by a Go context.
func openCapture(ctx context.Context, source string) (*whisper.Whisper, error) {
	readOnly := os.O_RDONLY
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		w, err := whisper.OpenWithOptions(source, &whisper.Options{FLock: true, FlockType: syscall.LOCK_SH | syscall.LOCK_NB, OpenFileFlag: &readOnly})
		if err == nil {
			return w, nil
		}
		if !errors.Is(err, syscall.EWOULDBLOCK) && !errors.Is(err, syscall.EAGAIN) {
			return nil, err
		}
		timer := time.NewTimer(10 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
}
