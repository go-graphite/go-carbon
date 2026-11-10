//go:build unix

package handoff

import (
	"context"
	"errors"
	"os"
	"syscall"
	"time"
)

// LockExclusive holds an exclusive advisory lock on path until release. A
// stopping instance holds it on the input log it still appends to, so a
// successor neither replays nor removes the log early.
func LockExclusive(path string) (release func() error, err error) {
	return lock(path, syscall.LOCK_EX)
}

// LockExclusiveUntil waits for a predecessor's immutable tail without making
// cancellation of the successor's historical replay depend on predecessor IO.
func LockExclusiveUntil(path string, stop <-chan struct{}) (func() error, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	for {
		select {
		case <-stop:
			_ = f.Close()
			return nil, context.Canceled
		default:
		}
		err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
		if err == nil {
			return f.Close, nil
		}
		if !errors.Is(err, syscall.EWOULDBLOCK) && !errors.Is(err, syscall.EINTR) {
			_ = f.Close()
			return nil, err
		}
		timer := time.NewTimer(10 * time.Millisecond)
		select {
		case <-stop:
			timer.Stop()
			_ = f.Close()
			return nil, context.Canceled
		case <-timer.C:
		}
	}
}

// LockShared waits until no process holds an exclusive lock on path, then holds
// a shared lock until release. The lock ends with the holder's process, so the
// wait ends when a stopping instance exits even if it never released it.
func LockShared(path string) (release func() error, err error) {
	return lock(path, syscall.LOCK_SH)
}

// Locked reports whether another process holds an exclusive lock on path.
func Locked(path string) bool {
	f, err := os.Open(path)
	if err != nil {
		return false
	}
	defer f.Close()
	err = syscall.Flock(int(f.Fd()), syscall.LOCK_SH|syscall.LOCK_NB)
	return errors.Is(err, syscall.EWOULDBLOCK)
}

func lock(path string, how int) (func() error, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	for {
		err = syscall.Flock(int(f.Fd()), how)
		if !errors.Is(err, syscall.EINTR) {
			break
		}
	}
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	return f.Close, nil
}
