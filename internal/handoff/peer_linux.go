//go:build linux

package handoff

import (
	"fmt"
	"net"

	"golang.org/x/sys/unix"
)

// peerExecutable returns a path that executes the binary of the process on
// the other end of conn.
func peerExecutable(conn *net.UnixConn) (string, error) {
	raw, err := conn.SyscallConn()
	if err != nil {
		return "", err
	}
	var cred *unix.Ucred
	var credErr error
	if err = raw.Control(func(fd uintptr) {
		cred, credErr = unix.GetsockoptUcred(int(fd), unix.SOL_SOCKET, unix.SO_PEERCRED)
	}); err != nil {
		return "", err
	}
	if credErr != nil {
		return "", credErr
	}
	return fmt.Sprintf("/proc/%d/exe", cred.Pid), nil
}
