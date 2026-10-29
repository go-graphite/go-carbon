//go:build unix && !linux

package handoff

import (
	"errors"
	"net"
)

func peerExecutable(*net.UnixConn) (string, error) {
	return "", errors.New("peer executable unknown on this platform")
}
