//go:build !linux

package socketstats

import "net"

const supported = false

func sample(map[string]*net.TCPAddr) snapshot { return snapshot{} }
