//go:build linux

package socketstats

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

const supported = true

func sample(listeners map[string]*net.TCPAddr) snapshot {
	start := time.Now()
	s := snapshot{host: map[string]uint64{}, listeners: map[string]queues{}, hostOK: true, socketsOK: true}
	for _, file := range []struct{ path, protocol string }{{"/proc/net/netstat", "TcpExt:"}, {"/proc/net/snmp", "Tcp:"}} {
		f, err := os.Open(file.path)
		if err != nil {
			s.hostOK = false
			continue
		}
		err = readCounters(f, file.protocol, s.host)
		f.Close()
		if err != nil {
			s.hostOK = false
		}
	}
	for name, addr := range listeners {
		q, err := readQueues(addr)
		if err != nil {
			s.socketsOK = false
			continue
		}
		s.listeners[name] = q
	}
	s.duration = time.Since(start).Seconds()
	return s
}

// Headers vary between kernel versions; absent counters stay absent, never zero.
func readCounters(r io.Reader, protocol string, out map[string]uint64) error {
	scan := bufio.NewScanner(r)
	for scan.Scan() {
		header := strings.Fields(scan.Text())
		if len(header) == 0 || header[0] != protocol {
			continue
		}
		if !scan.Scan() {
			return fmt.Errorf("missing %s values", protocol)
		}
		values := strings.Fields(scan.Text())
		if len(values) != len(header) || values[0] != protocol {
			return fmt.Errorf("invalid %s values", protocol)
		}
		parsed := map[string]uint64{}
		for i, key := range header[1:] {
			if _, ok := hostMetrics[key]; !ok {
				continue
			}
			value, err := strconv.ParseUint(values[i+1], 10, 64)
			if err != nil {
				return err
			}
			parsed[key] = value
		}
		for key, value := range parsed {
			out[key] = value
		}
		return nil
	}
	if err := scan.Err(); err != nil {
		return err
	}
	return fmt.Errorf("missing %s header", protocol)
}

func readQueues(addr *net.TCPAddr) (queues, error) {
	var q queues
	fd, err := unix.Socket(unix.AF_NETLINK, unix.SOCK_DGRAM|unix.SOCK_CLOEXEC, unix.NETLINK_INET_DIAG)
	if err != nil {
		return q, err
	}
	defer unix.Close(fd)
	// Bound a failed diagnostic request; no receive-path goroutine waits on it.
	if err = unix.SetsockoptTimeval(fd, unix.SOL_SOCKET, unix.SO_RCVTIMEO, &unix.Timeval{Usec: 100000}); err != nil {
		return q, err
	}
	buf := make([]byte, 65536)
	for _, family := range []byte{unix.AF_INET, unix.AF_INET6} {
		if addr.IP.To4() != nil && family == unix.AF_INET6 {
			continue
		}
		// inet_diag_req_v2, followed by a source-port bytecode filter. The
		// filter keeps unrelated sockets out of the netlink reply.
		req := make([]byte, 16+56+12)
		binary.NativeEndian.PutUint32(req, uint32(len(req)))
		binary.NativeEndian.PutUint16(req[4:], 20) // SOCK_DIAG_BY_FAMILY
		binary.NativeEndian.PutUint16(req[6:], unix.NLM_F_REQUEST|unix.NLM_F_DUMP)
		binary.NativeEndian.PutUint32(req[8:], 1)
		req[16] = family
		req[17] = unix.IPPROTO_TCP
		binary.NativeEndian.PutUint32(req[20:], 0xfff&^(1<<6|1<<3)) // omit TIME_WAIT and SYN_RECV
		binary.NativeEndian.PutUint16(req[72:], 12)                 // nlattr length
		binary.NativeEndian.PutUint16(req[74:], 1)                  // INET_DIAG_REQ_BYTECODE
		req[76] = 11
		req[77] = 8                                 // INET_DIAG_BC_S_EQ; accept at end
		binary.NativeEndian.PutUint16(req[78:], 12) // reject beyond end
		binary.NativeEndian.PutUint16(req[82:], uint16(addr.Port))
		if err = unix.Sendto(fd, req, 0, &unix.SockaddrNetlink{Family: unix.AF_NETLINK}); err != nil {
			return queues{}, err
		}
		done := false
		for !done {
			n, _, flags, _, err := unix.Recvmsg(fd, buf, nil, 0)
			if errors.Is(err, unix.EINTR) {
				continue
			}
			if err != nil {
				return queues{}, err
			}
			if flags&unix.MSG_TRUNC != 0 {
				return queues{}, fmt.Errorf("truncated socket diagnostics")
			}
			msgs, err := syscall.ParseNetlinkMessage(buf[:n])
			if err != nil {
				return queues{}, err
			}
			for _, m := range msgs {
				if m.Header.Flags&unix.NLM_F_DUMP_INTR != 0 {
					return queues{}, fmt.Errorf("interrupted socket diagnostics")
				}
				switch m.Header.Type {
				case unix.NLMSG_DONE:
					if len(m.Data) >= 4 && int32(binary.NativeEndian.Uint32(m.Data)) < 0 {
						return queues{}, syscall.Errno(-int32(binary.NativeEndian.Uint32(m.Data)))
					}
					done = true
				case unix.NLMSG_ERROR:
					if len(m.Data) < 4 {
						return queues{}, fmt.Errorf("short netlink error")
					}
					code := int32(binary.NativeEndian.Uint32(m.Data))
					if code != 0 {
						return queues{}, syscall.Errno(-code)
					}
				case 20:
					if err = addSocket(&q, m.Data, addr); err != nil {
						return queues{}, err
					}
				}
			}
		}
	}
	return q, nil
}

func addSocket(q *queues, data []byte, addr *net.TCPAddr) error {
	if len(data) < 72 {
		return fmt.Errorf("short inet_diag_msg")
	}
	if int(binary.BigEndian.Uint16(data[4:])) != addr.Port {
		return nil
	}
	ip := net.IP(data[8:24])
	if data[0] == unix.AF_INET {
		ip = net.IP(data[8:12])
	}
	if !addr.IP.IsUnspecified() && len(addr.IP) > 0 && !addr.IP.Equal(ip) {
		return nil
	}
	rx := uint64(binary.NativeEndian.Uint32(data[56:]))
	if data[1] == 10 { // TCP_LISTEN: rqueue is backlog length, wqueue is limit.
		q.length += rx
		q.limit += uint64(binary.NativeEndian.Uint32(data[60:]))
	} else if data[1] != 6 && data[1] != 3 && binary.NativeEndian.Uint32(data[68:]) != 0 {
		q.bytes += rx
		q.connections++
	}
	return nil
}
