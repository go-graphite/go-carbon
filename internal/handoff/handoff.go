//go:build unix

// Package handoff passes a listening TCP socket from a stopping instance to its
// successor so reads never see a refused connection during a restart. The
// stopping instance keeps accepting until the successor serves from the same
// socket, then stops accepting before the successor opens anything else.
package handoff

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"time"
)

const (
	msgRequest  = 'R'
	msgListener = 'L'
	msgTaken    = 'T'
	msgStopped  = 'S'
)

// Protocol is printed by a binary that can take over a listener, so a
// stopping instance offers it only to a successor that supports handoff.
const Protocol = "go-carbon-read-handoff/1"

// RegisterTimeout bounds how long a stopping instance waits for a capable
// successor to register, which it does right after parsing its config.
var RegisterTimeout = 10 * time.Second

// SuccessorSupported runs the binary the service manager will start next and
// reports whether it supports handoff. Binaries without support reject the
// flag, so the stopping instance closes its listener for them as before.
func SuccessorSupported(binary string) bool {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, binary, "-handoff-protocol").Output()
	return err == nil && strings.TrimSpace(string(out)) == Protocol
}

// Offer is the stopping side of one handoff.
type Offer struct {
	path string
	ln   *net.UnixListener
}

// NewOffer listens on a private unix socket at path, replacing a stale one.
func NewOffer(path string) (*Offer, error) {
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	ln, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
	if err != nil {
		return nil, err
	}
	if err = os.Chmod(path, 0600); err != nil {
		_ = ln.Close()
		return nil, err
	}
	return &Offer{path: path, ln: ln}, nil
}

// Close removes the socket; a successor that has not connected binds normally.
func (o *Offer) Close() error {
	o.ln.SetUnlinkOnClose(true)
	return o.ln.Close()
}

// Serve waits RegisterTimeout for a successor to register, then up to timeout
// for it to request tcp, and sends it a duplicate. Once the successor reports
// that it serves the socket, stopAccepting runs and the successor is told so.
// It returns whether the successor took over; on any failure the caller still
// owns tcp and stops it normally.
func (o *Offer) Serve(timeout time.Duration, tcp *net.TCPListener, stopAccepting func()) (bool, error) {
	defer o.Close()
	if err := o.ln.SetDeadline(time.Now().Add(RegisterTimeout)); err != nil {
		return false, err
	}
	conn, err := o.ln.AcceptUnix()
	if err != nil {
		return false, err
	}
	defer conn.Close()
	// The successor may need minutes to restore before it can serve. Keep
	// serving meanwhile; a crash closes the connection and ends the wait.
	if err = conn.SetReadDeadline(time.Now().Add(timeout)); err != nil {
		return false, err
	}
	var b [1]byte
	if _, err = conn.Read(b[:]); err != nil || b[0] != msgRequest {
		return false, fmt.Errorf("successor did not request the listener: %w", err)
	}
	file, err := tcp.File()
	if err != nil {
		return false, err
	}
	_, _, err = conn.WriteMsgUnix([]byte{msgListener}, syscall.UnixRights(int(file.Fd())), nil)
	_ = file.Close()
	if err != nil {
		return false, err
	}
	// The successor still waits for its index before it serves.
	if err = conn.SetReadDeadline(time.Now().Add(timeout)); err != nil {
		return false, err
	}
	if _, err = conn.Read(b[:]); err != nil || b[0] != msgTaken {
		return false, fmt.Errorf("successor did not take over: %w", err)
	}
	stopAccepting()
	_, err = conn.Write([]byte{msgStopped})
	return true, err
}

// Claim is a successor's registration with a stopping instance.
type Claim struct{ conn *net.UnixConn }

// Register tells a stopping instance at path that this process will take its
// listener. Call it early in startup; it returns nil when nothing is offered.
func Register(path string) *Claim {
	conn, err := net.DialTimeout("unix", path, time.Second)
	if err != nil {
		return nil
	}
	return &Claim{conn: conn.(*net.UnixConn)}
}

// Close abandons the claim; the stopping instance then closes its listener.
func (c *Claim) Close() error { return c.conn.Close() }

// Take requests the listener. The caller must call commit once it serves the
// listener; commit returns after the old instance stopped accepting. On error
// the claim is abandoned and the old instance closes its listener.
func (c *Claim) Take(want *net.TCPAddr) (net.Listener, func() error, error) {
	uc := c.conn
	if _, err := uc.Write([]byte{msgRequest}); err != nil {
		_ = uc.Close()
		return nil, nil, err
	}
	if err := uc.SetReadDeadline(time.Now().Add(10 * time.Second)); err != nil {
		_ = uc.Close()
		return nil, nil, err
	}
	buf, oob := make([]byte, 1), make([]byte, syscall.CmsgSpace(4))
	n, oobn, _, _, err := uc.ReadMsgUnix(buf, oob)
	if err == nil && (n != 1 || buf[0] != msgListener) {
		err = errors.New("unexpected handoff message")
	}
	var fds []int
	if err == nil {
		var msgs []syscall.SocketControlMessage
		if msgs, err = syscall.ParseSocketControlMessage(oob[:oobn]); err == nil && len(msgs) == 1 {
			fds, err = syscall.ParseUnixRights(&msgs[0])
		}
		if err == nil && len(fds) != 1 {
			err = errors.New("handoff carried no listener")
		}
	}
	if err != nil {
		_ = uc.Close()
		return nil, nil, err
	}
	file := os.NewFile(uintptr(fds[0]), "inherited-listener")
	ln, err := net.FileListener(file)
	_ = file.Close()
	if err == nil && !sameAddr(ln.Addr(), want) {
		_ = ln.Close()
		err = fmt.Errorf("inherited listener %s does not match %s", ln.Addr(), want)
	}
	if err != nil {
		_ = uc.Close()
		return nil, nil, err
	}
	commit := func() error {
		defer uc.Close()
		if _, err := uc.Write([]byte{msgTaken}); err != nil {
			return err
		}
		if err := uc.SetReadDeadline(time.Now().Add(30 * time.Second)); err != nil {
			return err
		}
		var b [1]byte
		if _, err := uc.Read(b[:]); err != nil || b[0] != msgStopped {
			return fmt.Errorf("old instance did not confirm it stopped accepting: %w", err)
		}
		return nil
	}
	return ln, commit, nil
}

func sameAddr(got net.Addr, want *net.TCPAddr) bool {
	g, ok := got.(*net.TCPAddr)
	if !ok || g.Port != want.Port {
		return false
	}
	if len(want.IP) == 0 || want.IP.IsUnspecified() {
		return g.IP.IsUnspecified()
	}
	return g.IP.Equal(want.IP)
}
