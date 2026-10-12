//go:build unix

// Package handoff passes a listening TCP socket from a stopping instance to its
// successor so reads never see a refused connection during a restart. The
// stopping instance keeps accepting until the successor serves from the same
// socket, then stops accepting before the successor opens anything else.
//
// A successor that also supports input handoff then asks for the stopping
// instance's input: its listening sockets and accepted connections, each with
// the bytes read but not yet parsed, so senders never see input closed.
package handoff

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
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
	// Input handoff, after the old instance stopped accepting reads.
	msgStoppedInput = 'Q' // like msgStopped; input is offered
	msgInputRequest = 'I'
	msgInputNone    = 'N' // input already stopped; nothing to take
	msgInput        = 'P' // header, then file descriptors
	msgFiles        = 'F'
	msgFilesTaken   = 'f'
	msgInputTaken   = 'A'
)

// Protocol is printed by a binary that can take over a listener, so a
// stopping instance offers it only to a successor that supports handoff.
const Protocol = "go-carbon-read-handoff/1"

// InputProtocol is printed by a binary that can also take over input sockets.
// Older binaries reject the flag, and are offered only the read listener.
const InputProtocol = "go-carbon-input-handoff/1"

// maxFilesPerMessage stays well below Linux's SCM_MAX_FD (253); other
// kernels may refuse even fewer, and a refused batch is halved.
const maxFilesPerMessage = 64

// Input is a stopping instance's input as handed to its successor.
type Input struct {
	Receivers []InputReceiver
}

// InputReceiver is one receiver's listening socket and accepted connections.
type InputReceiver struct {
	Name     string
	Listener *os.File
	Conns    []InputConn
}

// InputConn is an accepted connection and the bytes read from it but not yet
// parsed, which the successor must parse first.
type InputConn struct {
	File     *os.File
	Leftover []byte
}

// Close closes every file of in; the receiving side owns its own copies.
func (in *Input) Close() {
	for _, r := range in.Receivers {
		if r.Listener != nil {
			_ = r.Listener.Close()
		}
		for _, c := range r.Conns {
			_ = c.File.Close()
		}
	}
}

func (in *Input) files() []*os.File {
	var files []*os.File
	for _, r := range in.Receivers {
		files = append(files, r.Listener)
		for _, c := range r.Conns {
			files = append(files, c.File)
		}
	}
	return files
}

type inputHeader struct {
	Receivers []struct {
		Name      string
		Leftovers [][]byte
	}
}

// InputSource is the stopping side's input.
type InputSource interface {
	// Detach stops input without closing its sockets, after every point read
	// so far is stored. It returns nil if input already stopped.
	Detach() (*Input, error)
}

// InputSupported reports whether binary also takes over input sockets.
func InputSupported(binary string) bool {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, binary, "-input-handoff-protocol").Output()
	return err == nil && strings.TrimSpace(string(out)) == InputProtocol
}

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
//
// With a non-nil input, the successor is then offered input, and Serve waits
// up to inputTimeout for it to ask. input.Detach runs only on that request.
func (o *Offer) Serve(timeout time.Duration, tcp *net.TCPListener, stopAccepting func(), input InputSource, inputTimeout time.Duration) (bool, error) {
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
	if input == nil {
		_, err = conn.Write([]byte{msgStopped})
		return true, err
	}
	if _, err = conn.Write([]byte{msgStoppedInput}); err != nil {
		return true, err
	}
	return true, serveInput(conn, input, inputTimeout)
}

// serveInput waits for the successor's input request and hands input over.
func serveInput(conn *net.UnixConn, input InputSource, timeout time.Duration) error {
	if err := conn.SetDeadline(time.Now().Add(timeout)); err != nil {
		return err
	}
	var b [1]byte
	if _, err := conn.Read(b[:]); err != nil || b[0] != msgInputRequest {
		return fmt.Errorf("successor did not request input: %w", err)
	}
	in, err := input.Detach()
	if err != nil || in == nil {
		_, werr := conn.Write([]byte{msgInputNone})
		return errors.Join(err, werr)
	}
	// The successor holds its own copies once they are sent.
	defer in.Close()
	var header inputHeader
	for _, r := range in.Receivers {
		var leftovers [][]byte
		for _, c := range r.Conns {
			leftovers = append(leftovers, c.Leftover)
		}
		header.Receivers = append(header.Receivers, struct {
			Name      string
			Leftovers [][]byte
		}{r.Name, leftovers})
	}
	raw, err := json.Marshal(header)
	if err != nil {
		return err
	}
	msg := binary.BigEndian.AppendUint32([]byte{msgInput}, uint32(len(raw)))
	if _, err = conn.Write(append(msg, raw...)); err != nil {
		return err
	}
	files, size := in.files(), maxFilesPerMessage
	for len(files) > 0 {
		batch := files[:min(len(files), size)]
		fds := make([]int, len(batch))
		for i, f := range batch {
			fds[i] = int(f.Fd())
		}
		_, _, err = conn.WriteMsgUnix([]byte{msgFiles}, syscall.UnixRights(fds...), nil)
		if (errors.Is(err, syscall.EMSGSIZE) || errors.Is(err, syscall.ENOBUFS)) && size > 1 {
			size /= 2
			continue
		}
		if err != nil {
			return err
		}
		files = files[len(batch):]
		// Some kernels refuse descriptors while earlier ones are unread.
		if err = conn.SetReadDeadline(time.Now().Add(30 * time.Second)); err != nil {
			return err
		}
		if _, err = conn.Read(b[:]); err != nil || b[0] != msgFilesTaken {
			return fmt.Errorf("successor did not receive input files: %w", err)
		}
	}
	if err = conn.SetReadDeadline(time.Now().Add(30 * time.Second)); err != nil {
		return err
	}
	if _, err = conn.Read(b[:]); err != nil || b[0] != msgInputTaken {
		return fmt.Errorf("successor did not confirm input: %w", err)
	}
	return nil
}

// Claim is a successor's registration with a stopping instance.
type Claim struct {
	conn         *net.UnixConn
	inputOffered bool
}

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
		keep := false
		defer func() {
			if !keep {
				_ = uc.Close()
			}
		}()
		if _, err := uc.Write([]byte{msgTaken}); err != nil {
			return err
		}
		if err := uc.SetReadDeadline(time.Now().Add(30 * time.Second)); err != nil {
			return err
		}
		var b [1]byte
		if _, err := uc.Read(b[:]); err != nil || (b[0] != msgStopped && b[0] != msgStoppedInput) {
			return fmt.Errorf("old instance did not confirm it stopped accepting: %w", err)
		}
		keep = b[0] == msgStoppedInput
		c.inputOffered = keep
		return nil
	}
	return ln, commit, nil
}

// InputOffered reports whether the stopping instance offered its input when it
// confirmed the read handoff.
func (c *Claim) InputOffered() bool { return c.inputOffered }

// TakeInput asks for the stopping instance's input. It returns nil if none was
// offered or input already stopped. The caller owns the returned files.
func (c *Claim) TakeInput() (*Input, error) {
	if !c.inputOffered {
		return nil, nil
	}
	uc := c.conn
	defer uc.Close()
	c.inputOffered = false
	if _, err := uc.Write([]byte{msgInputRequest}); err != nil {
		return nil, err
	}
	// The old instance stops reading and syncs input it already read first.
	if err := uc.SetReadDeadline(time.Now().Add(time.Minute)); err != nil {
		return nil, err
	}
	var b [5]byte
	if _, err := io.ReadFull(uc, b[:1]); err != nil {
		return nil, err
	}
	switch b[0] {
	case msgInputNone:
		return nil, nil
	case msgInput:
	default:
		return nil, errors.New("unexpected input handoff message")
	}
	if _, err := io.ReadFull(uc, b[1:]); err != nil {
		return nil, err
	}
	raw := make([]byte, binary.BigEndian.Uint32(b[1:]))
	if _, err := io.ReadFull(uc, raw); err != nil {
		return nil, err
	}
	var header inputHeader
	if err := json.Unmarshal(raw, &header); err != nil {
		return nil, err
	}
	want := 0
	for _, r := range header.Receivers {
		want += 1 + len(r.Leftovers)
	}
	files, err := receiveFiles(uc, want)
	if err != nil {
		for _, f := range files {
			_ = f.Close()
		}
		return nil, err
	}
	in := &Input{}
	for _, r := range header.Receivers {
		ir := InputReceiver{Name: r.Name, Listener: files[0]}
		files = files[1:]
		for _, leftover := range r.Leftovers {
			ir.Conns = append(ir.Conns, InputConn{File: files[0], Leftover: leftover})
			files = files[1:]
		}
		in.Receivers = append(in.Receivers, ir)
	}
	if _, err = uc.Write([]byte{msgInputTaken}); err != nil {
		in.Close()
		return nil, err
	}
	return in, nil
}

func receiveFiles(uc *net.UnixConn, want int) ([]*os.File, error) {
	files := make([]*os.File, 0, want)
	buf, oob := make([]byte, 1), make([]byte, syscall.CmsgSpace(4*maxFilesPerMessage))
	for len(files) < want {
		n, oobn, flags, _, err := uc.ReadMsgUnix(buf, oob)
		if err != nil {
			return files, err
		}
		if n != 1 || buf[0] != msgFiles || flags&syscall.MSG_CTRUNC != 0 {
			return files, errors.New("unexpected input handoff files")
		}
		msgs, err := syscall.ParseSocketControlMessage(oob[:oobn])
		if err != nil {
			return files, err
		}
		for _, m := range msgs {
			fds, err := syscall.ParseUnixRights(&m)
			if err != nil {
				return files, err
			}
			for _, fd := range fds {
				syscall.CloseOnExec(fd)
				files = append(files, os.NewFile(uintptr(fd), "inherited-input"))
			}
		}
		if _, err = uc.Write([]byte{msgFilesTaken}); err != nil {
			return files, err
		}
	}
	if len(files) != want {
		return files, fmt.Errorf("received %d input files, want %d", len(files), want)
	}
	return files, nil
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
