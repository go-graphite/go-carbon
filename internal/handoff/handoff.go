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
	// Shared-store input handoff: the stopping instance stopped its reads
	// (their data is in a database only one process opens) and offers input.
	msgInputOnly = 'O'
)

// Protocol is printed by a binary that can take over a listener, so a
// stopping instance offers it only to a successor that supports handoff.
const Protocol = "go-carbon-read-handoff/1"

// InputProtocol is printed by a binary that can also take over input sockets.
// Older binaries reject the flag, and are offered only the read listener.
const InputProtocol = "go-carbon-input-handoff/1"

// SharedStoreProtocol confirms that mirrored migration can take both sockets
// before opening a predecessor-owned database. Older successors keep the
// existing database-release order.
const SharedStoreProtocol = "go-carbon-shared-store-handoff/1"

// StoreInputProtocol is printed by a binary that can take input from a
// stopping instance that keeps no read listener: a shared store's reads need
// its database, which only one process opens, so they restart while input
// moves. Older binaries reject the flag and get the stop/rebind path.
const StoreInputProtocol = "go-carbon-store-input-handoff/1"

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
	return protocolSupported(binary, "-input-handoff-protocol", InputProtocol)
}

// StoreInputSupported reports whether binary takes input without the read
// listener (see StoreInputProtocol).
func StoreInputSupported(binary string) bool {
	return protocolSupported(binary, "-store-input-handoff-protocol", StoreInputProtocol)
}

func SharedStoreSupported(binary, config string) bool {
	return protocolSupported(binary, "-shared-store-handoff-protocol", SharedStoreProtocol, "-config", config)
}

// RegisterTimeout bounds how long a stopping instance waits for a capable
// successor to register, which it does right after parsing its config.
var RegisterTimeout = 10 * time.Second

// SuccessorSupported runs the binary the service manager will start next and
// reports whether it supports handoff. Binaries without support reject the
// flag, so the stopping instance closes its listener for them as before.
func SuccessorSupported(binary string) bool {
	return protocolSupported(binary, "-handoff-protocol", Protocol)
}

func protocolSupported(binary, flag, protocol string, extra ...string) bool {
	supported, _ := protocolSupport(binary, flag, protocol, extra...)
	return supported
}

// protocolSupport also reports whether binary answered at all: known is false
// when it could not be run or did not exit in time, which says nothing about
// its support. A binary without the flag rejects it and exits non-zero.
func protocolSupport(binary, flag, protocol string, extra ...string) (supported, known bool) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	args := append([]string{flag}, extra...)
	out, err := exec.CommandContext(ctx, binary, args...).Output()
	if err == nil {
		return strings.TrimSpace(string(out)) == protocol, true
	}
	var exit *exec.ExitError
	return false, errors.As(err, &exit) && ctx.Err() == nil
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
	_, err = serveInput(conn, input, inputTimeout)
	return true, err
}

// ErrSuccessorCannotTakeInput reports that the process that registered for a
// store input handoff runs a binary without support for it.
var ErrSuccessorCannotTakeInput = errors.New("successor binary cannot take input")

// PeerExecutable returns a path executing the binary of the process that
// registered on conn; tests replace it. It fails where the platform cannot
// tell.
var PeerExecutable = peerExecutable

// ServeInputStats describes a ServeInput call for the stopping side's log.
type ServeInputStats struct {
	// Registered is when the successor registered, or zero.
	Registered time.Time
	// Peer is the binary of the registered successor, if known.
	Peer string
	// PeerSupported is whether Peer takes store input; PeerKnown whether
	// that could be determined.
	PeerSupported, PeerKnown bool
}

// ServeInput is the stopping side of a shared-store input handoff, for an
// instance that already stopped its reads. It waits RegisterTimeout for the
// successor to register and up to timeout for its listener request, answers
// that no listener is offered but input is, so the successor binds reads
// itself, then hands input over as Serve does. input.Detach runs only on the
// successor's input request. It reports whether the successor took input; on
// any failure the caller still owns input and stops it normally.
//
// The successor registers right after parsing its config but asks only once
// its storage is open and its checkpoint serves. A binary without store input
// handoff (a rollback through a service override starts a different binary
// than the one checked before the dump) never takes input and replays all the
// input received meanwhile from the tail before it reads input itself, so the
// binary of the registered process is checked first: if it certainly cannot
// take input, ServeInput returns ErrSuccessorCannotTakeInput at once.
func (o *Offer) ServeInput(input InputSource, timeout time.Duration) (bool, error) {
	_, taken, err := o.ServeInputStats(input, timeout)
	return taken, err
}

// ServeInputStats is ServeInput, also describing the successor.
func (o *Offer) ServeInputStats(input InputSource, timeout time.Duration) (ServeInputStats, bool, error) {
	var stats ServeInputStats
	defer o.Close()
	if err := o.ln.SetDeadline(time.Now().Add(RegisterTimeout)); err != nil {
		return stats, false, err
	}
	conn, err := o.ln.AcceptUnix()
	if err != nil {
		return stats, false, err
	}
	defer conn.Close()
	stats.Registered = time.Now()
	if exe, perr := PeerExecutable(conn); perr == nil {
		stats.Peer = exe
		stats.PeerSupported, stats.PeerKnown = protocolSupport(exe, "-store-input-handoff-protocol", StoreInputProtocol)
		if stats.PeerKnown && !stats.PeerSupported {
			return stats, false, fmt.Errorf("%w: %s", ErrSuccessorCannotTakeInput, exe)
		}
	}
	if err = conn.SetReadDeadline(time.Now().Add(timeout)); err != nil {
		return stats, false, err
	}
	var b [1]byte
	if _, err = conn.Read(b[:]); err != nil || b[0] != msgRequest {
		return stats, false, fmt.Errorf("successor did not request the listener: %w", err)
	}
	if _, err = conn.Write([]byte{msgInputOnly}); err != nil {
		return stats, false, err
	}
	taken, err := serveInput(conn, input, timeout)
	return stats, taken, err
}

// serveInput waits for the successor's input request and hands input over. It
// reports whether the successor confirmed it holds the input.
func serveInput(conn *net.UnixConn, input InputSource, timeout time.Duration) (bool, error) {
	if err := conn.SetDeadline(time.Now().Add(timeout)); err != nil {
		return false, err
	}
	var b [1]byte
	if _, err := conn.Read(b[:]); err != nil || b[0] != msgInputRequest {
		return false, fmt.Errorf("successor did not request input: %w", err)
	}
	in, err := input.Detach()
	if err != nil || in == nil {
		_, werr := conn.Write([]byte{msgInputNone})
		return false, errors.Join(err, werr)
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
		return false, err
	}
	msg := binary.BigEndian.AppendUint32([]byte{msgInput}, uint32(len(raw)))
	if _, err = conn.Write(append(msg, raw...)); err != nil {
		return false, err
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
			return false, err
		}
		files = files[len(batch):]
		// Some kernels refuse descriptors while earlier ones are unread.
		if err = conn.SetReadDeadline(time.Now().Add(30 * time.Second)); err != nil {
			return false, err
		}
		if _, err = conn.Read(b[:]); err != nil || b[0] != msgFilesTaken {
			return false, fmt.Errorf("successor did not receive input files: %w", err)
		}
	}
	if err = conn.SetReadDeadline(time.Now().Add(30 * time.Second)); err != nil {
		return false, err
	}
	if _, err = conn.Read(b[:]); err != nil || b[0] != msgInputTaken {
		return false, fmt.Errorf("successor did not confirm input: %w", err)
	}
	return true, nil
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
	if err == nil && n == 1 && buf[0] == msgInputOnly && oobn == 0 {
		// A shared-store instance stopped its reads but offers input: bind
		// reads normally and take input when receivers start (TakeInput).
		c.inputOffered = true
		return nil, nil, nil
	}
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
