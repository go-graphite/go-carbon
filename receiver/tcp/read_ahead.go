package tcp

import (
	"errors"
	"io"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

const handoffReadAheadBytes = 8 << 20
const handoffReadBlock = 64 << 10

// readAheadConn drains inherited sockets while their dictionaries and the input
// tail are replayed. A lazy byte ring bounds retained input independently of
// packet sizes; full rings apply TCP backpressure. Only the handler parses
// bytes or emits points. The pump owns one additional read block.
type readAheadConn struct {
	net.Conn
	mu                 sync.Mutex
	readable, writable *sync.Cond
	data               []byte
	head, size         int
	last               []byte
	err                error
	stopping           bool
	once               sync.Once
	done               chan struct{}
	ready              atomic.Bool
	direct             bool
	tracked            *handoffConn
	interrupted        bool
}

func newReadAheadConn(c net.Conn) *readAheadConn {
	r := &readAheadConn{Conn: c, done: make(chan struct{})}
	r.readable = sync.NewCond(&r.mu)
	r.writable = sync.NewCond(&r.mu)
	go r.pump()
	return r
}

func (r *readAheadConn) pump() {
	defer close(r.done)
	b := make([]byte, handoffReadBlock)
	for {
		n, err := r.Conn.Read(b)
		r.mu.Lock()
		rest := b[:n]
		for len(rest) > 0 {
			if len(r.data)-r.size < len(rest) && len(r.data) < handoffReadAheadBytes {
				data := make([]byte, min(handoffReadAheadBytes, max(handoffReadBlock, 2*len(r.data))))
				first := copy(data, r.data[r.head:min(len(r.data), r.head+r.size)])
				copy(data[first:], r.data[:r.size-first])
				r.data, r.head = data, 0
			}
			if r.size == len(r.data) {
				if r.stopping {
					r.last = append([]byte(nil), rest...)
					break
				}
				r.writable.Wait()
				continue
			}
			tail := (r.head + r.size) % len(r.data)
			count := min(len(rest), len(r.data)-r.size, len(r.data)-tail)
			copy(r.data[tail:], rest[:count])
			r.size += count
			rest = rest[count:]
			r.readable.Signal()
		}
		if err != nil || r.stopping {
			r.interrupted = r.stopping && (err == nil || errors.Is(err, os.ErrDeadlineExceeded))
			if err == nil {
				err = io.EOF
			}
			r.err = err
			r.readable.Broadcast()
			r.mu.Unlock()
			return
		}
		r.mu.Unlock()
	}
}

func (r *readAheadConn) Read(b []byte) (int, error) {
	if len(b) == 0 {
		return 0, nil
	}
	r.mu.Lock()
	for {
		if r.ready.Load() && !r.direct && r.err == nil {
			r.mu.Unlock()
			r.pause()
			// Only the handler switches back to direct reads. Order the
			// deadline reset with Detach's interrupt on the original socket.
			r.mu.Lock()
			if r.interrupted {
				r.tracked.setReadDeadline(r.Conn, time.Now().Add(2*time.Minute))
				r.direct, r.err = true, nil
			}
		}
		if r.size > 0 || r.err != nil || r.direct {
			break
		}
		r.readable.Wait()
	}
	if r.size == 0 {
		if len(r.last) > 0 {
			n := copy(b, r.last)
			r.last = r.last[n:]
			r.mu.Unlock()
			return n, nil
		}
		if r.direct {
			r.data, r.last, r.head = nil, nil, 0
			r.mu.Unlock()
			return r.Conn.Read(b)
		}
		err := r.err
		if r.ready.Load() {
			r.data, r.head = nil, 0
		}
		r.mu.Unlock()
		return 0, err
	}
	n := min(len(b), r.size, len(r.data)-r.head)
	copy(b, r.data[r.head:r.head+n])
	r.head = (r.head + n) % len(r.data)
	r.size -= n
	r.writable.Signal()
	r.mu.Unlock()
	return n, nil
}

func (r *readAheadConn) finish() {
	r.ready.Store(true)
	r.mu.Lock()
	r.readable.Broadcast()
	r.mu.Unlock()
}

func (r *readAheadConn) pause() {
	r.once.Do(func() {
		r.mu.Lock()
		r.stopping = true
		r.writable.Broadcast()
		r.mu.Unlock()
		_ = r.Conn.SetReadDeadline(time.Unix(1, 0))
	})
	<-r.done
}

// pending runs only in the handler, after its last Read. Preserve read-ahead
// too when a successor restarts before its buffer has drained.
func (r *readAheadConn) pending(dst []byte) []byte {
	r.pause()
	r.mu.Lock()
	defer r.mu.Unlock()
	first := min(r.size, len(r.data)-r.head)
	dst = append(dst, r.data[r.head:r.head+first]...)
	dst = append(dst, r.data[:r.size-first]...)
	return append(dst, r.last...)
}

func (r *readAheadConn) Close() error {
	err := r.Conn.Close()
	r.pause()
	return err
}
