package partlog

import "io"

type readChunk struct {
	data []byte
	err  error
}

// Two buffers overlap sequential device reads with CRC/decode/apply. Section
// bounds stop prefetch at the captured complete-record frontier, so following a
// growing segment never reads a future payload and then reads it again.
type readAhead struct {
	free       chan []byte
	ready      chan readChunk
	stop, done chan struct{}
	current    []byte
	used       int
	err        error
	buffers    [2][]byte
}

func newReadAhead(src io.Reader, size int64, buffers [2][]byte) *readAhead {
	r := &readAhead{free: make(chan []byte, 2), ready: make(chan readChunk, 2), stop: make(chan struct{}), done: make(chan struct{})}
	for i := range buffers {
		if int64(cap(buffers[i])) < size {
			buffers[i] = make([]byte, max(1, size))
		}
		buffers[i] = buffers[i][:max(1, size)]
		r.free <- buffers[i]
	}
	r.buffers = buffers
	go func() {
		defer close(r.done)
		for {
			var buf []byte
			select {
			case <-r.stop:
				return
			case buf = <-r.free:
			}
			n, err := io.ReadFull(src, buf)
			select {
			case <-r.stop:
				return
			case r.ready <- readChunk{buf[:n], err}:
			}
			if err != nil {
				return
			}
		}
	}()
	return r
}

func (r *readAhead) Read(dst []byte) (int, error) {
	if len(dst) == 0 {
		return 0, nil
	}
	if r.used == len(r.current) {
		if r.err != nil {
			if r.err == io.ErrUnexpectedEOF {
				return 0, io.EOF
			}
			return 0, r.err
		}
		if r.current != nil {
			r.free <- r.current[:cap(r.current)]
		}
		chunk := <-r.ready
		r.current, r.used, r.err = chunk.data, 0, chunk.err
		if len(r.current) == 0 {
			return 0, r.err
		}
	}
	n := copy(dst, r.current[r.used:])
	r.used += n
	return n, nil
}

func (r *readAhead) Close() { close(r.stop); <-r.done }
