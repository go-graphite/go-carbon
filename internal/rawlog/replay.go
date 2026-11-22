package rawlog

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/go-graphite/go-carbon/points"
	"github.com/go-graphite/go-carbon/receiver/stream/codec"
)

type connectionState struct {
	flags   byte
	decoder *codec.Decoder
	broken  bool
}
type states map[uint64]*connectionState

func closeStates(s states) {
	for _, c := range s {
		c.decoder.Close()
	}
}

func (s states) accept(r record, visit codec.Visit) error {
	if r.kind == openRecord {
		flags, err := codec.ParsePreamble(r.data)
		if err != nil {
			return err
		}
		d, err := codec.NewDecoder(codec.Options{Flags: flags})
		if err != nil {
			return err
		}
		if old := s[r.conn]; old != nil {
			old.decoder.Close()
		}
		s[r.conn] = &connectionState{flags: flags, decoder: d}
		return nil
	}
	c := s[r.conn]
	if c == nil {
		return ErrCorrupt
	}
	if c.broken {
		return codec.ErrBroken
	}
	if r.kind == snapshotRecord {
		if len(r.data) < codec.HeaderSize || (r.data[0] != codec.ResetFrame && r.data[0] != codec.StateFrame) {
			c.broken = true
			return ErrCorrupt
		}
		visit = nil
	}
	_, err := c.decoder.Decode(r.data, visit)
	if err != nil {
		c.broken = true
	}
	return err
}

func scanStates(rd io.Reader) (states, error) {
	s := make(states)
	err := walk(rd, func(r record) error { return s.accept(r, nil) })
	if err != nil {
		closeStates(s)
		return nil, err
	}
	return s, nil
}

// walk verifies the complete snapshot prefix before exposing live records. The
// ready marker records the prior segment's last sequence. A missing/partial
// marker or any outer checksum/sequence failure is a hard segment error.
func walk(rd io.Reader, visit func(record) error) error {
	rd = bufio.NewReaderSize(rd, 1<<20)
	if err := checkMagic(rd); err != nil {
		return err
	}
	ready := false
	var sequence uint64
	for {
		r, err := readRecord(rd)
		if err == io.EOF && ready {
			return nil
		}
		if err != nil {
			return err
		}
		if !ready {
			if r.kind == readyRecord {
				if len(r.data) != 0 || r.conn != 0 {
					return ErrCorrupt
				}
				ready = true
				sequence = r.seq
				continue
			}
			if r.seq != 0 || (r.kind != openRecord && r.kind != snapshotRecord) {
				return ErrCorrupt
			}
		} else {
			if (r.kind != openRecord && r.kind != frameRecord) || r.seq != sequence+1 {
				return ErrCorrupt
			}
			sequence = r.seq
		}
		if err = visit(r); err != nil {
			return err
		}
	}
}

type ReplayStats struct{ Frames, Points, Broken uint64 }
type ReplayOptions struct{ Workers int }
type task struct {
	index uint64
	r     record
}
type result struct {
	index  uint64
	r      record
	points []*points.Points
	err    error
}

// Replay decodes connections concurrently, then applies complete records in
// global capture order. apply owns its Points. This is stricter than per-name
// order and preserves cross-connection corrections and tagged alias barriers.
// Payload errors poison only that connection until a later segment's snapshot;
// other connections still apply. The returned error forbids silent success.
// Reader must end at a durable frontier when used beside live capture.
func Replay(rd io.Reader, o ReplayOptions, apply func(uint64, *points.Points) error) (ReplayStats, error) {
	workers := o.Workers
	if workers <= 0 {
		workers = 1
	}
	workers = min(workers, 280)
	queues := make([]chan task, workers)
	window := make(chan struct{}, workers*2)
	results := make(chan result, workers*2)
	var wg sync.WaitGroup
	for i := range queues {
		queues[i] = make(chan task, 2)
		wg.Add(1)
		go func(q <-chan task) {
			defer wg.Done()
			s := make(states)
			defer closeStates(s)
			for t := range q {
				out := result{index: t.index, r: t.r}
				var previousID uint32
				var group *points.Points
				out.err = s.accept(t.r, func(id uint32, name []byte, value float64, stamp int64, _ *any) {
					if group == nil || previousID != id {
						group = &points.Points{Metric: string(name), FirstArrival: t.r.arrival}
						out.points = append(out.points, group)
						previousID = id
					}
					group.Data = append(group.Data, points.Point{Value: value, Timestamp: stamp})
				})
				results <- out
			}
		}(queues[i])
	}
	readDone := make(chan error, 1)
	go func() {
		var index uint64
		err := walk(rd, func(r record) error {
			window <- struct{}{}
			queues[r.conn%uint64(workers)] <- task{index: index, r: r}
			index++
			return nil
		})
		for _, q := range queues {
			close(q)
		}
		wg.Wait()
		close(results)
		readDone <- err
	}()
	var stats ReplayStats
	var replayErr error
	applyFailed := false
	pending := make(map[uint64]result)
	var next uint64
	for out := range results {
		pending[out.index] = out
		for {
			v, ok := pending[next]
			if !ok {
				break
			}
			delete(pending, next)
			next++
			if v.r.kind == frameRecord {
				stats.Frames++
			}
			if v.err != nil {
				stats.Broken++
				if replayErr == nil {
					replayErr = fmt.Errorf("connection %d record %d: %w", v.r.conn, v.r.seq, v.err)
				}
			}
			if v.err == nil && !applyFailed {
				for _, p := range v.points {
					if apply != nil {
						if err := apply(v.r.seq, p); err != nil {
							replayErr = errors.Join(replayErr, err)
							applyFailed = true
							break
						}
					}
					stats.Points += uint64(len(p.Data))
				}
			}
			<-window
		}
	}
	return stats, errors.Join(replayErr, <-readDone)
}
