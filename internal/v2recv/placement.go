package v2recv

import (
	"bufio"
	"errors"
	"io"
	"net"
	"time"

	"github.com/go-graphite/go-carbon/internal/v2wire"
)

// PlacedSink returns only after original stamps/positions and data are durable.
// Retries must be deduplicated against the recovered original journal.
type PlacedSink interface{ AcceptPlaced(v2wire.Frame) error }
type PlacedBatchSink interface{ AcceptPlacedBatch([]v2wire.Frame) error }

func (s *Server) readPlaced(c net.Conn) error {
	reader := bufio.NewReaderSize(countedReader{c, &s.stats.wire}, 256<<10)
	writer := bufio.NewWriterSize(c, 256<<10)
	frames := make([]v2wire.Frame, 0, 256)
	for {
		frames = frames[:0]
		batchBytes := 0
		var terminal error
		for len(frames) < 256 {
			if len(frames) > 0 && reader.Buffered() == 0 {
				_ = c.SetReadDeadline(time.Now().Add(2 * time.Millisecond))
				_, err := reader.Peek(1)
				_ = c.SetReadDeadline(time.Time{})
				if err != nil {
					var timeout net.Error
					if !errors.As(err, &timeout) || !timeout.Timeout() {
						terminal = err
					}
					break
				}
			}
			f, err := v2wire.ReadKeys(reader, s.cfg.PlacementKeys)
			if err != nil {
				terminal = err
				break
			}
			if f.Kind != v2wire.Data {
				s.stats.rejected.Add(1)
				return errors.New("unauthorized placement frame kind")
			}
			if len(frames) > 0 && (f.Stream.Producer != frames[0].Stream.Producer || f.Stream.Lane != frames[0].Stream.Lane) {
				return errors.New("mixed placement connection lane")
			}
			frames = append(frames, f)
			batchBytes += 256 + len(f.Name) + len(f.Stream.Group) + len(f.Stream.Replica) + len(f.Stream.Store) + len(f.Stream.MapDigest) + len(f.Stream.Producer) + len(f.Stream.Lane)
			if batchBytes >= 256<<10 {
				break
			}
		}
		if len(frames) > 0 {
			var err error
			if sink, ok := s.cfg.Placement.(PlacedBatchSink); ok {
				err = sink.AcceptPlacedBatch(frames)
			} else {
				for _, f := range frames {
					if err = s.cfg.Placement.AcceptPlaced(f); err != nil {
						break
					}
				}
			}
			if err != nil {
				s.stats.rejected.Add(uint64(len(frames)))
				return err
			}
			s.stats.accepted.Add(uint64(len(frames)))
			for _, f := range frames {
				if err = v2wire.Write(writer, f.Acknowledgement(), s.cfg.PlacementKeys[f.Stream.Producer]); err != nil {
					return err
				}
			}
			if err = writer.Flush(); err != nil {
				return err
			}
		}
		if errors.Is(terminal, io.EOF) {
			return nil
		}
		if terminal != nil {
			return terminal
		}
	}
}
