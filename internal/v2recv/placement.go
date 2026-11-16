package v2recv

import (
	"errors"
	"github.com/go-graphite/go-carbon/internal/v2wire"
	"io"
	"net"
)

// PlacedSink returns only after both the original stamp/position and data are
// durable. Retries must be deduplicated against the recovered original journal.
// Implementations serialize calls to each serial storage producer.
type PlacedSink interface{ AcceptPlaced(v2wire.Frame) error }

func (s *Server) readPlaced(c net.Conn) error {
	for {
		f, err := v2wire.ReadKeys(countedReader{c, &s.stats.wire}, s.cfg.PlacementKeys)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if f.Kind != v2wire.Data {
			s.stats.rejected.Add(1)
			return errors.New("unauthorized placement producer or frame kind")
		}
		if err = s.cfg.Placement.AcceptPlaced(f); err != nil {
			s.stats.rejected.Add(1)
			return err
		}
		s.stats.accepted.Add(1)
		if err = v2wire.Write(c, f.Acknowledgement(), s.cfg.PlacementKeys[f.Stream.Producer]); err != nil {
			return err
		}
	}
}
