package v2daemon

import (
	"errors"
	"net"

	"github.com/go-graphite/go-carbon/internal/v2recv"
)

// ReceiveTCP activates bounded relay decoding and serial producer ownership.
// Stop receiver intake with Wait before Store.Close. Layer 3 requires original
// application policies and is intentionally rejected by this raw-points adapter.
func (s *Store) ReceiveTCP(l net.Listener, cfg v2recv.Config) (*v2recv.Server, error) {
	if s.cfg.Layer != 1 && s.cfg.Layer != 2 {
		return nil, errors.New("raw TCP adapter requires named layer 1 or journaled layer 2")
	}
	owners := make([]v2recv.Owner, len(s.Producers))
	for i, p := range s.Producers {
		owners[i] = p
	}
	return v2recv.Start(l, owners, cfg)
}
