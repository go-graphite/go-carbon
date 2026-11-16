package partsops

import (
	"encoding/binary"
	"errors"
	"time"

	"github.com/go-graphite/go-carbon/internal/partsformat"
)

// Activity is the changing part of a metric catalogue entry. Its canonical name
// is carried by the scoped key; schema and policy remain in the metric entry.
type Activity struct {
	Name                 string
	Generation, Revision uint64
	LastUpdate           time.Time
}

func encodeActivity(scope Scope, a Activity) ([]byte, error) {
	if a.Name == "" || len(a.Name) > partsformat.MaxNameBytes || partsformat.Route(partsformat.ID(a.Name)) != scope.VShard || a.Generation == 0 || a.Revision == 0 {
		return nil, errors.New("invalid metric activity")
	}
	b := make([]byte, 30)
	b[0], b[1] = 'A', 1
	binary.BigEndian.PutUint64(b[2:], a.Generation)
	binary.BigEndian.PutUint64(b[10:], a.Revision)
	binary.BigEndian.PutUint64(b[18:], uint64(a.LastUpdate.Unix()))
	binary.BigEndian.PutUint32(b[26:], uint32(a.LastUpdate.Nanosecond()))
	return b, nil
}

func decodeActivity(name string, b []byte) (Activity, error) {
	if len(b) != 30 || b[0] != 'A' || b[1] != 1 || binary.BigEndian.Uint32(b[26:]) >= 1e9 {
		return Activity{}, errors.New("invalid metric activity encoding")
	}
	a := Activity{Name: name, Generation: binary.BigEndian.Uint64(b[2:]), Revision: binary.BigEndian.Uint64(b[10:]), LastUpdate: time.Unix(int64(binary.BigEndian.Uint64(b[18:])), int64(binary.BigEndian.Uint32(b[26:])))}
	if a.Generation == 0 || a.Revision == 0 {
		return Activity{}, errors.New("invalid metric activity frontier")
	}
	return a, nil
}
