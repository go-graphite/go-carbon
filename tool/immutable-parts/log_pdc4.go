package main

// This is the numeric replay benchmark envelope, not daemon name admission.
// PDC4's key lane carries the exact numeric ID, its clock lane carries the
// original sequence bits, and its point carries timestamp and IEEE value bits.
// The identity-bound corpus.json remains the name/policy/apply-clock authority.
import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sync"

	"github.com/go-graphite/go-carbon/internal/partlog"
	"github.com/go-graphite/go-carbon/points"
)

type replayCatalog struct {
	// Only a single journal stream's catalog publication and lookup share this
	// mutex. It is never held by a storage owner or a render reader.
	mu      sync.Mutex
	catalog *partlog.NameCatalog
	encoder partlog.CatalogEncoder
	decoder partlog.CatalogDecoder
	frames  []partlog.DictionaryFrame
	points  []points.Point
}

var numericCatalogDefinition = []byte("numeric-row-v1:id/key,sequence/clock,timestamp+bits/point")

func openReplayCatalogs(logDir string, streams int, create bool) ([]*replayCatalog, error) {
	root := filepath.Join(logDir, "numeric-pdc4")
	if !create {
		if _, err := os.Stat(root); errors.Is(err, os.ErrNotExist) {
			return nil, nil
		} else if err != nil {
			return nil, err
		}
	}
	cs := make([]*replayCatalog, streams)
	for p := range cs {
		c, err := partlog.OpenNameCatalog(filepath.Join(root, fmt.Sprintf("stream-%04d", p)))
		if err != nil {
			return nil, errors.Join(err, closeReplayCatalogs(cs))
		}
		cs[p] = &replayCatalog{catalog: c}
		// An independent epoch in every record permits seeking any durable
		// cursor and trimming earlier segments without predictor pre-roll.
		cs[p].encoder = partlog.CatalogEncoder{Catalog: c, Independent: true, SparseClocks: true}
		cs[p].decoder = partlog.CatalogDecoder{Catalog: c}
	}
	return cs, nil
}

func closeReplayCatalogs(cs []*replayCatalog) error {
	var err error
	for _, c := range cs {
		if c == nil {
			continue
		}
		c.encoder.Close()
		c.decoder.Close()
		err = errors.Join(err, c.catalog.Close())
	}
	return err
}

func (c *replayCatalog) encode(rows []row) ([]byte, error) {
	if len(rows) == 0 {
		return nil, os.ErrInvalid
	}
	if cap(c.frames) < len(rows) {
		c.frames = make([]partlog.DictionaryFrame, len(rows))
		c.points = make([]points.Point, len(rows))
	}
	c.frames = c.frames[:len(rows)]
	c.points = c.points[:len(rows)]
	for i, r := range rows {
		var key [8]byte
		binary.LittleEndian.PutUint64(key[:], r.ID)
		c.points[i] = points.Point{Timestamp: r.Time, Value: math.Float64frombits(r.Bits)}
		c.frames[i] = partlog.DictionaryFrame{Key: string(key[:]), Definition: numericCatalogDefinition, Clock: int64(r.Seq), Points: c.points[i : i+1]}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	// Definitions become durable BEFORE the referencing journal append.
	return c.encoder.EncodeDurable(c.frames)
}

func (c *replayCatalog) decode(rows []row, payload []byte) ([]row, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	frames, err := c.decoder.Decode(payload)
	if err != nil {
		return nil, err
	}
	if cap(rows) < len(frames) {
		rows = make([]row, len(frames))
	} else {
		rows = rows[:len(frames)]
	}
	for i, f := range frames {
		if len(f.Key) != 8 || string(f.Definition) != string(numericCatalogDefinition) || len(f.Points) != 1 {
			return nil, os.ErrInvalid
		}
		rows[i] = row{ID: binary.LittleEndian.Uint64([]byte(f.Key)), Time: f.Points[0].Timestamp, Bits: math.Float64bits(f.Points[0].Value), Seq: uint64(f.Clock)}
	}
	return rows, nil
}
