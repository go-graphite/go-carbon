package v2daemon

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"

	"github.com/go-graphite/go-carbon/internal/partlog"
)

// verifyJournal is a fixture oracle, outside benchmark accounting. It decodes
// every stream after close and compares every tuple in original append order.
// This is not a production restart or replay implementation.
func verifyJournal(root string, f Fixture, names []string, policies [][]byte) error {
	return parallel(f.Producers, func(p int) (result error) {
		dir := filepath.Join(root, "placement-group-00000001", "journal", fmt.Sprintf("producer-%03d", p))
		catalog, err := partlog.OpenNameCatalog(filepath.Join(dir, "catalog"))
		if err != nil {
			return err
		}
		defer func() { result = errors.Join(result, catalog.Close()) }()
		decoder := partlog.CatalogDecoder{Catalog: catalog}
		defer decoder.Close()
		file, err := os.Open(filepath.Join(dir, "segment.pdc4"))
		if err != nil {
			return err
		}
		defer func() { result = errors.Join(result, file.Close()) }()
		width := 0
		if p < f.Names {
			width = (f.Names-1-p)/f.Producers + 1
		}
		var ordinal int
		var record []byte
		for {
			var h [4]byte
			_, err = io.ReadFull(file, h[:])
			if err == io.EOF {
				break
			}
			if err != nil {
				return err
			}
			size := int(binary.LittleEndian.Uint32(h[:]))
			if size < 1 || size > 8<<20 {
				return errors.New("invalid fixture journal length")
			}
			if cap(record) < size {
				record = make([]byte, size)
			}
			record = record[:size]
			if _, err = io.ReadFull(file, record); err != nil {
				return err
			}
			if len(record) >= 4 && string(record[:4]) == string(applicationMagic) {
				batches, cursor, err := DecodeApplications(record, &decoder, catalog, p, f.Producers)
				if err != nil {
					return err
				}
				for _, batch := range batches {
					if width == 0 || ordinal >= width*f.Rounds || len(batch.Points) != 1 {
						return errors.New("application boundary changed")
					}
					round := ordinal / width
					id := p + (ordinal%width)*f.Producers
					point := fixturePoint(id, round)
					r := batch.Points[0]
					expectedPolicy := policies[id]
					if batch.ID != uint64(id+1) || batch.Order != uint64(ordinal+1) || batch.Clock != 1794700800+int64((round/4+1)*4) || !bytes.Equal(batch.Policy, expectedPolicy) || r.Time != point.Timestamp || r.Bits != math.Float64bits(point.Value) || r.Seq != uint64(round+1) {
						return fmt.Errorf("journal application changed producer%d ordinal%d", p, ordinal)
					}
					ordinal++
				}
				if cursor != uint64(ordinal) {
					return errors.New("journal cursor changed")
				}
				continue
			}
			frames, err := decoder.Decode(record)
			if err != nil {
				return err
			}
			for _, frame := range frames {
				if width == 0 || ordinal >= width*f.Rounds || len(frame.Points) != 1 {
					return errors.New("fixture journal count or apply boundary changed")
				}
				round := ordinal / width
				id := p + (ordinal%width)*f.Producers
				want := fixturePoint(id, round)
				point := frame.Points[0]
				if frame.Key != names[id] || len(frame.Definition) != 0 || frame.Clock != 1794700800+int64((round/4+1)*4) || point.Timestamp != want.Timestamp || math.Float64bits(point.Value) != math.Float64bits(want.Value) {
					return fmt.Errorf("fixture journal producer %d ordinal %d changed", p, ordinal)
				}
				ordinal++
			}
		}
		if ordinal != width*f.Rounds {
			return fmt.Errorf("fixture journal producer %d count %d want %d", p, ordinal, width*f.Rounds)
		}
		return nil
	})
}
