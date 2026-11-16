package v2daemon

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"syscall"

	"github.com/go-graphite/go-carbon/internal/partlog"
	"github.com/go-graphite/go-carbon/internal/v2replay"
)

type restartGeometry struct{ Version, Layer, Producers, Shards int }

// lockRoot prevents two writable opens, including before core has opened its DBs.
// Geometry is independent of tuning knobs and survives a process crash.
func lockRoot(root string, c Config) (*os.File, bool, error) {
	f, err := os.OpenFile(filepath.Join(root, "restart.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, false, err
	}
	fail := func(err error) (*os.File, bool, error) { _ = f.Close(); return nil, false, err }
	if err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return fail(err)
	}
	path := filepath.Join(root, "restart.json")
	want := restartGeometry{1, c.Layer, c.Producers, c.Shards}
	b, err := os.ReadFile(path)
	if err == nil {
		var got restartGeometry
		if json.Unmarshal(b, &got) != nil || got != want {
			return fail(errors.New("restart geometry mismatch"))
		}
		if c.Layer < 2 {
			return fail(errors.New("restart requires durable journal layer"))
		}
		return f, true, nil
	}
	if !os.IsNotExist(err) {
		return fail(err)
	}
	// An older experiment has no restart contract or exact L2 sequences.
	if _, err = os.Stat(filepath.Join(root, "experiment.lock")); !os.IsNotExist(err) {
		return fail(errors.New("legacy experiment has no lossless restart contract"))
	}
	b, _ = json.Marshal(want)
	out, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return fail(err)
	}
	_, err = out.Write(b)
	err = errors.Join(err, out.Sync(), out.Close(), syncDir(root))
	if err != nil {
		return fail(err)
	}
	return f, false, nil
}

// recoverProducer ports the earlier loggedCore replay loop: ordered per-stream
// decode -> the same AppendBatches queue -> synchronous final seal. Owners alone
// decide which packets were already committed. Every segment resets predictors.
func (p *Producer) recoverProducer(dir string) error {
	paths, err := filepath.Glob(filepath.Join(dir, "segment*.pdc4"))
	if err != nil {
		return err
	}
	// The original segment sorts first; suffixes use fixed-width generations.
	slices.Sort(paths)
	original := filepath.Join(dir, "segment.pdc4")
	if i := slices.Index(paths, original); i >= 0 {
		paths = append([]string{original}, append(paths[:i], paths[i+1:]...)...)
	}
	for i, path := range paths {
		decoder := partlog.CatalogDecoder{Catalog: p.catalog}
		_, err = v2replay.Scan(path, i == len(paths)-1, func(record []byte) error {
			inner, stamp, err := decodePlacementEnvelope(record)
			if err != nil {
				return err
			}
			if p.placement != nil {
				if stamp == nil {
					return errors.New("placement stamp missing from retained journal")
				}
				if err = p.placement.Replay(*stamp); err != nil {
					return fmt.Errorf("journal placement authority: %w", err)
				}
			} else if stamp != nil {
				return errors.New("placed journal requires placement authority")
			}
			if stamp != nil && stamp.RelaySequence != 0 {
				if err := p.rememberRelay(*stamp); err != nil {
					return err
				}
			}
			batches, cursor, err := DecodeApplications(inner, &decoder, p.catalog, p.index, p.count)
			if err != nil {
				return err
			}
			if p.placement != nil {
				names := make([]string, len(batches))
				for j, batch := range batches {
					id := (batch.ID-uint64(p.index)-1)/uint64(p.count) + 1
					name, _, ok := p.catalog.Lookup(id)
					if !ok {
						return errors.New("journal placement name missing")
					}
					names[j] = name
				}
				if err = p.placement.CheckNames(names); err != nil {
					return fmt.Errorf("journal placement names: %w", err)
				}
			}
			var n uint64
			for _, b := range batches {
				n += uint64(len(b.Points))
			}
			// Packet boundaries are durable cursor boundaries. Never split an original
			// application to accommodate a partially committed owner vector.
			if cursor <= p.cursor {
				return errors.New("repeated or regressing journal cursor")
			}
			if cursor-p.cursor != n {
				return errors.New("journal cursor gap")
			}
			for _, b := range batches {
				if b.Order <= p.applyOrder {
					return errors.New("journal application order regressed")
				}
				p.applyOrder = b.Order
			}
			if err = p.engine.AppendBatchesPipelined(p.index, batches, cursor, false); err != nil {
				return err
			}
			p.cursor = cursor
			return nil
		})
		decoder.Close()
		if err != nil {
			return err
		}
	}
	// A frontier beyond the retained journal would silently reuse cursors.
	for _, frontier := range p.engine.DurableFrontiers() {
		if frontier[p.index] > p.cursor {
			return errors.New("core frontier exceeds retained journal")
		}
	}
	if p.cursor > 0 {
		if err = p.engine.AppendBatches(p.index, nil, p.cursor, true); err != nil {
			return err
		}
	}
	name := "segment.pdc4"
	if len(paths) > 0 {
		name = fmt.Sprintf("segment-%020d.pdc4", len(paths))
	}
	p.journal, err = os.OpenFile(filepath.Join(dir, name), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	if err = syncDir(dir); err != nil {
		return err
	}
	p.writer = bufio.NewWriterSize(p.journal, 1<<20)
	return nil
}
