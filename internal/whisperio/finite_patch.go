package whisperio

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"strconv"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

// PlanFinitePatch supports the classic continuation image used by flattened
// native transfers. Compressed/sidecar images are explicitly unsupported.
func (c *ContinuationCodec) PlanFinitePatch(ctx context.Context, kv store.ContinuationIO, p store.FinitePatch) (store.FinitePatchPlan, error) {
	var plan store.FinitePatchPlan
	if err := p.Validate(); err != nil {
		return plan, err
	}
	anchors, err := c.PlanArchiveAnchors(ctx, kv)
	if err != nil {
		return plan, err
	}
	for _, a := range anchors.Archives {
		if a.Blocked != "" || a.Changed {
			return plan, fmt.Errorf("finite patch requires healthy archive anchors")
		}
	}
	// Sidecar precedence could hide a main-file correction. Do not publish
	// unless the complete namespace is the single classic main file.
	if err := kv.Visit([]byte("n/"), func(key, value []byte) error {
		if string(key) != "n/main.wsp" {
			return store.ErrFormat
		}
		return nil
	}); err != nil {
		return plan, err
	}
	fs := newContinuationFiles(kv, false)
	file, err := fs.OpenFile(fs.main(), os.O_RDONLY, 0)
	if err != nil {
		return plan, err
	}
	defer file.Close()
	f := file.(*continuationFile)
	if f.data.layout.Compressed || len(f.data.layout.Archives) == 0 || f.data.layout.Archives[0].Step != p.Step {
		return plan, store.ErrFormat
	}
	before, _ := strconv.ParseUint(p.BeforeBits, 16, 64)
	donor, _ := strconv.ParseUint(p.DonorBits, 16, 64)
	prefix := continuationBlockPrefix(f.data.id, 'p', 0, 0)
	found := 0
	err = kv.Visit(prefix, func(key, value []byte) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if len(key) != len(prefix)+4 {
			return store.ErrFormat
		}
		index := int(binary.BigEndian.Uint32(key[len(prefix):]))
		var block store.PointBlock
		if err := store.DecodePointBlock(value, &block); err != nil {
			return err
		}
		for i, point := range block.Points {
			if block.Present[i/64]&(uint64(1)<<uint(i%64)) == 0 || point.Timestamp != p.Timestamp {
				continue
			}
			found++
			if found != 1 || index*128+i >= f.data.layout.Archives[0].Count {
				return store.ErrConflict
			}
			bits := math.Float64bits(point.Value)
			if bits == donor {
				plan.Noop = true
				continue
			}
			if bits != before {
				return store.ErrConflict
			}
			block.Points[i].Value = math.Float64frombits(donor)
			plan.Writes = append(plan.Writes, store.RelayoutKV{Key: append([]byte(nil), key...), Value: store.EncodePointBlock(&block)})
		}
		return nil
	})
	if err == nil && found != 1 {
		err = store.ErrNotFound
	}
	return plan, err
}
