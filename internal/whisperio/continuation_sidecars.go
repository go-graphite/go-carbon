package whisperio

import (
	"context"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	whisper "github.com/go-graphite/go-whisper"
)

var errSidecarFirst = errors.New("first sidecar group")

func sidecarGroupStats(block *store.PointBlock) (count, oldest uint64) {
	for i, p := range block.Points {
		if block.Present[i/64]&(uint64(1)<<uint(i%64)) != 0 {
			count++
			if oldest == 0 || uint64(p.Timestamp) < oldest {
				oldest = uint64(p.Timestamp)
			}
		}
	}
	return
}

// A sidecar keeps a count and an ordered index of group minima. Updating one
// slot touches its group only; obtaining the oldest timestamp is one seek.
func (f *continuationFile) indexSidecarGroup(key []byte, block *store.PointBlock) error {
	summary := append(continuationFilePrefix(f.data.id), 'q')
	summary = append(summary, key[10:]...)
	old, err := f.fs.kv.Get(summary)
	if err != nil && !errors.Is(err, store.ErrNotFound) {
		return err
	}
	prefix := append(continuationFilePrefix(f.data.id), 'r')
	if len(old) != 0 {
		if len(old) != 16 {
			return errors.New("invalid sidecar group summary")
		}
		previous := binary.BigEndian.Uint64(old)
		if previous > *f.data.sidecarCount {
			return errors.New("sidecar count is smaller than its group")
		}
		*f.data.sidecarCount -= previous
		index := binary.BigEndian.AppendUint64(append([]byte(nil), prefix...), binary.BigEndian.Uint64(old[8:]))
		if err := f.fs.kv.Delete(append(index, key[10:]...)); err != nil {
			return err
		}
	}
	count, oldest := sidecarGroupStats(block)
	if count == 0 {
		return f.fs.kv.Delete(summary)
	}
	*f.data.sidecarCount += count
	value := binary.BigEndian.AppendUint64(nil, count)
	value = binary.BigEndian.AppendUint64(value, oldest)
	if err := f.fs.kv.Set(summary, value); err != nil {
		return err
	}
	index := binary.BigEndian.AppendUint64(prefix, oldest)
	return f.fs.kv.Set(append(index, key[10:]...), nil)
}

func (f *continuationFile) initSidecarIndex() error {
	if f.data.sidecarCount != nil {
		return nil
	}
	key := append(continuationFilePrefix(f.data.id), 's')
	value, err := f.fs.kv.Get(key)
	if err != nil && !errors.Is(err, store.ErrNotFound) {
		return err
	}
	f.data.sidecarCount = new(uint64)
	if len(value) != 0 {
		if len(value) != 8 {
			return errors.New("invalid sidecar count")
		}
		*f.data.sidecarCount = binary.BigEndian.Uint64(value)
		return nil
	}
	// Older stores are indexed once, either by the admin scan or first mutation.
	return f.fs.kv.Visit(append(continuationFilePrefix(f.data.id), 'p'), func(key, value []byte) error {
		var block store.PointBlock
		if err := store.DecodePointBlock(value, &block); err != nil {
			return err
		}
		return f.indexSidecarGroup(key, &block)
	})
}

func (fs *continuationFiles) flushSidecarSummary() error {
	if !fs.sidecarDirty {
		return nil
	}
	summary := store.SidecarSummary{}
	path := whisper.OutOfOrderSidecarPath(fs.main())
	d, err := fs.load(filepath.Base(path))
	if err == nil {
		f := &continuationFile{fs: fs, data: d, sidecar: true}
		if err := f.initSidecarIndex(); err != nil {
			return err
		}
		if err := fs.kv.Set(append(continuationFilePrefix(d.id), 's'), binary.BigEndian.AppendUint64(nil, *d.sidecarCount)); err != nil {
			return err
		}
		summary.Present, summary.Points = true, *d.sidecarCount
		prefix := append(continuationFilePrefix(d.id), 'r')
		err := fs.kv.Visit(prefix, func(key, value []byte) error {
			if len(key) < len(prefix)+8 {
				return errors.New("invalid sidecar oldest index")
			}
			summary.Oldest = int64(binary.BigEndian.Uint64(key[len(prefix):]))
			return errSidecarFirst
		})
		if err != nil && !errors.Is(err, errSidecarFirst) {
			return err
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return fs.kv.Set(store.SidecarSummaryKey, summary.Encode())
}

// InspectSidecar indexes legacy sidecars once and returns their physical slot
// inventory, including expired records. It never changes their metric values.
func (c *ContinuationCodec) InspectSidecar(kv store.ContinuationIO) (store.SidecarSummary, error) {
	fs := newContinuationFiles(kv, false)
	if _, err := fs.load(filepath.Base(whisper.OutOfOrderSidecarPath(fs.main()))); errors.Is(err, os.ErrNotExist) {
		return store.SidecarSummary{}, nil
	} else if err != nil {
		return store.SidecarSummary{}, err
	}
	fs.sidecarDirty = true
	if err := fs.flushSidecarSummary(); err != nil {
		return store.SidecarSummary{}, err
	}
	value, err := kv.Get(store.SidecarSummaryKey)
	if err != nil {
		return store.SidecarSummary{}, err
	}
	return store.DecodeSidecarSummary(value)
}

func checkSidecarRemoved(fs *continuationFiles) error {
	if _, err := fs.Stat(whisper.OutOfOrderSidecarPath(fs.main())); !errors.Is(err, os.ErrNotExist) {
		return errors.Join(errors.New("continuation sidecar fold left its sidecar"), err)
	}
	return nil
}

func (c *ContinuationCodec) FoldSidecar(ctx context.Context, kv store.ContinuationIO, clock func() time.Time) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	fs := newContinuationFiles(kv, false).withClock(clock)
	// Keep the same complete history as conversion's offline merge oracle.
	w, err := whisper.OpenWithOptions(fs.main(), continuationOptions(fs, func() time.Time { return importFoldTime }))
	if err != nil {
		return "", err
	}
	result, err := foldSidecar(w, importFoldTime)
	if err = errors.Join(err, w.Close()); err != nil {
		return result.String(), err
	}
	if err := ctx.Err(); err != nil {
		return result.String(), err
	}
	if err := checkSidecarRemoved(fs); err != nil {
		return result.String(), err
	}
	return result.String(), fs.flush()
}
