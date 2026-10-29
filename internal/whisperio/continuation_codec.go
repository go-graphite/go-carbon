package whisperio

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"os"
	"reflect"
	"sort"
	"sync/atomic"
	"syscall"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/stagetime"
	whisper "github.com/go-graphite/go-whisper"
)

// ContinuationCodec reuses the source writer's state machine over shared exact
// blocks. It deliberately preserves buffers, main/OOO precedence and physical
// block ownership, which a flattened archive snapshot cannot reconstruct.
type ContinuationCodec struct {
	// ReuseBlockKeys skips deleting a rewritten block's old groups when the
	// new groups use the same keys (v6 layout, or the same time bucket): they
	// are overwritten as the block refills and reads stop at the block's count.
	// Leftover groups past the count stay until the block refills or the file
	// is removed, at most one block per archive.
	ReuseBlockKeys bool
	// DirectLateWrites stores late base points in their blocks instead of an
	// out-of-order sidecar, recomputing the coarse windows they change in place,
	// and folds existing and fallback sidecars before committing. In-place
	// failures use the ordinary full rewrite in the same atomic store batch.
	DirectLateWrites bool
	// keepImportedSidecars reproduces legacy imports in oracle tests.
	keepImportedSidecars bool
}

// foldsImportedSidecars is disabled only by legacy-oracle tests.
func (c *ContinuationCodec) foldsImportedSidecars() bool {
	return !c.keepImportedSidecars
}

func continuationOptions(fs *continuationFiles, clock func() time.Time) *whisper.Options {
	options := &whisper.Options{FileSystem: fs, Now: clock, Compressed: true, OutOfOrder: true}
	if fs.readOnly {
		readOnly := os.O_RDONLY
		options.OpenFileFlag = &readOnly
	}
	return options
}

func (c *ContinuationCodec) Create(ctx context.Context, kv store.ContinuationIO, m store.Metadata, clock func() time.Time) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	fs := newContinuationFiles(kv, false).withClock(clock)
	retentions := make([]whisper.Retention, len(m.Retentions))
	for i, r := range m.Retentions {
		retentions[i] = whisper.NewRetention(r.Step, r.Count)
	}
	w, err := whisper.CreateWithOptions(fs.main(), whisper.NewRetentionsNoPointer(retentions), whisper.AggregationMethod(m.AggregationMethod), m.XFilesFactor, continuationOptions(fs, clock))
	if err != nil {
		return err
	}
	if err := w.Close(); err != nil {
		return err
	}
	return fs.flush()
}

// Drop deletes the metric generation's time-partitioned blocks. The store
// deletes its metric-prefixed keys, descriptors included, in the same batch.
func (c *ContinuationCodec) Drop(ctx context.Context, kv store.ContinuationIO) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	buckets, err := continuationBuckets(kv, []byte{'f'})
	if err != nil {
		return err
	}
	for _, bucket := range buckets {
		if err := kv.DeletePrefix(store.ContinuationBucketKey(bucket, nil)); err != nil {
			return err
		}
	}
	return nil
}

var _ store.ContinuationDropper = (*ContinuationCodec)(nil)

func sourceContinuationFile(ctx context.Context, path string) (*whisper.Whisper, error) {
	return whisper.OpenWithOptions(path, conversionSourceOptions(ctx))
}

func continuationPolicy(w *whisper.Whisper, m store.Metadata) error {
	if whisper.AggregationMethod(m.AggregationMethod) != w.AggregationMethod() || m.XFilesFactor != w.XFilesFactor() {
		return errors.New("continuation source aggregation differs")
	}
	retentions := w.Retentions()
	if len(retentions) != len(m.Retentions) {
		return errors.New("continuation source archive count differs")
	}
	for i, r := range retentions {
		if r.SecondsPerPoint() != m.Retentions[i].Step || r.NumberOfPoints() != m.Retentions[i].Count {
			return errors.New("continuation source retention differs")
		}
	}
	return nil
}

// Import converts a fenced source pair. It folds the
// sidecar before publishing any keys, using a full rewrite when an in-place
// fold cannot represent the merge. A failed fold fails conversion.
func (c *ContinuationCodec) Import(ctx context.Context, kv store.ContinuationIO, path string, m store.Metadata, clock func() time.Time) error {
	w, err := sourceContinuationFile(ctx, path)
	if err != nil {
		return err
	}
	defer w.Close()
	if err := continuationPolicy(w, m); err != nil {
		return err
	}
	side, err := sourceContinuationSidecar(ctx, w, m)
	if err != nil {
		return err
	}
	if side != nil {
		defer side.Close()
	}
	if side == nil || !c.foldsImportedSidecars() {
		fs := newContinuationFiles(kv, false).withClock(clock)
		if err := importContinuationFiles(ctx, fs, w, side); err != nil {
			return err
		}
		return fs.flush()
	}
	staged, result, err := importFoldedContinuation(ctx, kv, w, side, clock)
	if err != nil {
		countImportSidecarFold(importFoldFailed)
		return err
	}
	if err := staged.commit(); err != nil {
		return err
	}
	countImportSidecarFold(result)
	return nil
}

// sourceContinuationSidecar opens the source's sidecar, or returns nil when it
// has none.
func sourceContinuationSidecar(ctx context.Context, w *whisper.Whisper, m store.Metadata) (*whisper.Whisper, error) {
	sidecar := w.OutOfOrderPath()
	if sidecar == "" {
		return nil, nil
	}
	side, err := sourceContinuationFile(ctx, sidecar)
	if err != nil {
		return nil, err
	}
	if side.IsCompressed() {
		side.Close()
		return nil, errors.New("compressed continuation sidecar")
	}
	if err := continuationPolicy(side, m); err != nil {
		side.Close()
		return nil, err
	}
	return side, nil
}

// importContinuationFiles imports the main file and the sidecar, when side is
// not nil, under their continuation names.
func importContinuationFiles(ctx context.Context, fs *continuationFiles, w, side *whisper.Whisper) error {
	if err := importContinuationFile(ctx, fs, fs.main(), w); err != nil {
		return err
	}
	if side == nil {
		return nil
	}
	return importContinuationFile(ctx, fs, whisper.OutOfOrderSidecarPath(fs.main()), side)
}

// importFoldTime is the time an import reads the sidecar it folds as of: the
// Unix epoch, as the conversion's source snapshot merges it
// (whisper.OfflineMergeOutOfOrderSnapshotIn), so the fold keeps every
// historical sidecar point the snapshot does and Verify can repeat it later.
var importFoldTime = time.Unix(0, 0)

// importFoldFailed is the import fold result counted when the fold failed.
const importFoldFailed = whisper.InPlaceResult(255)

// importFoldedContinuation stages the import and fold together. No keys reach
// the caller until the entire fold succeeds.
func importFoldedContinuation(ctx context.Context, kv store.ContinuationIO, w, side *whisper.Whisper, clock func() time.Time) (*continuationOverlay, whisper.InPlaceResult, error) {
	imported := newContinuationOverlay(kv)
	fs := newContinuationFiles(imported, false).withClock(clock)
	if err := importContinuationFiles(ctx, fs, w, side); err != nil {
		return nil, 0, err
	}
	if err := fs.flush(); err != nil {
		return nil, 0, err
	}
	folded := newContinuationOverlay(imported)
	result, err := foldContinuationSidecar(folded, clock)
	if ctxErr := ctx.Err(); ctxErr != nil {
		return nil, 0, ctxErr
	}
	if err != nil {
		return nil, result, err
	}
	if err := folded.commit(); err != nil {
		return nil, 0, err
	}
	return imported, result, nil
}

// foldContinuationSidecar preserves the offline conversion's retention horizon.
func foldContinuationSidecar(kv store.ContinuationIO, clock func() time.Time) (whisper.InPlaceResult, error) {
	fs := newContinuationFiles(kv, false).withClock(clock)
	if err := fs.prefetchNames(); err != nil {
		return importFoldFailed, err
	}
	w, err := whisper.OpenWithOptions(fs.main(), continuationOptions(fs, func() time.Time { return importFoldTime }))
	if err != nil {
		return importFoldFailed, err
	}
	result, err := foldSidecar(w, importFoldTime)
	if err = errors.Join(err, w.Close()); err != nil {
		return result, err
	}
	if err := fs.flush(); err != nil {
		return result, err
	}
	return result, checkSidecarRemoved(fs)
}

// foldSidecar uses the same merge semantics for conversion, maintenance and
// direct-write fallbacks. Its caller discards the batch on any error.
func foldSidecar(w *whisper.Whisper, now time.Time) (whisper.InPlaceResult, error) {
	result, folded, err := w.FoldOutOfOrderInPlaceAt(now)
	if err == nil && !folded {
		err = w.MergeOutOfOrder()
	}
	if err == nil && w.OutOfOrderPath() != "" {
		err = errors.New("sidecar remains after fold")
	}
	return result, err
}

// importSidecarFolds counts imports of a source with a sidecar by the fold's
// result; importSidecarFoldErrors those whose fold
// failed. Non-in-place results identify completed full rewrites.
var (
	importSidecarFolds      [16]atomic.Uint64
	importSidecarFoldErrors atomic.Uint64
)

func countImportSidecarFold(result whisper.InPlaceResult) {
	if result == importFoldFailed {
		importSidecarFoldErrors.Add(1)
		return
	}
	if int(result) < len(importSidecarFolds) {
		importSidecarFolds[result].Add(1)
	}
}

// ImportSidecarFolds returns the process-wide number of converted files whose
// sidecar Import folded, by in-place decision or full-rewrite reason.
func ImportSidecarFolds(result whisper.InPlaceResult) uint64 {
	if int(result) >= len(importSidecarFolds) {
		return 0
	}
	return importSidecarFolds[result].Load()
}

// ImportSidecarFoldErrors returns the process-wide number of converted files
// whose sidecar fold failed and prevented conversion.
func ImportSidecarFoldErrors() uint64 { return importSidecarFoldErrors.Load() }

func importContinuationFile(ctx context.Context, fs *continuationFiles, name string, source *whisper.Whisper) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	out, err := fs.OpenFile(name, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
	if err != nil {
		return err
	}
	defer out.Close()
	f := out.(*continuationFile)
	layout := source.StorageLayout()
	if err := f.ConfigureStorage(layout); err != nil {
		return err
	}
	header := make([]byte, layout.HeaderSize)
	if _, err := source.StorageFile().ReadAt(header, 0); err != nil {
		return err
	}
	if _, err := f.WriteAt(header, 0); err != nil {
		return err
	}
	if layout.Compressed {
		physical, err := source.ContinuationLayout()
		if err != nil {
			return err
		}
		// Each archive appends only to its newest block; the rest of the
		// history goes to the history bucket.
		tails := make(map[int]int)
		for i, block := range physical.Blocks {
			if t, ok := tails[block.Archive]; block.Count > 0 && (!ok || block.Last > physical.Blocks[t].Last) {
				tails[block.Archive] = i
			}
		}
		var scratch whisper.ContinuationBlockBuffer
		for i, block := range physical.Blocks {
			if err := ctx.Err(); err != nil {
				return err
			}
			points, err := source.ContinuationBlockPointsBuffered(block, &scratch)
			if err != nil {
				return err
			}
			fresh := fs.newBlock()
			if t, ok := tails[block.Archive]; !ok || t != i {
				fresh = fs.historyBlock()
			}
			if err := f.appendStorageBlock(block, 0, points, fresh); err != nil {
				return err
			}
		}
		return nil
	}
	for ai, a := range layout.Archives {
		groups := make(map[int]*store.PointBlock)
		err := source.VisitClassicArchiveSlots(ai, func(slot int, p whisper.TimeSeriesPoint) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			group := groups[slot/128]
			if group == nil {
				group = &store.PointBlock{}
				groups[slot/128] = group
			}
			within := slot % 128
			group.Present[within/64] |= uint64(1) << uint(within%64)
			group.Points[within] = store.Point{Timestamp: int64(p.Time), Value: p.Value}
			f.allocateClassicPages(a.Offset+slot*12, a.Offset+slot*12+12)
			return nil
		})
		if err != nil {
			return err
		}
		for index, group := range groups {
			if err := f.setPointBlock(continuationBodyKey(f.data.id, 'p', ai, 0, index), group); err != nil {
				return err
			}
		}
	}
	if err := f.Truncate(int64(layout.Size)); err != nil {
		return err
	}
	return nil
}

func (c *ContinuationCodec) Verify(ctx context.Context, kv store.ContinuationIO, path string, m store.Metadata) error {
	source, err := sourceContinuationFile(ctx, path)
	if err != nil {
		return err
	}
	defer source.Close()
	if err := continuationPolicy(source, m); err != nil {
		return err
	}
	fs := newContinuationFiles(kv, true)
	sidecarName := whisper.OutOfOrderSidecarPath(fs.main())
	if sidecar := source.OutOfOrderPath(); sidecar != "" {
		side, err := sourceContinuationFile(ctx, sidecar)
		if err != nil {
			return err
		}
		defer side.Close()
		if _, err := fs.Stat(sidecarName); errors.Is(err, os.ErrNotExist) {
			// Import folded the sidecar (or the store lost it).
			return verifyFoldedContinuation(ctx, fs, source, side, m)
		} else if err != nil {
			return err
		}
		if err := verifyContinuationFile(ctx, fs, fs.main(), source); err != nil {
			return err
		}
		return verifyContinuationFile(ctx, fs, sidecarName, side)
	}
	if err := verifyContinuationFile(ctx, fs, fs.main(), source); err != nil {
		return err
	}
	if _, err := fs.Stat(sidecarName); !errors.Is(err, os.ErrNotExist) {
		return errors.New("unexpected continuation sidecar")
	}
	return nil
}

// VerifySnapshot decodes stored blocks independently of Import, then certifies
// an unchanged source view or reuses the stored decodes for a fresh view. A
// sidecar needs canonical folding; keep its existing independent paths.
func (c *ContinuationCodec) VerifySnapshot(ctx context.Context, kv store.ContinuationIO, path string, m store.Metadata, clock func() time.Time) (store.Snapshot, error) {
	source, err := sourceContinuationFile(ctx, path)
	if err != nil {
		return store.Snapshot{}, err
	}
	defer source.Close()
	if !source.IsCompressed() || source.OutOfOrderPath() != "" {
		if err := c.Verify(ctx, kv, path, m); err != nil {
			return store.Snapshot{}, err
		}
		return c.Snapshot(ctx, kv, m, clock)
	}
	if err := continuationPolicy(source, m); err != nil {
		return store.Snapshot{}, err
	}
	fs := newContinuationFiles(kv, true)
	cache, cached := ctx.Value(conversionSourceKey{}).(*conversionSource)
	if !cached || cache.view == nil {
		fs.verifiedBlocks = make(map[verifiedBlockKey][]whisper.TimeSeriesPoint)
	}
	if err := verifyContinuationFile(ctx, fs, fs.main(), source); err != nil {
		return store.Snapshot{}, err
	}
	if _, err := fs.Stat(whisper.OutOfOrderSidecarPath(fs.main())); !errors.Is(err, os.ErrNotExist) {
		return store.Snapshot{}, errors.New("unexpected continuation sidecar")
	}
	// Physical equality proves the same canonical visible view: layout,
	// complete header (buffers and encoder state included), and every physical
	// point were compared against independently decoded Pebble values above.
	// Only reuse a private source view whose header and every encoded block
	// were checked unchanged in this verification. No import output supplies
	// the stored side of the proof. OOO folds never take this path.
	if cached {
		physical, err := source.ContinuationLayout()
		if err != nil {
			return store.Snapshot{}, err
		}
		if result, ok := cache.verifiedView(path, physical.Header, m); ok {
			return result, nil
		}
	}
	w, err := whisper.OpenWithOptions(fs.main(), continuationOptions(fs, func() time.Time { return importFoldTime }))
	if err != nil {
		return store.Snapshot{}, err
	}
	defer w.Close()
	return continuationArchiveSnapshot(ctx, w, m)
}

// verifyFoldedContinuation checks that the stored main file, which has no
// sidecar, is what Import stores when it folds source's sidecar: it repeats
// the import and the fold in memory and compares the physical state with the
// stored one, as Verify compares an unfolded import with its source.
func verifyFoldedContinuation(ctx context.Context, fs *continuationFiles, source, side *whisper.Whisper, m store.Metadata) error {
	if err := continuationPolicy(side, m); err != nil {
		return err
	}
	if side.IsCompressed() {
		return errors.New("compressed continuation sidecar")
	}
	scratch := newContinuationOverlay(emptyContinuationIO{})
	sfs := newContinuationFiles(scratch, false)
	if err := importContinuationFiles(ctx, sfs, source, side); err != nil {
		return err
	}
	if err := sfs.flush(); err != nil {
		return err
	}
	_, err := foldContinuationSidecar(scratch, nil)
	if err != nil {
		return fmt.Errorf("stored continuation has no sidecar, and folding its source's failed: %w", err)
	}
	want := newContinuationFiles(scratch, true)
	expected, err := whisper.OpenWithOptions(want.main(), continuationOptions(want, nil))
	if err != nil {
		return err
	}
	defer expected.Close()
	return verifyContinuationFile(ctx, fs, fs.main(), expected)
}

func verifyContinuationFile(ctx context.Context, fs *continuationFiles, name string, source *whisper.Whisper) error {
	in, err := fs.OpenFile(name, os.O_RDONLY, 0600)
	if err != nil {
		return err
	}
	defer in.Close()
	f := in.(*continuationFile)
	layout := source.StorageLayout()
	if !reflect.DeepEqual(layout, f.data.layout) {
		return fmt.Errorf("stored continuation layout differs for %s: source=%+v stored=%+v", name, layout, f.data.layout)
	}
	if layout.Compressed {
		// The layout reads the header through the source's storage, which
		// may itself be a continuation (see verifyFoldedContinuation).
		physical, err := source.ContinuationLayout()
		if err != nil {
			return err
		}
		if !bytes.Equal(physical.Header, f.data.header) {
			return errors.New("stored continuation header differs")
		}
		var scratch whisper.ContinuationBlockBuffer
		var got []whisper.TimeSeriesPoint
		for _, block := range physical.Blocks {
			if err := ctx.Err(); err != nil {
				return err
			}
			want, err := source.ContinuationBlockPointsBuffered(block, &scratch)
			if err != nil {
				return err
			}
			got, err = f.ReadStorageBlockInto(block, got)
			if err != nil {
				return err
			}
			if err := sameContinuationPoints(want, got); err != nil {
				return err
			}
		}
		return nil
	}
	header := make([]byte, layout.HeaderSize)
	if _, err := source.StorageFile().ReadAt(header, 0); err != nil {
		return err
	}
	if !bytes.Equal(header, f.data.header) {
		return errors.New("stored continuation header differs")
	}
	for ai, a := range layout.Archives {
		wantCount := 0
		groupIndex := -1
		var group *store.PointBlock
		err := source.VisitClassicArchiveSlots(ai, func(slot int, point whisper.TimeSeriesPoint) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			if groupIndex != slot/128 {
				var err error
				groupIndex = slot / 128
				// Read through the allocation, so verification also proves
				// the import allocated every stored slot.
				group, err = f.classicGroup(ai, a, groupIndex, false)
				if err != nil {
					return err
				}
			}
			within := slot % 128
			got := group.Points[within]
			if group.Present[within/64]&(uint64(1)<<uint(within%64)) == 0 || got.Timestamp != int64(point.Time) || math.Float64bits(got.Value) != math.Float64bits(point.Value) {
				return fmt.Errorf("classic continuation slot %d differs", slot)
			}
			wantCount++
			return nil
		})
		if err != nil {
			return err
		}
		gotCount := 0
		prefix := continuationBlockPrefix(f.data.id, 'p', ai, 0)
		if err := fs.kv.Visit(prefix, func(key, value []byte) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			if len(key) != 20 {
				return errors.New("invalid classic continuation point key")
			}
			index := int(binary.BigEndian.Uint32(key[16:]))
			var block store.PointBlock
			if err := store.DecodePointBlock(value, &block); err != nil {
				return err
			}
			for slot, p := range block.Points {
				if block.Present[slot/64]&(uint64(1)<<uint(slot%64)) != 0 {
					if index*128+slot >= a.Count || p.Timestamp <= 0 || p.Timestamp > math.MaxUint32 {
						return errors.New("invalid classic continuation point position")
					}
					gotCount++
				}
			}
			return nil
		}); err != nil {
			return err
		}
		if wantCount != gotCount {
			return errors.New("classic continuation physical point count differs")
		}
	}
	return nil
}

func sameContinuationPoints(want, got []whisper.TimeSeriesPoint) error {
	if len(want) != len(got) {
		return errors.New("continuation physical point count differs")
	}
	for i, point := range want {
		if point.Time != got[i].Time || math.Float64bits(point.Value) != math.Float64bits(got[i].Value) {
			return fmt.Errorf("continuation physical point %d differs", i)
		}
	}
	return nil
}

// invalidContinuationPoints counts input points dropped because their
// timestamps do not fit a 32-bit Whisper timestamp.
var invalidContinuationPoints atomic.Uint64

// InvalidContinuationPoints returns how many input points continuation updates
// dropped for timestamps outside the 32-bit Whisper range.
func InvalidContinuationPoints() uint64 { return invalidContinuationPoints.Load() }

func (c *ContinuationCodec) Update(ctx context.Context, kv store.ContinuationIO, m store.Metadata, input []store.Point, retention int, clock func() time.Time, maintenance *store.ContinuationMaintenance) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	var buffer *continuationPointBuffer
	if coalescePointWrites && len(m.Retentions) > 1 && len(input) >= 2*(m.Retentions[1].Step/m.Retentions[0].Step) {
		buffer = &continuationPointBuffer{ContinuationIO: kv}
		kv = buffer
	}
	timer := stagetime.FromContext(ctx)
	timer.Enter(stagetime.Load)
	fs := newContinuationFiles(kv, false).withClock(clock)
	fs.reuseBlockKeys = c.ReuseBlockKeys
	fs.timer = timer
	if err := fs.prefetchNames(); err != nil {
		return err
	}
	options := continuationOptions(fs, clock)
	options.OutOfOrderDirect = c.DirectLateWrites
	w, err := whisper.OpenWithOptions(fs.main(), options)
	if err != nil {
		return err
	}
	defer w.Close()
	timer.Enter(stagetime.Decode)
	points := make([]*whisper.TimeSeriesPoint, 0, len(input))
	for _, point := range input {
		// Whisper timestamps are 32-bit. Drop a point outside that range (often
		// milliseconds sent as seconds) instead of failing the update: the
		// persister requeues failed updates, so one such point would keep the
		// metric's valid points from ever being written.
		if point.Timestamp < 0 || point.Timestamp > math.MaxUint32 || point.Timestamp > int64(^uint(0)>>1) {
			invalidContinuationPoints.Add(1)
			continue
		}
		points = append(points, &whisper.TimeSeriesPoint{Time: int(point.Timestamp), Value: point.Value})
	}
	// An empty update may still request sidecar maintenance.
	if len(points) == 0 && maintenance == nil {
		return ctx.Err()
	}
	if len(points) > 0 {
		if err := w.UpdateManyForArchive(points, retention); err != nil {
			return err
		}
	}
	if c.DirectLateWrites && len(points) > 0 && w.OutOfOrderPath() != "" {
		if _, err := foldSidecar(w, fs.clock()); err != nil {
			return err
		}
	}
	if maintenance != nil {
		timer.Enter(stagetime.OOOMerge)
		if err := compactContinuation(w, fs, maintenance); err != nil {
			return err
		}
	}
	timer.Enter(stagetime.Encode)
	if err := w.Close(); err != nil {
		return err
	}
	if c.DirectLateWrites && len(points) > 0 {
		if err := checkSidecarRemoved(fs); err != nil {
			return err
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := fs.flush(); err != nil {
		return err
	}
	if buffer != nil {
		return buffer.flush(nil)
	}
	return nil
}

// earlySidecarFolds lets maintenance fold a due sidecar in place before the
// size threshold applies. Tests clear it to exercise sidecar slot collisions.
var earlySidecarFolds = true

func compactContinuation(w *whisper.Whisper, fs *continuationFiles, policy *store.ContinuationMaintenance) error {
	path := w.OutOfOrderPath()
	if path == "" || policy.Allow == nil {
		return nil
	}
	// Fold a due sidecar in place first, whatever its size and without the
	// merge rate limit: it touches only the blocks the merge changes, and a
	// small sidecar left below the threshold would otherwise stay until it
	// collides with ordinary input a retention later and replays the file.
	// "Due" needs an age rule; without one every sidecar would be due on
	// every update, and merges stay paced by Allow as before.
	options := whisper.OutOfOrderMergeOptions{MinPoints: policy.MinPoints, MaxPointAge: policy.MaxPointAge, RetentionMargin: policy.RetentionMargin}
	if earlySidecarFolds && (policy.MaxPointAge > 0 || policy.RetentionMargin > 0) {
		if folded, err := w.FoldOutOfOrderInPlace(options); err != nil || folded {
			if policy.Result != nil {
				policy.Result(folded, err)
			}
			return err
		}
	}
	info, err := fs.Stat(path)
	if err != nil {
		return err
	}
	size := info.Size()
	if stat, ok := info.Sys().(*syscall.Stat_t); ok {
		size = stat.Blocks * 512
	}
	if policy.MinPoints <= 0 && size < policy.MinBytes || !policy.Allow() {
		return nil
	}
	merged, err := w.MergeOutOfOrderWithOptions(options)
	if policy.Result != nil {
		policy.Result(merged, err)
	}
	return err
}

func (c *ContinuationCodec) Fetch(ctx context.Context, kv store.ContinuationIO, m store.Metadata, from, until int, clock func() time.Time) (*store.Series, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	fs := newContinuationFiles(kv, true)
	w, err := whisper.OpenWithOptions(fs.main(), continuationOptions(fs, clock))
	if err != nil {
		return nil, err
	}
	defer w.Close()
	series, err := w.Fetch(from, until)
	if err != nil || series == nil {
		return nil, err
	}
	return &store.Series{Metadata: m, FromTime: series.FromTime(), UntilTime: series.UntilTime(), Step: series.Step(), Values: series.Values()}, nil
}

func (c *ContinuationCodec) Snapshot(ctx context.Context, kv store.ContinuationIO, m store.Metadata, clock func() time.Time) (store.Snapshot, error) {
	// A private overlay permits canonical OOO folding for the transfer API.
	// Ordinary Fetch never folds or mutates its database snapshot.
	overlay := newContinuationOverlay(kv)
	fs := newContinuationFiles(overlay, false).withClock(clock)
	w, err := whisper.OpenWithOptions(fs.main(), continuationOptions(fs, func() time.Time { return time.Unix(0, 0) }))
	if err != nil {
		return store.Snapshot{}, err
	}
	defer w.Close()
	if w.IsCompressed() {
		if err := w.MergeOutOfOrder(); err != nil {
			return store.Snapshot{}, err
		}
	}
	return continuationArchiveSnapshot(ctx, w, m)
}

func continuationArchiveSnapshot(ctx context.Context, w *whisper.Whisper, m store.Metadata) (store.Snapshot, error) {
	result := store.Snapshot{Metadata: m, Archives: make([]store.Archive, len(m.Retentions))}
	for i, r := range m.Retentions {
		if err := ctx.Err(); err != nil {
			return store.Snapshot{}, err
		}
		points, err := w.ArchivePointsWithRollups(i)
		if err != nil {
			return store.Snapshot{}, err
		}
		result.Archives[i] = store.Archive{Retention: r, Points: make([]store.Point, len(points))}
		for j, p := range points {
			result.Archives[i].Points[j] = store.Point{Timestamp: int64(p.Time), Value: p.Value}
		}
	}
	return result, nil
}

var _ store.ContinuationCodec = (*ContinuationCodec)(nil)

// RelayoutPlan moves every v6 compressed block except each archive's newest,
// which still receives appends, into the history bucket. Group values are
// copied unchanged; only their keys and the descriptors differ.
func (c *ContinuationCodec) RelayoutPlan(ctx context.Context, kv store.ContinuationIO) (store.RelayoutPlan, error) {
	type blockID struct {
		file           uint64
		archive, index int
	}
	type info struct {
		desc   blockDescriptor
		raw    []byte
		groups []store.RelayoutKV
		last   int64
	}
	blocks := make(map[blockID]*info)
	get := func(id blockID) *info {
		b := blocks[id]
		if b == nil {
			b = &info{}
			blocks[id] = b
		}
		return b
	}
	err := kv.Visit([]byte{'f'}, func(key, value []byte) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if len(key) < 16 || (key[9] != 'b' && key[9] != 'c') {
			return nil
		}
		id := blockID{binary.BigEndian.Uint64(key[1:9]), int(binary.BigEndian.Uint16(key[10:12])), int(binary.BigEndian.Uint32(key[12:16]))}
		switch {
		case key[9] == 'b' && len(key) == 16:
			d, err := decodeBlockDescriptor(value)
			if err != nil {
				return err
			}
			b := get(id)
			b.desc, b.raw = d, value
		case key[9] == 'c' && len(key) == 20:
			b := get(id)
			b.groups = append(b.groups, store.RelayoutKV{Key: key, Value: value})
		}
		return nil
	})
	if err != nil {
		return store.RelayoutPlan{}, err
	}
	// Each archive's newest block is the one CWhisper appends to.
	type archiveID struct {
		file    uint64
		archive int
	}
	newest := make(map[archiveID]blockID)
	for id, b := range blocks {
		if b.raw == nil || b.desc.count == 0 {
			continue
		}
		key := b.desc.groupKey(id.file, id.archive, id.index, (b.desc.count-1)/128)
		var value []byte
		if !b.desc.bucketed && len(b.groups) > 0 {
			value = b.groups[len(b.groups)-1].Value
		} else if v, err := kv.Get(key); err == nil {
			value = v
		} else {
			return store.RelayoutPlan{}, err
		}
		var group store.PointBlock
		if err := store.DecodePointBlock(value, &group); err != nil {
			return store.RelayoutPlan{}, err
		}
		b.last = group.Points[(b.desc.count-1)%128].Timestamp
		a := archiveID{id.file, id.archive}
		if n, ok := newest[a]; !ok || b.last > blocks[n].last {
			newest[a] = id
		}
	}
	var plan store.RelayoutPlan
	ids := make([]blockID, 0, len(blocks))
	for id := range blocks {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool {
		a, b := ids[i], ids[j]
		if a.file != b.file {
			return a.file < b.file
		}
		if a.archive != b.archive {
			return a.archive < b.archive
		}
		return a.index < b.index
	})
	for _, id := range ids {
		b := blocks[id]
		if b.raw == nil || b.desc.bucketed || b.desc.count == 0 || newest[archiveID{id.file, id.archive}] == id ||
			len(b.groups) != (b.desc.count+127)/128 {
			continue
		}
		moved := blockDescriptor{count: b.desc.count, bucketed: true, bucket: continuationHistoryBucket}
		block := store.RelayoutBlock{Descriptor: continuationBlockPrefix(id.file, 'b', id.archive, id.index), Old: b.raw, New: moved.encode()}
		for _, g := range b.groups {
			block.Copies = append(block.Copies, store.RelayoutKV{Key: store.ContinuationBucketKey(continuationHistoryBucket, g.Key), Value: g.Value})
			block.Removed = append(block.Removed, g.Key)
			plan.Bytes += len(g.Key) + len(g.Value)
		}
		plan.Blocks = append(plan.Blocks, block)
	}
	return plan, nil
}

var _ store.ContinuationRelayouter = (*ContinuationCodec)(nil)

// NewestFinestPoint reads physical archive slots, including history outside the
// current retention window. Fetch would hide exactly the idle points we need.
func (c *ContinuationCodec) NewestFinestPoint(ctx context.Context, kv store.ContinuationIO, m store.Metadata) (time.Time, error) {
	overlay := newContinuationOverlay(kv)
	fs := newContinuationFiles(overlay, false)
	w, err := whisper.OpenWithOptions(fs.main(), continuationOptions(fs, func() time.Time { return time.Unix(0, 0) }))
	if err != nil {
		return time.Time{}, err
	}
	defer w.Close()
	if w.IsCompressed() {
		if err := w.MergeOutOfOrder(); err != nil {
			return time.Time{}, err
		}
	}
	if err := ctx.Err(); err != nil {
		return time.Time{}, err
	}
	points, err := w.ArchivePointsWithRollups(0)
	if err != nil {
		return time.Time{}, err
	}
	var newest int
	for _, p := range points {
		if p.Time > newest && !math.IsNaN(p.Value) {
			newest = p.Time
		}
	}
	if newest == 0 {
		return time.Time{}, nil
	}
	return time.Unix(int64(newest), 0), nil
}
