package whisper

// Out-of-order support for the compressed format.
//
// cwhisper encodes points with delta-of-delta + XOR and is addressable only per
// block, so a point that is not strictly newer than the current block watermark
// cannot be written in place and is discarded (see archiveInfo.splitOutOfOrder).
// That silently loses late arrivals from lagging relays, and makes historical
// backfill into an existing file impossible.
//
// With Options.OutOfOrder set, those points are diverted into a sidecar: a plain
// *classic* whisper file normally named "<path>.ooo", with identical retentions,
// aggregation method and xFilesFactor, created sparse. Near the filesystem's
// filename limit, the sidecar uses a stable hashed name in the same directory.
// Classic whisper is already a random-order, point-addressable store with
// retention enforcement and per-slot dedup, so no new file format is involved.
//
// The compressed file itself stays byte-compatible, so unmodified readers keep
// working; they simply do not see the diverted points. Fetch merges the sidecar
// back in, and MergeOutOfOrder folds it into the compressed file, recomputes the
// coarse archives over the affected windows, and deletes it.
//
// Two things callers own. Merging is a full file rewrite, so it is theirs to
// rate-limit (OutOfOrderPoints says how much has accumulated). And deleting a
// metric must delete its sidecar: see RemoveOutOfOrderSidecar.

import (
	"errors"
	"fmt"
	"math"
	"os"
	"sort"
)

// oooSuffix is appended to the compressed file's path when the result fits.
// It deliberately does not end in ".wsp": go-carbon's carbonserver indexes
// metrics by that suffix, and sidecars must stay invisible to find/glob.
const oooSuffix = ".ooo"

// errOOOIncompatible reports a sidecar whose grid no longer lines up with the
// compressed file, which happens when UpdateConfig changes retentions while a
// sidecar is present.
var errOOOIncompatible = errors.New("out-of-order sidecar is incompatible with the main file")

// oooPoint is a point the compressed encoder rejected, tagged with the max
// retention of the archive it was meant for.
//
// The tag matters: a propagated aggregate destined for a coarse archive can
// still be recent enough that routing it by age alone would land it in the
// sidecar's base archive instead.
type oooPoint struct {
	retention int
	point     dataPoint
}

// extraPoint is a point folded into an archive during a rewrite. replace marks
// it as superseding the file's own value at the same interval, which is true of
// recomputed aggregates and false of raw sidecar points.
type extraPoint struct {
	dataPoint
	replace bool
}

// appendOOO records points the encoder rejected, tagged with their archive.
//
// It is a no-op unless diverting is actually on, so the default path does not
// pay an allocation per discarded point.
func (whisper *Whisper) appendOOO(dst []oooPoint, archive *archiveInfo, ps []dataPoint) []oooPoint {
	if len(ps) == 0 || !whisper.oooEnabled() {
		return dst
	}

	retention := archive.MaxRetention()
	for _, p := range ps {
		dst = append(dst, oooPoint{retention: retention, point: p})
	}

	return dst
}

// oooEnabled reports whether rejected points should be diverted to a sidecar.
func (whisper *Whisper) oooEnabled() bool {
	return whisper.compressed &&
		whisper.opts != nil &&
		whisper.opts.OutOfOrder &&
		// Mix already forbids backfilling lower archives and drops on
		// propagation; do not pretend otherwise.
		whisper.aggregationMethod != Mix &&
		// a memFile has no directory to place a sidecar in
		!whisper.opts.InMemory
}

func (whisper *Whisper) oooSidecarPath() string { return OutOfOrderSidecarPath(whisper.file.Name()) }

// detectOOO records whether a sidecar is present, so the read path can skip the
// open entirely for the overwhelmingly common case of a file that has never
// received an out-of-order point.
//
// Detection is a stat rather than a flag in the compressed header: it keeps the
// .wsp bytes untouched, and a negative dentry makes the repeated miss cheap.
func (whisper *Whisper) detectOOO() error {
	if !whisper.compressed || whisper.opts == nil || whisper.opts.InMemory {
		return nil
	}

	path := whisper.oooSidecarPath()
	if _, err := os.Stat(path); err == nil {
		whisper.oooPath = path
	} else if os.IsNotExist(err) {
		whisper.oooPath = ""
	} else {
		return fmt.Errorf("detect out-of-order sidecar %s: %w", path, err)
	}

	return nil
}

// discardOrphanedOOO removes a sidecar left behind by a previous incarnation of
// this metric.
//
// CreateWithOptions only gets this far when the main file did not exist, so any
// sidecar at this path has no owner: the metric was deleted (by a retention
// sweep, say) and is now being recreated. Merging it would resurrect the old
// metric's data into the new file.
func (whisper *Whisper) discardOrphanedOOO() {
	if !whisper.compressed || whisper.opts == nil || whisper.opts.InMemory {
		return
	}

	path := whisper.oooSidecarPath()
	if _, err := os.Stat(path); err != nil {
		return
	}
	if err := os.Remove(path); err != nil {
		whisper.NonFatalErrors = append(whisper.NonFatalErrors, fmt.Errorf("remove orphaned out-of-order sidecar %s: %w", path, err))
	}
}

func (whisper *Whisper) oooOptions() *Options {
	return &Options{
		// A sidecar is nearly always near-empty, and a full classic whisper
		// file is ~12 bytes per point of its whole retention. Sparse makes the
		// on-disk cost proportional to the points actually written.
		Sparse:           true,
		FLock:            whisper.opts.FLock,
		FlockType:        whisper.opts.FlockType,
		OpenFileFlag:     whisper.opts.OpenFileFlag,
		IgnoreNowOnWrite: whisper.opts.IgnoreNowOnWrite,
	}
}

// openOOO opens the sidecar, creating it when create is set and it is absent.
//
// It returns (nil, nil) when there is no usable sidecar: either none exists and
// create is false, or one exists whose retentions no longer match the main file
// (recorded as a non-fatal error, since merging it would scatter values into the
// wrong slots). The caller must handle a nil sidecar.
func (whisper *Whisper) openOOO(create bool) (*Whisper, error) {
	if whisper.opts == nil || whisper.opts.InMemory {
		return nil, nil
	}

	// an unusable sidecar stays unusable until an operator intervenes; do not
	// re-stat it, and do not grow NonFatalErrors once per read and write
	if whisper.oooBroken {
		return nil, nil
	}

	path := whisper.oooSidecarPath()
	opts := whisper.oooOptions()

	sidecar, err := OpenWithOptions(path, opts)
	switch {
	case err == nil:
		if cerr := whisper.checkOOOCompatible(sidecar); cerr != nil {
			sidecar.Close()
			whisper.markOOOBroken(cerr)
			return nil, nil
		}
		return sidecar, nil
	case !os.IsNotExist(err):
		return nil, err
	case !create:
		return nil, nil
	}

	sidecar, err = CreateWithOptions(
		path,
		NewRetentionsNoPointer(whisper.Retentions()),
		whisper.aggregationMethod,
		whisper.xFilesFactor,
		opts,
	)
	if errors.Is(err, os.ErrExist) {
		// Someone created it between our open and our create. Note that
		// CreateWithOptions stats and then creates rather than using O_EXCL, so
		// this catches the sequential case only; two writers racing on the same
		// metric is already outside what this package serialises.
		return OpenWithOptions(path, opts)
	}

	return sidecar, err
}

// oooSidecar returns the sidecar handle, opening it on first use and caching it
// for the life of this Whisper. It returns nil when there is no usable sidecar.
//
// The handle is owned by whisper: callers must not close it. Reopening per fetch
// was measurably wasteful - a metric that once received a single late point
// would pay an open, flock and header parse on every subsequent read, forever.
func (whisper *Whisper) oooSidecar(create bool) (*Whisper, error) {
	if whisper.oooFile != nil {
		return whisper.oooFile, nil
	}

	sidecar, err := whisper.openOOO(create)
	if err != nil || sidecar == nil {
		return nil, err
	}

	whisper.oooFile = sidecar
	whisper.oooPath = sidecar.file.Name()

	return sidecar, nil
}

// closeOOO releases the cached sidecar handle, if any.
func (whisper *Whisper) closeOOO() error {
	if whisper.oooFile == nil {
		return nil
	}

	err := whisper.oooFile.Close()
	whisper.oooFile = nil

	return err
}

// checkOOOCompatible verifies the sidecar shares the main file's grid, so that
// archive i of one lines up slot-for-slot with archive i of the other.
func (whisper *Whisper) checkOOOCompatible(sidecar *Whisper) error {
	main, side := whisper.Retentions(), sidecar.Retentions()
	if len(main) != len(side) {
		return fmt.Errorf("%w: %s has %d archives, want %d", errOOOIncompatible, sidecar.file.Name(), len(side), len(main))
	}

	for i := range main {
		if main[i].secondsPerPoint != side[i].secondsPerPoint || main[i].numberOfPoints != side[i].numberOfPoints {
			return fmt.Errorf("%w: %s archive %d is %s, want %s", errOOOIncompatible, sidecar.file.Name(), i, side[i].String(), main[i].String())
		}
	}

	return nil
}

// markOOOBroken latches the sidecar as unusable and records why, once.
//
// Reads then skip it and writes fall back to discarding late points, which is
// the behaviour without Options.OutOfOrder. Failing the write instead would
// take the metric down entirely for as long as the mismatch lasts, and the
// mismatch outlives the process: UpdateConfig can leave one behind on disk.
func (whisper *Whisper) markOOOBroken(err error) {
	whisper.closeOOO()
	whisper.oooBroken = true
	whisper.oooPath = ""
	whisper.NonFatalErrors = append(whisper.NonFatalErrors, err)
}

// divertOutOfOrder writes rejected points into the sidecar, creating it on first
// use. Each point is written against the same archive that rejected it, rather
// than re-routed by age.
//
// An unusable sidecar is not an error: the points are dropped exactly as they
// would be without Options.OutOfOrder, and markOOOBroken has already recorded
// the reason in NonFatalErrors.
func (whisper *Whisper) divertOutOfOrder(dropped []oooPoint) error {
	if len(dropped) == 0 {
		return nil
	}

	sidecar, err := whisper.oooSidecar(true)
	if err != nil {
		return fmt.Errorf("divert out-of-order points: %w", err)
	}
	if sidecar == nil {
		return nil
	}

	byRetention := map[int][]*TimeSeriesPoint{}
	for _, d := range dropped {
		byRetention[d.retention] = append(byRetention[d.retention], &TimeSeriesPoint{
			Time:  d.point.interval,
			Value: d.point.value,
		})
	}

	for retention, points := range byRetention {
		if err := sidecar.UpdateManyForArchive(points, retention); err != nil {
			return fmt.Errorf("divert out-of-order points: %w", err)
		}
	}

	whisper.oooPath = sidecar.file.Name()
	whisper.OutOfOrderPoints += uint32(len(dropped))

	return nil
}

// mergeOutOfOrderValues overlays sidecar data onto values read from the
// compressed archives.
//
// The sidecar shares the main file's retentions, so archive i covers the same
// intervals at the same step and the two value slices are directly aligned. A
// value is taken from the sidecar only where the compressed file has none: on-time
// data stays authoritative, and for a coarse archive a partial aggregate is used
// only when nothing on time covered that window at all.
//
// So a diverted point shows up immediately at base resolution, but a coarse
// window that already holds an aggregate keeps the stale one until
// MergeOutOfOrder recomputes it - the encoded slot cannot be rewritten in place.
func (whisper *Whisper) mergeOutOfOrderValues(archiveIndex, fromTime, untilTime int, values []float64) error {
	if whisper.oooBroken {
		return nil
	}
	if whisper.oooPath == "" {
		if err := whisper.detectOOO(); err != nil {
			return err
		}
		if whisper.oooPath == "" {
			return nil
		}
	}

	sidecar, err := whisper.oooSidecar(false)
	if err != nil {
		return err
	}
	if sidecar == nil {
		return nil
	}

	if archiveIndex >= len(sidecar.archives) {
		return nil
	}

	ts, err := sidecar.fetchFromArchive(sidecar.archives[archiveIndex], fromTime, untilTime)
	if err != nil {
		return fmt.Errorf("fetch out-of-order sidecar: %w", err)
	}
	if ts == nil {
		return nil
	}

	for i, v := range ts.values {
		if i >= len(values) {
			break
		}
		if math.IsNaN(values[i]) && !math.IsNaN(v) {
			values[i] = v
		}
	}

	return nil
}

// MergeOutOfOrder folds the out-of-order sidecar into the compressed file and
// removes it, so subsequent reads need not consult it.
//
// It is a full rewrite of the file (the same machinery extension uses), so it is
// far from free; callers are expected to rate-limit it and to trigger it on how
// much has accumulated rather than on every diverted point.
//
// The sidecar is deleted only after the merged file has been renamed into place.
// A crash in between leaves a sidecar whose points are already in the main file,
// which is harmless: on read the main file wins.
//
// IMPORTANT: like UpdateConfig, this replaces the underlying file. The caller
// should reopen the path rather than keep using stale handles to it.
func (whisper *Whisper) MergeOutOfOrder() error {
	if !whisper.compressed {
		return errors.New("out-of-order merge is only supported for the compressed format")
	}
	if whisper.aggregationMethod == Mix {
		// oooEnabled refuses to create one under Mix, and classic whisper
		// refuses the duplicated retentions Mix reports, so a Mix sidecar
		// should not exist; say so rather than take an untested path.
		return errors.New("out-of-order merge is not supported for mix aggregation")
	}

	sidecar, err := whisper.oooSidecar(false)
	if err != nil {
		return fmt.Errorf("merge out-of-order points: %w", err)
	}
	if sidecar == nil {
		if whisper.oooBroken {
			whisper.oooPath = whisper.oooSidecarPath()
			return fmt.Errorf("merge out-of-order points: %w", errOOOIncompatible)
		}
		// nothing usable to merge
		whisper.oooPath = ""
		whisper.OutOfOrderPoints = 0
		return nil
	}

	extras := make([][]dataPoint, len(whisper.archives))
	for i := range whisper.archives {
		if i >= len(sidecar.archives) {
			break
		}
		if extras[i], err = readArchivePoints(sidecar, i); err != nil {
			whisper.closeOOO()
			return fmt.Errorf("merge out-of-order points: %w", err)
		}
	}

	sidecarPath := sidecar.file.Name()
	if err := whisper.closeOOO(); err != nil {
		return fmt.Errorf("merge out-of-order points: %w", err)
	}

	recomputed, err := whisper.recomputeAggregates(extras)
	if err != nil {
		return fmt.Errorf("merge out-of-order points: %w", err)
	}
	pending := make([][]extraPoint, len(extras))
	for i := range extras {
		pending[i] = combineExtras(extras[i], recomputed[i])
	}

	// reuse the extension sizing so a rewrite that is already overdue is not
	// wasted, and so the added points get a little more room where needed
	rets, _, _ := whisper.computeExtendedRetentions()
	if err := whisper.rewrite(rets, "compact", func(i int) []extraPoint { return pending[i] }); err != nil {
		return fmt.Errorf("merge out-of-order points: %w", err)
	}

	// The merge itself is done here: the points are in the main file and the
	// rewrite has been renamed into place. A sidecar that will not delete is
	// therefore stale rather than pending, so latch it unusable and report it
	// non-fatally. Returning an error instead would leave oooPath set, and the
	// caller would redo the whole file rewrite on every subsequent write,
	// spending its compaction budget on data that is already merged.
	//
	// Reopening the file does pick the leftover up again and merge it once more,
	// harmlessly (the main file wins on read), so the waste is bounded per open
	// rather than per write.
	if err := os.Remove(sidecarPath); err != nil && !os.IsNotExist(err) {
		whisper.markOOOBroken(fmt.Errorf("remove out-of-order sidecar %s: %w", sidecarPath, err))
		whisper.OutOfOrderPoints = 0

		return nil
	}
	whisper.oooPath = ""
	whisper.OutOfOrderPoints = 0

	return nil
}

// recomputeAggregates recalculates the coarse archives over every window a
// merged base-archive point falls into.
//
// Folding sidecar points into archive 0 is not enough on its own. The coarse
// archives were aggregated when the on-time data arrived, from a window that was
// missing the late point, and those slots are already encoded in the file: a
// gap-filling merge leaves them stale, so a backfill would stay invisible at
// every resolution above the base archive. The sidecar's own propagated values
// are no help either - they aggregate only the sidecar's sparse data.
//
// So each level is recomputed from the union of the file's points and the merged
// ones, one archive at a time, exactly as archiveUpdateManyCompressed propagates
// on write: window start from AggregateInterval, xFilesFactor against the full
// slot count, and the file's aggregation method. The results are marked replace
// so they supersede the stale values during the rewrite.
//
// extras[i] holds the sidecar points already destined for archive i. They are
// merged into that archive as-is rather than recomputed, but they still count as
// a change to it, so they drive recomputation of the level below just as a
// recomputed aggregate does. Windows that fail xFilesFactor are left alone
// rather than written as a partial aggregate.
func (whisper *Whisper) recomputeAggregates(extras [][]dataPoint) ([][]dataPoint, error) {
	out := make([][]dataPoint, len(whisper.archives))
	if len(whisper.archives) < 2 || whisper.aggregationMethod == Mix {
		return out, nil
	}

	for i := 0; i+1 < len(whisper.archives); i++ {
		higher, lower := whisper.archives[i], whisper.archives[i+1]

		// what changes archive i: the sidecar points folded into it, plus what
		// the level above already had us recompute for it. A point can be
		// diverted at any archive, not only the base one, so a level with no
		// change above it can still have one of its own - hence continue rather
		// than stopping the cascade here.
		changed := make([]dataPoint, 0, len(extras[i])+len(out[i]))
		changed = append(changed, extras[i]...)
		changed = append(changed, out[i]...)

		touched := windowsOf(higher, changed)
		if len(touched) == 0 {
			continue
		}

		values, err := whisper.mergedArchiveValues(i, extras[i], out[i])
		if err != nil {
			return nil, err
		}

		perWindow := lower.secondsPerPoint / higher.secondsPerPoint

		var next []dataPoint
		for _, start := range touched {
			var known []float64
			for t := start; t < start+lower.secondsPerPoint; t += higher.secondsPerPoint {
				if v, ok := values[t]; ok {
					known = append(known, v)
				}
			}
			if len(known) == 0 || float32(len(known))/float32(perWindow) < whisper.xFilesFactor {
				continue
			}
			next = append(next, dataPoint{start, aggregate(whisper.aggregationMethod, known)})
		}

		sort.Slice(next, func(a, b int) bool { return next[a].interval < next[b].interval })
		out[i+1] = next
	}

	return out, nil
}

// mergedArchiveValues reads archive index over the span the given points cover
// and returns interval -> value for the union of the file, the sidecar points
// and the recomputed ones, in increasing order of authority.
func (whisper *Whisper) mergedArchiveValues(index int, sidecar, recomputed []dataPoint) (map[int]float64, error) {
	archive := whisper.archives[index]

	from, until, ok := spanOf(sidecar, recomputed)
	if !ok {
		return map[int]float64{}, nil
	}

	// widen to whole windows of the next archive down, so a partially covered
	// window is still aggregated over all of its slots
	if archive.next != nil {
		from = archive.AggregateInterval(from)
		until = archive.AggregateInterval(until) + archive.next.secondsPerPoint - archive.secondsPerPoint
	}

	series, err := whisper.storedPoints(archive, from, until)
	if err != nil {
		return nil, fmt.Errorf("recompute aggregates: read archive %d: %w", index, err)
	}

	values := make(map[int]float64, len(series)+len(sidecar)+len(recomputed))
	for _, p := range series {
		if p.interval != 0 {
			values[p.interval] = p.value
		}
	}
	// the file is authoritative over the sidecar, but a recomputed aggregate is
	// strictly better informed than either
	for _, p := range sidecar {
		if _, ok := values[p.interval]; !ok {
			values[p.interval] = p.value
		}
	}
	for _, p := range recomputed {
		values[p.interval] = p.value
	}

	return values, nil
}

// combineExtras interleaves the sidecar points with the recomputed aggregates
// for one archive, ascending. A recomputed aggregate supersedes the sidecar
// point at the same interval and is marked to supersede the file's too.
func combineExtras(sidecar, recomputed []dataPoint) []extraPoint {
	if len(recomputed) == 0 {
		return markExtras(sidecar, false)
	}
	if len(sidecar) == 0 {
		return markExtras(recomputed, true)
	}

	out := make([]extraPoint, 0, len(sidecar)+len(recomputed))
	i, j := 0, 0
	for i < len(sidecar) && j < len(recomputed) {
		switch {
		case sidecar[i].interval < recomputed[j].interval:
			out = append(out, extraPoint{dataPoint: sidecar[i]})
			i++
		case sidecar[i].interval > recomputed[j].interval:
			out = append(out, extraPoint{dataPoint: recomputed[j], replace: true})
			j++
		default:
			out = append(out, extraPoint{dataPoint: recomputed[j], replace: true})
			i++
			j++
		}
	}
	out = append(out, markExtras(sidecar[i:], false)...)
	out = append(out, markExtras(recomputed[j:], true)...)

	return out
}

func markExtras(points []dataPoint, replace bool) []extraPoint {
	if len(points) == 0 {
		return nil
	}

	out := make([]extraPoint, len(points))
	for i, p := range points {
		out[i] = extraPoint{dataPoint: p, replace: replace}
	}

	return out
}

// storedPoints returns the points archive actually holds in [from, until],
// straight out of its blocks and its own buffer.
//
// This is fetchCompressed without its tail. For a non-base archive that
// function also live aggregates whatever sits in the higher archives' buffers,
// which is not what the file holds: it ignores xFilesFactor and drops its own
// trailing group. Feeding those phantom values into an aggregation would inflate
// the known-slot count and could fold a partial aggregate into a stored one.
//
// No reachable case is known - a diverted point is by construction older than
// the buffers that would pollute it - but that argument rests on the ratio
// between every adjacent pair of retentions, which callers choose. Reading
// exactly what the file holds costs a few lines and does not.
func (whisper *Whisper) storedPoints(archive *archiveInfo, from, until int) ([]dataPoint, error) {
	var dst []dataPoint

	buf := make([]byte, archive.blockSize)
	for _, block := range archive.getSortedBlockRanges() {
		if block.end < from || until < block.start {
			continue
		}
		if err := whisper.fileReadAt(buf, int64(archive.blockOffset(block.index))); err != nil {
			return nil, fmt.Errorf("read block %d: %w", block.index, err)
		}

		var err error
		if dst, _, err = archive.ReadFromBlock(buf, dst, from, until); err != nil {
			return nil, fmt.Errorf("read block %d: %w", block.index, err)
		}

		for i := range buf {
			buf[i] = 0
		}
	}

	if archive.hasBuffer() {
		for _, p := range unpackDataPoints(archive.buffer) {
			if p.interval != 0 && from <= p.interval && p.interval <= until {
				dst = append(dst, p)
			}
		}
	}

	return dst, nil
}

// windowsOf returns the distinct next-archive window starts that points fall
// into, ascending.
func windowsOf(archive *archiveInfo, points []dataPoint) []int {
	if archive.next == nil || len(points) == 0 {
		return nil
	}

	seen := make(map[int]struct{}, len(points))
	var starts []int
	for _, p := range points {
		if p.interval <= 0 {
			continue
		}
		start := archive.AggregateInterval(p.interval)
		if _, ok := seen[start]; ok {
			continue
		}
		seen[start] = struct{}{}
		starts = append(starts, start)
	}

	sort.Ints(starts)

	return starts
}

// spanOf returns the inclusive interval range covered by the given point lists.
func spanOf(lists ...[]dataPoint) (from, until int, ok bool) {
	for _, points := range lists {
		for _, p := range points {
			if p.interval <= 0 {
				continue
			}
			if !ok || p.interval < from {
				from = p.interval
			}
			if !ok || p.interval > until {
				until = p.interval
			}
			ok = true
		}
	}

	return from, until, ok
}

// readArchivePoints returns every live point in archive index of a classic
// whisper file, ascending by interval.
func readArchivePoints(w *Whisper, index int) ([]dataPoint, error) {
	archive := w.archives[index]

	// Sparse sidecars can have long retentions but few live points. Keep
	// scratch space bounded and only allocate decoded points that survive.
	bufferSize := 4096 * PointSize
	if archive.Size() < bufferSize {
		bufferSize = archive.Size()
	}
	buf := make([]byte, bufferSize)

	// a classic archive is a ring buffer: unwritten slots are zero, and slots
	// not yet overwritten since the last wrap hold points older than retention
	oldest := int(Now().Unix()) - archive.MaxRetention()

	var points []dataPoint
	for offset := 0; offset < archive.Size(); {
		chunk := buf
		if remaining := archive.Size() - offset; len(chunk) > remaining {
			chunk = chunk[:remaining]
		}
		if err := w.fileReadAt(chunk, archive.Offset()+int64(offset)); err != nil {
			return nil, fmt.Errorf("read archive %d: %w", index, err)
		}
		for i := 0; i < len(chunk); i += PointSize {
			p := unpackDataPoint(chunk[i : i+PointSize])
			if p.interval <= 0 || p.interval <= oldest {
				continue
			}
			points = append(points, p)
		}
		offset += len(chunk)
	}

	sort.Slice(points, func(i, j int) bool { return points[i].interval < points[j].interval })

	return points, nil
}

// OutOfOrderPath returns the path of this file's out-of-order sidecar if one
// exists, otherwise the empty string.
func (whisper *Whisper) OutOfOrderPath() string { return whisper.oooPath }

// OutOfOrderSidecarPath returns the sidecar path belonging to the whisper file
// at path. The file need not exist. Paths near the filename limit use a stable
// hashed filename in the same directory.
func OutOfOrderSidecarPath(path string) string { return auxiliaryPath(path, oooSuffix) }

// RemoveOutOfOrderSidecar deletes the sidecar belonging to the whisper file at
// path, and reports whether there was one. A missing sidecar is not an error.
//
// Nothing in this package removes a whisper file, so nothing here can notice
// when one goes away. Callers that delete metrics themselves - a retention
// sweep, an explicit delete - must call this alongside the unlink, or the
// sidecar is orphaned until the same metric happens to be recreated (see
// discardOrphanedOOO). Deleting it is always safe: a sidecar holds only points
// the main file rejected.
func RemoveOutOfOrderSidecar(path string) (removed bool, err error) {
	if err := os.Remove(OutOfOrderSidecarPath(path)); err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}

		return false, err
	}

	return true, nil
}

// archiveIndexOf returns the position of archive in whisper.archives, or -1.
func (whisper *Whisper) archiveIndexOf(archive *archiveInfo) int {
	for i, a := range whisper.archives {
		if a == archive {
			return i
		}
	}

	return -1
}
