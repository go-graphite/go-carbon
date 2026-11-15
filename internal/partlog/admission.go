package partlog

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// Epochs reserve disjoint cursor ranges before reading an old tail. Frames and
// payloads retain their existing encoding. A fence is synced before any segment
// in its range can acknowledge an append; readers may cross only fenced gaps.
const epochStride uint64 = 1 << 48
const epochMask = epochStride - 1

type epochFence struct {
	Version  int
	Identity string
	Epoch    uint64
}

// OpenAdmission opens a new append epoch before historical recovery. Its
// append/sync owners never wait for that recovery. Replay, Follow and Truncate
// wait for WaitReady; historical failure is reported by Errors and WaitReady.
// Acceptance has the same group-sync durability contract as Open, not an fsync
// per point. The caller must keep intake closed if opening the fence fails.
func OpenAdmission(dir string, opts Options) (*Log, error) {
	return open(dir, opts, true)
}

// WaitReady joins validation of the historical prefix. It does not wait for
// downstream replay or database recovery. Canceling the wait does not cancel
// validation; Close always joins it before releasing exclusive ownership.
func (l *Log) WaitReady(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-l.recovered:
		return l.recoverErr
	}
}

func (l *Log) follows(previous, first uint64) bool {
	return previous != math.MaxUint64 && (first == previous+1 ||
		(first&epochMask == 1 && first > previous && l.fences[first/epochStride]))
}

func (l *Log) loadFences() error {
	l.fences = make(map[uint64]bool)
	paths, err := filepath.Glob(filepath.Join(l.dir, "epoch-*.fence"))
	if err != nil {
		return err
	}
	for _, path := range paths {
		var f epochFence
		if err = readMetadata(path, &f); err != nil {
			return err
		}
		if f.Version != 1 || f.Identity != l.identity || f.Epoch == 0 || f.Epoch > math.MaxUint16 || filepath.Base(path) != fmt.Sprintf("epoch-%020d.fence", f.Epoch) {
			return errors.New("invalid part log epoch fence")
		}
		l.fences[f.Epoch] = true
		l.epoch = max(l.epoch, f.Epoch)
	}
	if uint64(len(l.fences)) != l.epoch {
		return errors.New("missing part log epoch fence")
	}
	return nil
}

func segmentFirst(path string) (uint64, error) {
	name := strings.TrimSuffix(filepath.Base(path), ".plog")
	_, number, ok := strings.Cut(strings.TrimPrefix(name, "stream-"), "-")
	if !ok {
		return 0, os.ErrInvalid
	}
	return strconv.ParseUint(number, 10, 64)
}

func (l *Log) openAdmission() error {
	if l.epoch == math.MaxUint16 {
		return errors.New("part log epoch space exhausted")
	}
	paths := make([][]string, len(l.streams))
	entries, err := os.ReadDir(l.dir)
	if err != nil {
		return err
	}
	// ReadDir sorts once for all producers. Repeating a directory-wide glob
	// per stream makes admission depend on streams times historical segments.
	for _, entry := range entries {
		name := entry.Name()
		if len(name) < 13 || !strings.HasPrefix(name, "stream-") || name[11] != '-' || !strings.HasSuffix(name, ".plog") {
			continue
		}
		id, err := strconv.Atoi(name[7:11])
		if err != nil || id < 0 || id >= len(paths) {
			return errors.New("invalid part log segment stream")
		}
		first, err := segmentFirst(name)
		if err != nil || first == 0 || (first >= epochStride && !l.fences[first/epochStride]) {
			return errors.New("unfenced part log segment")
		}
		paths[id] = append(paths[id], filepath.Join(l.dir, name))
	}
	for i, s := range l.streams {
		// Preserve the pre-epoch legacy retirement boundary without reading its
		// data. Later truncations publish the watermark before removing files.
		if len(paths[i]) > 0 && l.epoch == 0 {
			first, e := segmentFirst(paths[i][0])
			if e != nil || first == 0 || first >= epochStride {
				return errors.New("invalid legacy part log cursor")
			}
			s.retired = first - 1
		}
		if err = s.loadRetired(); err != nil {
			return err
		}
	}
	results := make(chan error, len(l.streams))
	if l.epoch == 0 {
		for _, s := range l.streams {
			go func() { results <- s.saveRetired(s.retired) }()
		}
		var err error
		for range l.streams {
			err = errors.Join(err, <-results)
		}
		if err != nil {
			return err
		}
	}
	l.streams[0].hook("admission-frontiers-ready")
	l.epoch++
	if err := l.writeMetadata(fmt.Sprintf("epoch-%020d.fence", l.epoch), epochFence{1, l.identity, l.epoch}); err != nil {
		return err
	}
	l.fences[l.epoch] = true
	l.streams[0].hook("admission-fence-ready")
	// The durable epoch, not a precreated empty file, fences each stream's
	// cursors. Its owner creates and syncs a segment on the first append. Idle
	// streams need no file and first acceptance never waits for every owner.
	for _, s := range l.streams {
		s.last, s.durable = l.epoch*epochStride, l.epoch*epochStride
		go s.run()
	}
	l.streams[0].hook("admission-owners-ready")
	go func() {
		for i, s := range l.streams {
			go func() {
				history := &stream{log: l, id: s.id}
				e := history.recoverPaths(paths[i])
				_, e = l.call(s.id, request{op: "recovered", history: history.segments, from: history.retired, err: e})
				results <- e
			}()
		}
		for range l.streams {
			l.recoverErr = errors.Join(l.recoverErr, <-results)
		}
		close(l.recovered)
	}()
	return nil
}

type retiredFrontier struct {
	Version  int
	Identity string
	Stream   int
	Cursor   uint64
}

func (s *stream) loadRetired() error {
	var f retiredFrontier
	err := readMetadata(filepath.Join(s.log.dir, fmt.Sprintf("stream-%04d.retired", s.id)), &f)
	if errors.Is(err, os.ErrNotExist) && len(s.log.fences) == 0 {
		return nil
	}
	if err != nil {
		return err
	}
	if f.Version != 1 || f.Identity != s.log.identity || f.Stream != s.id || (f.Cursor >= epochStride && !s.log.fences[f.Cursor/epochStride]) {
		return errors.New("invalid part log retired frontier")
	}
	s.retired = max(s.retired, f.Cursor)
	return nil
}

func (s *stream) saveRetired(cursor uint64) error {
	return s.log.writeMetadata(fmt.Sprintf("stream-%04d.retired", s.id), retiredFrontier{1, s.log.identity, s.id, cursor})
}

func readMetadata(path string, out any) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	if len(data) < 4 || len(data) > 4096 || binary.LittleEndian.Uint32(data) != crc32.Checksum(data[4:], checksum) {
		return errors.New("invalid part log metadata checksum")
	}
	return json.Unmarshal(data[4:], out)
}

func (l *Log) writeMetadata(name string, value any) error {
	body, err := json.Marshal(value)
	if err != nil {
		return err
	}
	data := binary.LittleEndian.AppendUint32(nil, crc32.Checksum(body, checksum))
	data = append(data, body...)
	path := filepath.Join(l.dir, name)
	f, err := os.OpenFile(path+".tmp", os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	n, err := f.Write(data)
	if n != len(data) {
		err = errors.Join(err, errors.New("short part log metadata write"))
	}
	err = errors.Join(err, f.Sync(), f.Close())
	if err == nil {
		err = os.Rename(path+".tmp", path)
	}
	if err == nil {
		err = syncDir(l.dir)
	}
	return err
}
