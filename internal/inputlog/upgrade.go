package inputlog

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"
)

const UpgradeCutName = "STARTUP-UPGRADE-CUT.json"

// UpgradeSource is one read-only source in the predecessor's correction order.
// Journal denotes the historical journal's place among legacy files. Its own
// salted partition cursors are carried by UpgradePlan.JournalOffsets.
type UpgradeSource struct {
	Name    string `json:"name,omitempty"`
	Offset  int64  `json:"offset"`
	Size    int64  `json:"size"`
	Journal bool   `json:"journal,omitempty"`
}

type UpgradePlan struct {
	Root           string                  `json:"root"`
	Sources        []UpgradeSource         `json:"sources"`
	JournalOffsets map[uint16]ReplayCursor `json:"journal_offsets,omitempty"`
}

// UpgradeCut orders the complete read-only predecessor plan before new appends.
// A cut is a fresh segment/dictionary boundary, identified by salt and sequence.
// Keep this manifest until the legacy plan has durable persistence/snapshot
// coverage. Neither an applied cursor nor a successful append can retire it.
type UpgradeCut struct {
	Version int         `json:"version"`
	Segment uint64      `json:"segment"`
	Salt    uint64      `json:"salt"`
	Offset  int64       `json:"offset"`
	Plan    UpgradePlan `json:"plan"`
}

func (c UpgradeCut) validate() error {
	if c.Version != 1 || c.Segment == 0 || c.Segment == ^uint64(0) || c.Salt == 0 || c.Offset != segmentHeaderSize || !validUpgradeName(c.Plan.Root) {
		return errors.New("invalid input log upgrade cut")
	}
	journals := 0
	for _, source := range c.Plan.Sources {
		if source.Offset < 0 || source.Size < source.Offset {
			return errors.New("invalid input log upgrade source cursor")
		}
		if source.Journal {
			journals++
			if source.Name != "" {
				return errors.New("journal upgrade source must use partition cursors")
			}
		} else if !validUpgradeName(source.Name) {
			return errors.New("invalid input log upgrade source name")
		}
	}
	if journals != 1 {
		return errors.New("input log upgrade plan requires one historical journal boundary")
	}
	for part, cursor := range c.Plan.JournalOffsets {
		if part >= partitions || cursor.Salt == 0 || cursor.Segment == 0 || cursor.Offset < segmentHeaderSize {
			return errors.New("invalid input log upgrade journal cursor")
		}
	}
	return nil
}

func validUpgradeName(name string) bool {
	return name != "" && name != "." && filepath.IsLocal(name) && filepath.Base(name) == name
}

func ReadUpgradeCut(dir string) (*UpgradeCut, error) {
	f, err := os.Open(filepath.Join(dir, UpgradeCutName))
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer f.Close()
	if info, err := f.Stat(); err != nil {
		return nil, err
	} else if info.Size() >= 64<<20 {
		return nil, errors.New("input log upgrade plan exceeds manifest budget")
	}
	var cut UpgradeCut
	dec := json.NewDecoder(io.LimitReader(f, 64<<20))
	if err := dec.Decode(&cut); err != nil {
		return nil, fmt.Errorf("read input log upgrade cut: %w", err)
	}
	if err := cut.validate(); err != nil {
		return nil, err
	}
	var extra any
	if err := dec.Decode(&extra); !errors.Is(err, io.EOF) {
		return nil, errors.New("trailing input log upgrade cut data")
	}
	return &cut, nil
}

// CommitUpgradeCut must precede StartAppend and every acknowledgment of new
// journal intake. Reopening an interrupted upgrade returns the committed plan;
// callers must use that plan, not reinventory or create another capture epoch.
// This primitive deliberately does not activate the application's intake path.
func (l *Log) CommitUpgradeCut(plan UpgradePlan) (*UpgradeCut, error) {
	return l.commitUpgradeCut(plan, func(string) {})
}

func (l *Log) commitUpgradeCut(plan UpgradePlan, stage func(string)) (*UpgradeCut, error) {
	if l.started || l.closed {
		return nil, errors.New("input log upgrade requires an unopened append owner")
	}
	if cut, err := ReadUpgradeCut(l.dir); err != nil {
		return nil, err
	} else if cut != nil {
		// A predecessor can die after rename but before directory sync.
		// Reusing its visible plan must finish that durability boundary.
		if err := syncDir(l.dir); err != nil {
			return nil, err
		}
		return cut, nil
	}
	// Validate the immutable legacy order before allocating a segment.
	cut := &UpgradeCut{Version: 1, Segment: l.nextSeq, Salt: 1, Offset: segmentHeaderSize, Plan: plan}
	if err := cut.validate(); err != nil {
		return nil, err
	}
	seg, err := l.newSegment(l.opts.SegmentSize)
	if err != nil {
		return nil, err
	}
	l.recovered = append(l.recovered, seg)
	seg.salt = max(1, seg.salt)
	stage("segment_allocated")
	header := (segmentHeader{seq: seg.seq, salt: seg.salt, created: time.Now().UnixNano()}).append(nil)
	if _, err := seg.file.WriteAt(header, 0); err != nil {
		return nil, err
	}
	if err := errors.Join(seg.file.Sync(), syncDir(l.dir)); err != nil {
		return nil, err
	}
	stage("segment_synced")
	cut.Segment, cut.Salt = seg.seq, seg.salt
	data, err := json.Marshal(cut)
	if err != nil {
		return nil, err
	}
	if len(data) >= 64<<20 {
		return nil, errors.New("input log upgrade plan exceeds manifest budget")
	}
	tmp, err := os.CreateTemp(l.dir, ".startup-upgrade-cut-*")
	if err != nil {
		return nil, err
	}
	defer os.Remove(tmp.Name())
	_, err = tmp.Write(data)
	if err == nil {
		err = tmp.Sync()
	}
	if err = errors.Join(err, tmp.Close()); err != nil {
		return nil, err
	}
	stage("cut_synced")
	if err := os.Rename(tmp.Name(), filepath.Join(l.dir, UpgradeCutName)); err != nil {
		return nil, err
	}
	stage("cut_renamed")
	if err := syncDir(l.dir); err != nil {
		return nil, err
	}
	stage("cut_committed")
	// The first append can use this header without another allocation. Open's
	// historical frame inventory remains unchanged, including after a crash.
	l.active, l.wseg, l.woff = seg, seg, segmentHeaderSize
	return cut, nil
}
