package inputlog

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

const CopyMarkerName = "REQUIRE-COMPLETE-REPLAY"

// FinishCopyReplay makes recovery's next log boundary durable before retiring
// the one-time captured file list. Later starts follow normal log recycling.
func (l *Log) FinishCopyReplay() error {
	if err := l.Sync(); err != nil {
		return err
	}
	if err := os.Remove(filepath.Join(l.dir, CopyMarkerName)); err != nil {
		return err
	}
	return syncDir(l.dir)
}

// ValidateCopyFiles verifies exact captured frame boundaries before the clone's
// first replay. Ordinary logs have preallocated tails and intentionally stop at
// torn headers; a copy has exact lengths and must not silently accept that stop.
func ValidateCopyFiles(directory string, files []CopyFile) error {
	for _, file := range files {
		if filepath.Base(file.Name) != file.Name || file.Size < segmentHeaderSize {
			return errors.New("invalid captured input log file")
		}
		seg, err := openSegment(filepath.Join(directory, file.Name))
		if err != nil {
			return err
		}
		if seg == nil {
			return fmt.Errorf("invalid captured input log segment: %s", file.Name)
		}
		_, scanErr := seg.scanFrames(nil)
		err = errors.Join(scanErr, seg.file.Close())
		if err != nil {
			return err
		}
		if seg.size != file.Size || seg.end != file.Size {
			return fmt.Errorf("captured input log has a torn or corrupt header: %s", file.Name)
		}
	}
	return nil
}

// Position is a durable frame boundary. A copy retains all segments from From
// until ReleaseCopy, including the dictionary frames before its pending points.
type Position struct {
	Segment uint64 `json:"segment"`
	Offset  int64  `json:"offset"`
}

type CopyFile struct {
	Name string `json:"name"`
	Size int64  `json:"size"`
}

type CopyState struct {
	From  Position   `json:"from"`
	To    Position   `json:"to"`
	Files []CopyFile `json:"files"`
}

type copyRequest struct {
	op    string
	reply chan copyReply
}

type copyReply struct {
	state CopyState
	err   error
}

// PinCopy syncs accepted input and prevents segment recycling. Only one copy
// may own retention. The caller must release it, including on checkpoint failure.
func (l *Log) PinCopy() (CopyState, error) { return l.requestCopy("pin") }

// CaptureCopy syncs input and returns immutable prefixes of the pinned files.
// These files must be copied, never hard-linked: active segments still grow,
// and after release the source may recycle their inodes.
func (l *Log) CaptureCopy() (CopyState, error) { return l.requestCopy("capture") }

func (l *Log) ReleaseCopy() error {
	_, err := l.requestCopy("release")
	return err
}

func (l *Log) requestCopy(op string) (CopyState, error) {
	if !l.started {
		return CopyState{}, ErrClosed
	}
	reply := make(chan copyReply, 1)
	select {
	case l.copies <- copyRequest{op, reply}:
		r := <-reply
		return r.state, r.err
	case <-l.done:
		return CopyState{}, ErrClosed
	}
}

func (l *Log) copyRequest(r copyRequest) {
	if r.op == "release" {
		l.copyPin = nil
		r.reply <- copyReply{}
		return
	}
	if r.op == "pin" && l.copyPin != nil || r.op == "capture" && l.copyPin == nil {
		r.reply <- copyReply{err: errors.New("input log copy retention is not in the expected state")}
		return
	}
	if err := l.cycle(); err != nil {
		r.reply <- copyReply{err: err}
		return
	}
	if r.op == "pin" {
		from := Position{Segment: l.nextSeq, Offset: segmentHeaderSize}
		if len(l.live) > 0 {
			from.Segment = l.live[0].seq
		}
		l.copyPin = &from
	}
	state := CopyState{From: *l.copyPin, To: *l.copyPin}
	for _, seg := range l.live {
		state.Files = append(state.Files, CopyFile{filepath.Base(seg.path), seg.end})
		state.To = Position{seg.seq, seg.end}
	}
	r.reply <- copyReply{state: state}
}
