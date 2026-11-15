package inputlog

import (
	"errors"
	"path/filepath"
)

type appendStateRequest struct {
	release bool
	reply   chan appendStateReply
}
type appendStateReply struct {
	state CopyState
	err   error
}

// AppendState returns committed file frontiers without invoking cache admission.
// StartAppend retains these files until FinishAppendPersistence. Each independent
// reader may keep its descriptor and decoder while its frontier grows.
func (l *Log) AppendState() (CopyState, error) { return l.requestAppendState(false) }

// FinishAppendPersistence releases the startup suffix only after the caller has
// fenced intake, joined all readers, confirmed every source identity and synced
// the store (or committed a snapshot covering it). Application alone is insufficient.
func (l *Log) FinishAppendPersistence() error {
	_, err := l.requestAppendState(true)
	return err
}
func (l *Log) requestAppendState(release bool) (CopyState, error) {
	if !l.started {
		return CopyState{}, ErrClosed
	}
	reply := make(chan appendStateReply, 1)
	select {
	case l.appendStates <- appendStateRequest{release, reply}:
		r := <-reply
		return r.state, r.err
	case <-l.done:
		return CopyState{}, ErrClosed
	}
}
func (l *Log) appendState(r appendStateRequest) {
	if r.release {
		if l.src == nil {
			r.reply <- appendStateReply{err: errors.New("append persistence requires an attached source")}
			return
		}
		l.replayPendingBytes.Store(0)
		l.appendPin = nil
		r.reply <- appendStateReply{err: l.completeRecovery()}
		return
	}
	if l.appendErr != nil {
		r.reply <- appendStateReply{err: l.appendErr}
		return
	}
	state := CopyState{From: Position{Segment: l.nextSeq, Offset: segmentHeaderSize}, To: Position{Segment: l.nextSeq, Offset: segmentHeaderSize}}
	for _, seg := range l.live {
		state.Files = append(state.Files, CopyFile{Name: filepath.Base(seg.path), Size: seg.end})
		state.To = Position{Segment: seg.seq, Offset: seg.end}
	}
	if l.replayPin != nil {
		state.From = *l.replayPin
	}
	if l.appendPin != nil && (position{l.appendPin.Segment, l.appendPin.Offset}).less(position{state.From.Segment, state.From.Offset}) {
		state.From = *l.appendPin
	}
	r.reply <- appendStateReply{state: state}
}

// RecordAppendReplay accounts for points applied by independent startup readers.
func (l *Log) RecordAppendReplay(n int64) { l.total.replayed.Add(n) }
