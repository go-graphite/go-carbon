package buckyd

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"os"
	"sort"
	"time"

	"github.com/go-graphite/go-carbon/internal/chunkstore"
)

// Reserve before PinNativeSource: a timed-out create may already hold a view
// while its pending-name snapshot is still running. Census must include it.
func (s *Service) openNativeSource(ctx context.Context, pin func() (*chunkstore.NativeSource, error)) (string, *nativeSourceSession, error) {
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return "", nil, err
	}
	id := hex.EncodeToString(nonce[:])
	created := time.Now().UTC()
	s.nativeMu.Lock()
	if s.nativeSourceStopped {
		s.nativeMu.Unlock()
		return "", nil, errors.New("native source service closing")
	}
	if s.nativeCreating == nil {
		s.nativeCreating = make(map[string]time.Time)
	}
	if s.nativeSources[id] != nil || s.nativeClosing[id] != nil || !s.nativeCreating[id].IsZero() {
		s.nativeMu.Unlock()
		return "", nil, errors.New("native source ID collision")
	}
	s.nativeCreating[id] = created
	s.nativeMu.Unlock()
	published := false
	defer func() {
		if !published {
			s.nativeMu.Lock()
			delete(s.nativeCreating, id)
			s.nativeMu.Unlock()
		}
	}()
	if err := ctx.Err(); err != nil {
		return "", nil, err
	}
	source, err := pin()
	if err != nil {
		return "", nil, err
	}
	sessionCtx, cancel := context.WithCancel(context.Background())
	session := &nativeSourceSession{source: source, ctx: sessionCtx, cancel: cancel, createdAt: created}
	s.nativeMu.Lock()
	if err = ctx.Err(); err != nil || s.nativeSourceStopped {
		if err == nil {
			err = errors.New("native source service closing")
		}
		delete(s.nativeCreating, id)
		s.trackNativeClosingLocked(id, session)
		s.nativeMu.Unlock()
		return "", nil, errors.Join(err, s.finishNativeClose(id, session))
	}
	if s.nativeSources == nil {
		s.nativeSources = make(map[string]*nativeSourceSession)
	}
	session.timer = time.AfterFunc(15*time.Minute, func() { s.expireNativeSource(id) })
	delete(s.nativeCreating, id)
	s.nativeSources[id] = session
	published = true
	s.nativeMu.Unlock()
	return id, session, nil
}

func (s *Service) trackNativeClosingLocked(id string, session *nativeSourceSession) {
	if s.nativeClosing == nil {
		s.nativeClosing = make(map[string]*nativeSourceSession)
	}
	s.nativeClosing[id] = session
}

func (s *Service) finishNativeClose(id string, session *nativeSourceSession) error {
	if session.timer != nil {
		session.timer.Stop()
	}
	session.cancel()
	err := session.source.Close()
	s.nativeMu.Lock()
	defer s.nativeMu.Unlock()
	if err != nil {
		session.closeError = err.Error()
	} else if s.nativeClosing[id] == session {
		delete(s.nativeClosing, id)
	}
	return err
}

// Current-process census only. Byte attribution is unavailable, not zero.
// A completed census never rewrites historical client creation/release receipts.
func (s *Service) nativeSourceCensus() map[string]any {
	now := time.Now().UTC()
	s.nativeMu.Lock()
	defer s.nativeMu.Unlock()
	rows := make([]map[string]any, 0, len(s.nativeCreating)+len(s.nativeSources)+len(s.nativeClosing))
	add := func(id, state string, created time.Time, session *nativeSourceSession) {
		row := map[string]any{"ID": id, "State": state, "Owner": "UNKNOWN", "PinnedBytes": nil, "PinnedBytesKnown": false, "PinMayBeHeld": true, "CreatedAt": nil, "AgeSeconds": nil, "ActiveExports": 0}
		if !created.IsZero() {
			row["CreatedAt"] = created
			row["AgeSeconds"] = max(0, now.Sub(created).Seconds())
		}
		if session != nil {
			row["ActiveExports"] = session.active
			row["SnapshotStartedAt"] = session.source.SnapshotStartedAt
			row["SnapshotPinnedAt"] = session.source.SnapshotPinnedAt
			if session.closeError != "" {
				row["State"] = "CLOSE-FAILED"
				row["Error"] = session.closeError
			}
		}
		rows = append(rows, row)
	}
	for id, created := range s.nativeCreating {
		add(id, "CREATING", created, nil)
	}
	for id, session := range s.nativeSources {
		add(id, "READY", session.createdAt, session)
	}
	for id, session := range s.nativeClosing {
		add(id, "CLOSING", session.createdAt, session)
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i]["ID"].(string) < rows[j]["ID"].(string) })
	return map[string]any{"Protocol": 1, "Scope": "HTTP-native-source-sessions", "PID": os.Getpid(), "AsOf": now, "Complete": true, "CurrentProcessOnly": true, "CreatingRegisteredBeforePin": true, "IncludesClosing": true, "HistoricalClosureInferred": false, "ServiceClosing": s.nativeSourceStopped, "Count": len(rows), "PinnedBytes": nil, "PinnedBytesKnown": false, "PinnedBytesStatus": "UNAVAILABLE: no per-snapshot byte attribution", "Sources": rows}
}
