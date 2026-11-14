package buckyd

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/golang-jwt/jwt/v5"
	"github.com/lomik/zapwriter"
	"go.uber.org/zap"
)

type nativeSourceSession struct {
	source     *chunkstore.NativeSource
	timer      *time.Timer
	ctx        context.Context
	cancel     context.CancelFunc
	active     int
	createdAt  time.Time
	closeError string
}
type nativeExportRequest struct {
	Window  *chunkstore.NativeWindow `json:",omitempty"`
	Session string
	Names   []string
}
type nativePullRequest struct {
	Range   chunkstore.NativeRange
	Session string
}

const nativeMergeHeader = "X-Native-Merge-Session"

var nativeTransferClient = &http.Client{Transport: nativeTransport(), CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}

func nativeTransport() *http.Transport {
	t := bulkTransferClient.Transport.(*http.Transport).Clone()
	t.ResponseHeaderTimeout = 0
	return t
}

func (s *Service) closeNativeSource(id string) error {
	s.nativeMu.Lock()
	session := s.nativeSources[id]
	if session == nil {
		pending := s.nativeClosing[id] != nil || !s.nativeCreating[id].IsZero()
		s.nativeMu.Unlock()
		if pending {
			return errors.New("native source creation or close still pending")
		}
		return nil
	}
	delete(s.nativeSources, id)
	s.trackNativeClosingLocked(id, session)
	s.nativeMu.Unlock()
	return s.finishNativeClose(id, session)
}
func (s *Service) expireNativeSource(id string) {
	s.nativeMu.Lock()
	session := s.nativeSources[id]
	if session == nil || session.active > 0 {
		s.nativeMu.Unlock()
		return
	}
	delete(s.nativeSources, id)
	s.trackNativeClosingLocked(id, session)
	s.nativeMu.Unlock()
	_ = s.finishNativeClose(id, session)
}
func (s *Service) closeNativeSources() {
	s.nativeMu.Lock()
	s.nativeSourceStopped = true
	sessions := s.nativeSources
	s.nativeSources = nil
	for id, session := range sessions {
		s.trackNativeClosingLocked(id, session)
	}
	s.nativeMu.Unlock()
	for _, session := range sessions {
		session.timer.Stop()
		session.cancel()
	}
	for id, session := range sessions {
		_ = s.finishNativeClose(id, session)
	}
}

// The native engine intentionally does not use transfers, storagePressure or
// whole-store replacement readiness. The store owns hard integrity/disk stops.
func (s *Service) native(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path
	if path == "/v1/native/pull" || path == "/v1/native/export" || path == "/v1/native/abandon" {
		ctx, cancel := s.store.NativeTransferContext(r.Context())
		defer cancel()
		r = r.WithContext(ctx)
	}
	op := "read"
	if path == "/v1/native/pull" || path == "/v1/native/import" || path == "/v1/native/abandon" {
		op = "update"
	}
	if err := s.allowedRoot(op, r); err != nil {
		writeError(w, err, http.StatusForbidden)
		return
	}
	if path == "/v1/native/sources" && r.Method == http.MethodGet {
		writeNativeJSON(w, s.nativeSourceCensus())
		return
	}
	if path == "/v1/native/stats" && r.Method == http.MethodGet {
		writeNativeJSON(w, s.store.NativeStats())
		return
	}
	if path == "/v1/native/capabilities" && r.Method == http.MethodGet {
		writeNativeJSON(w, map[string]any{"NativeSourceCensus": true, "NativeSourceCensusProtocol": 1, "Protocol": 1, "Node": s.config.Node, "CPULimit": runtime.GOMAXPROCS(0), "NativeImportConcurrency": s.store.NativeImportConcurrency(), "StoreFormat": s.store.TransferFormat(), "AcceptStoreFormat": s.store.AcceptTransferFormat(), "NativeAbsentOnly": true, "NativeExactCombine": true, "NativeTimeWindow": true, "NativeDeferredCombine": true, "NativePinnedMerge": true, "DurableRangeReceipt": true})
		return
	}
	if strings.HasPrefix(path, "/v1/native/source/") && r.Method == http.MethodDelete {
		if err := s.closeNativeSource(strings.TrimPrefix(path, "/v1/native/source/")); err != nil {
			writeError(w, err, http.StatusConflict)
			return
		}
		writeNativeJSON(w, map[string]bool{"released": true})
		return
	}
	if r.Method != http.MethodPost {
		writeError(w, errors.New("POST required"), http.StatusMethodNotAllowed)
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 8<<20)
	decode := func(out any) bool {
		if err := json.NewDecoder(r.Body).Decode(out); err != nil {
			s.recordNativeRejection(r.URL.Path, err)
			writeError(w, err, bodyErrorStatus(err))
			return false
		}
		return true
	}
	fail := func(err error) {
		s.recordNativeRejection(r.URL.Path, err)
		status := http.StatusInternalServerError
		if errors.Is(err, pebble.ErrNotFound) || errors.Is(err, chunkstore.ErrNotFound) {
			status = 404
		}
		if errors.Is(err, chunkstore.ErrTransferLimit) || errors.Is(err, chunkstore.ErrNameTooLong) {
			status = http.StatusRequestEntityTooLarge
		}
		if errors.Is(err, chunkstore.ErrFormat) {
			status = 400
		}
		if errors.Is(err, chunkstore.ErrConflict) {
			status = 409
		}
		writeError(w, err, status)
	}
	switch path {
	case "/v1/native/source":
		var request struct{ MergeSession string }
		decoder := json.NewDecoder(r.Body)
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&request); err != nil && !errors.Is(err, io.EOF) {
			writeError(w, err, bodyErrorStatus(err))
			return
		}
		id, session, err := s.openNativeSource(r.Context(), func() (*chunkstore.NativeSource, error) {
			if request.MergeSession == "" {
				return s.store.PinNativeSource()
			}
			return s.store.PinNativeMergeSource(request.MergeSession)
		})
		if err != nil {
			fail(err)
			return
		}
		pin := session.source
		writeNativeJSON(w, map[string]any{"ID": id, "StoreFormat": s.store.TransferFormat(), "Expires": time.Now().Add(15 * time.Minute), "MergeSession": pin.MergeSession, "SnapshotStartedAt": pin.SnapshotStartedAt, "SnapshotPinnedAt": pin.SnapshotPinnedAt, "BackfillFromUnix": int64(0), "BackfillFullRetention": pin.MergeSession != ""})
	case "/v1/native/export":
		var request nativeExportRequest
		if !decode(&request) {
			return
		}
		s.nativeMu.Lock()
		session := s.nativeSources[request.Session]
		if session != nil {
			session.active++
			session.timer.Stop()
		}
		s.nativeMu.Unlock()
		if session == nil {
			writeError(w, errors.New("native source session expired or daemon restarted"), http.StatusGone)
			return
		}
		ctx, cancel := context.WithCancel(r.Context())
		stopSession := context.AfterFunc(session.ctx, cancel)
		stopWrite := context.AfterFunc(ctx, func() { _ = http.NewResponseController(w).SetWriteDeadline(time.Now()) })
		defer func() {
			stopWrite()
			stopSession()
			cancel()
			s.nativeMu.Lock()
			session.active--
			if session.active == 0 && s.nativeSources[request.Session] == session {
				session.timer.Reset(15 * time.Minute)
			}
			s.nativeMu.Unlock()
		}()
		contentType := "application/vnd.go-carbon.transfer-v2"
		if request.Window != nil {
			contentType = "application/vnd.go-carbon.transfer-v3"
		}
		w.Header().Set("Content-Type", contentType)
		w.Header().Set("X-Store-Format", strconv.Itoa(s.store.TransferFormat()))
		if session.source.MergeSession != "" {
			w.Header().Set(nativeMergeHeader, session.source.MergeSession)
		}
		out := &nativeResponseWriter{ResponseWriter: w}
		if err := session.source.WriteWindow(ctx, request.Names, out, request.Window); err != nil {
			if out.bytes == 0 {
				fail(err)
			} else {
				s.recordNativeRejection(r.URL.Path, err)
			}
		}
	case "/v1/native/receipt":
		var request chunkstore.NativeRange
		if !decode(&request) {
			return
		}
		receipt, err := s.store.NativeReceipt(r.Context(), request)
		if err != nil {
			fail(err)
			return
		}
		writeNativeJSON(w, receipt)
	case "/v1/native/pull", "/v1/native/abandon":
		if err := s.allowedRoot("read", r); err != nil {
			writeError(w, err, 403)
			return
		}
		s.copyMu.RLock()
		defer s.copyMu.RUnlock()
		if s.StoreCopyActive() {
			writeError(w, errors.New("whole-store replacement active"), 409)
			return
		}
		// Acquire before decoding or opening the donor response: queued pulls
		// retain neither range metadata nor donor history.
		s.nativePullOnce.Do(func() { s.nativePullAdmission = make(chan struct{}, s.store.NativeImportConcurrency()) })
		select {
		case s.nativePullAdmission <- struct{}{}:
			defer func() { <-s.nativePullAdmission }()
		case <-r.Context().Done():
			fail(r.Context().Err())
			return
		}
		if err := r.Context().Err(); err != nil {
			fail(err)
			return
		}
		if path == "/v1/native/abandon" {
			var request struct{ Identity string }
			if !decode(&request) {
				return
			}
			receipt, err := s.store.AbandonNativeCombine(r.Context(), request.Identity)
			if err != nil {
				fail(err)
				return
			}
			writeNativeJSON(w, receipt)
			return
		}

		var request nativePullRequest
		if !decode(&request) {
			return
		}
		if receipt, err := s.store.NativeReceipt(r.Context(), request.Range); err == nil {
			writeNativeJSON(w, receipt)
			return
		} else if !errors.Is(err, pebble.ErrNotFound) {
			fail(err)
			return
		}
		if receipt, ready, err := s.store.ResumeNativeCombine(r.Context(), request.Range); err != nil {
			fail(err)
			return
		} else if ready {
			s.changed()
			writeNativeJSON(w, receipt)
			return
		}
		if err := chunkstore.ValidateNativeNames(request.Range.Names); err != nil {
			fail(err)
			return
		}
		candidates, err := s.store.NativeCandidates(r.Context(), request.Range.Names)
		if err != nil {
			fail(err)
			return
		}
		if request.Range.Combine {
			for i := range request.Range.Names {
				if request.Range.Selection == nil || request.Range.Selection[i/8]&(1<<uint(i%8)) != 0 {
					candidates[i/8] |= 1 << uint(i%8)
				} else {
					candidates[i/8] &^= 1 << uint(i%8)
				}
			}
		}
		request.Range.Candidates = candidates
		var names []string
		for i, name := range request.Range.Names {
			if candidates[i/8]&(1<<uint(i%8)) != 0 {
				names = append(names, name)
			}
		}
		response, err := s.nativeExport(r.Context(), request.Range.Source, request.Session, names, request.Range.Window, request.Range.MergeSession, request.Range.Combine)
		if err != nil {
			fail(err)
			return
		}
		defer response.Body.Close()
		receipt, err := s.store.NativeImport(r.Context(), request.Range, response.Body)
		if err != nil {
			fail(err)
			return
		}
		s.changed()
		writeNativeJSON(w, receipt)
	default:
		http.NotFound(w, r)
	}
}

type nativeResponseWriter struct {
	http.ResponseWriter
	bytes int64
}

func (w *nativeResponseWriter) Write(data []byte) (int, error) {
	n, err := w.ResponseWriter.Write(data)
	w.bytes += int64(n)
	return n, err
}
func writeNativeJSON(w http.ResponseWriter, value any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(value)
}

func (s *Service) nativeExport(ctx context.Context, source, session string, names []string, window *chunkstore.NativeWindow, mergeSession string, combine bool) (*http.Response, error) {
	u, err := s.transferSourceOrigin(source)
	if err != nil {
		return nil, err
	}
	u.Path = "/v1/native/export"
	data, err := json.Marshal(nativeExportRequest{Session: session, Names: names, Window: window})
	if err != nil {
		return nil, err
	}
	r, err := http.NewRequestWithContext(ctx, http.MethodPost, u.String(), bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	r.Header.Set("Content-Type", "application/json")
	if len(s.secret) > 0 {
		token, err := jwt.NewWithClaims(jwt.SigningMethodHS256, ACL{RegisteredClaims: jwt.RegisteredClaims{ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour))}, Namespaces: []string{"*"}, Ops: []string{"read"}}).SignedString(s.secret)
		if err != nil {
			return nil, err
		}
		r.Header.Set(authHeader, token)
	}
	response, err := nativeTransferClient.Do(r)
	if err != nil {
		return nil, err
	}
	if response.StatusCode != http.StatusOK {
		defer response.Body.Close()
		body, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
		return nil, fmt.Errorf("native source export HTTP%d: %s", response.StatusCode, body)
	}
	mode := response.Header.Get(nativeMergeHeader)
	if mode != mergeSession || mode != "" && combine {
		response.Body.Close()
		return nil, errors.New("pinned merge source requires matching absent-only merge session")
	}
	format, _ := strconv.Atoi(response.Header.Get("X-Store-Format"))
	if format > s.store.AcceptTransferFormat() {
		response.Body.Close()
		return nil, errors.New("native source format exceeds target capability")
	}
	return response, nil
}

// NativeRejectedRequests includes admission and decoding failures before import starts.
func (s *Service) NativeRejectedRequests() uint64 { return s.nativeRejectedRequests.Load() }

func (s *Service) recordNativeRejection(path string, err error) {
	s.nativeRejectedRequests.Add(1)
	zapwriter.Logger("buckyd").Error("native request rejected", zap.String("path", path), zap.Error(err))
}
