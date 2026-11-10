package buckyd

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/golang-jwt/jwt/v5"
)

type nativeSourceSession struct {
	source *chunkstore.NativeSource
	timer  *time.Timer
	ctx    context.Context
	cancel context.CancelFunc
	active int
}
type nativeExportRequest struct {
	Session string
	Names   []string
}
type nativePullRequest struct {
	Range   chunkstore.NativeRange
	Session string
}

var nativeTransferClient = &http.Client{Transport: nativeTransport(), CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}

func nativeTransport() *http.Transport {
	t := bulkTransferClient.Transport.(*http.Transport).Clone()
	t.ResponseHeaderTimeout = 0
	return t
}

func (s *Service) closeNativeSource(id string) {
	s.nativeMu.Lock()
	session := s.nativeSources[id]
	delete(s.nativeSources, id)
	s.nativeMu.Unlock()
	if session != nil {
		session.timer.Stop()
		session.cancel()
		_ = session.source.Close()
	}
}
func (s *Service) expireNativeSource(id string) {
	s.nativeMu.Lock()
	session := s.nativeSources[id]
	if session == nil || session.active > 0 {
		s.nativeMu.Unlock()
		return
	}
	delete(s.nativeSources, id)
	s.nativeMu.Unlock()
	session.cancel()
	_ = session.source.Close()
}

func (s *Service) closeNativeSources() {
	s.nativeMu.Lock()
	sessions := s.nativeSources
	s.nativeSources = nil
	s.nativeMu.Unlock()
	for _, session := range sessions {
		session.timer.Stop()
		session.cancel()
	}
	for _, session := range sessions {
		session.timer.Stop()
		session.cancel()
		_ = session.source.Close()
	}
}

// The native engine intentionally does not use transfers, storagePressure or
// whole-store replacement readiness. The store owns hard integrity/disk stops.
func (s *Service) native(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path
	op := "read"
	if path == "/v1/native/pull" || path == "/v1/native/import" {
		op = "update"
	}
	if err := s.allowedRoot(op, r); err != nil {
		writeError(w, err, http.StatusForbidden)
		return
	}
	if path == "/v1/native/stats" && r.Method == http.MethodGet {
		writeNativeJSON(w, s.store.NativeStats())
		return
	}
	if path == "/v1/native/capabilities" && r.Method == http.MethodGet {
		writeNativeJSON(w, map[string]any{"Protocol": 1, "Node": s.config.Node, "CPULimit": runtime.GOMAXPROCS(0), "StoreFormat": s.store.TransferFormat(), "AcceptStoreFormat": s.store.AcceptTransferFormat(), "NativeAbsentOnly": true, "NativeExactCombine": true, "NativeDeferredCombine": true, "DurableRangeReceipt": true, "AtomicMissingOnlyNonNaN": s.finiteCacheFence != nil, "AtomicMissingOnly": s.finiteCacheFence != nil, "AtomicMissingOnlyProtocol": 1, "AtomicMissingOnlyMaxPoints": chunkstore.MissingOnlyMaxPoints, "AtomicFinitePoint": s.finiteCacheFence != nil, "AtomicFinitePointProtocol": 1, "AtomicFinitePointMaxPoints": 1})
		return
	}
	if strings.HasPrefix(path, "/v1/native/source/") && r.Method == http.MethodDelete {
		s.closeNativeSource(strings.TrimPrefix(path, "/v1/native/source/"))
		writeNativeJSON(w, map[string]bool{"released": true})
		return
	}
	if r.Method != http.MethodPost {
		writeError(w, errors.New("POST required"), http.StatusMethodNotAllowed)
		return
	}
	decode := func(out any) bool {
		if err := json.NewDecoder(r.Body).Decode(out); err != nil {
			writeError(w, err, 400)
			return false
		}
		return true
	}
	fail := func(err error) {
		status := http.StatusInternalServerError
		if errors.Is(err, pebble.ErrNotFound) || errors.Is(err, chunkstore.ErrNotFound) {
			status = 404
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
		pin, err := s.store.PinNativeSource()
		if err != nil {
			fail(err)
			return
		}
		var nonce [16]byte
		if _, err := rand.Read(nonce[:]); err != nil {
			pin.Close()
			fail(err)
			return
		}
		id := hex.EncodeToString(nonce[:])
		s.nativeMu.Lock()
		if s.nativeSources == nil {
			s.nativeSources = make(map[string]*nativeSourceSession)
		}
		sessionCtx, cancelSession := context.WithCancel(context.Background())
		session := &nativeSourceSession{source: pin, ctx: sessionCtx, cancel: cancelSession}
		session.timer = time.AfterFunc(15*time.Minute, func() { s.expireNativeSource(id) })
		s.nativeSources[id] = session
		s.nativeMu.Unlock()
		writeNativeJSON(w, map[string]any{"ID": id, "StoreFormat": s.store.TransferFormat(), "Expires": time.Now().Add(15 * time.Minute)})
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
		w.Header().Set("Content-Type", "application/vnd.go-carbon.transfer-v2")
		w.Header().Set("X-Store-Format", strconv.Itoa(s.store.TransferFormat()))
		out := &nativeResponseWriter{ResponseWriter: w}
		if err := session.source.Write(ctx, request.Names, out); err != nil {
			if out.bytes == 0 {
				fail(err)
			} else {
				log.Printf("native export interrupted: session=%s bytes=%d error=%v", request.Session, out.bytes, err)
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
	case "/v1/native/pull":
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
		response, err := s.nativeExport(r.Context(), request.Range.Source, request.Session, names)
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

func (s *Service) nativeExport(ctx context.Context, source, session string, names []string) (*http.Response, error) {
	u, err := s.transferSourceOrigin(source)
	if err != nil {
		return nil, err
	}
	u.Path = "/v1/native/export"
	data, err := json.Marshal(nativeExportRequest{Session: session, Names: names})
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
	format, _ := strconv.Atoi(response.Header.Get("X-Store-Format"))
	if format > s.store.AcceptTransferFormat() {
		response.Body.Close()
		return nil, errors.New("native source format exceeds target capability")
	}
	return response, nil
}
