package buckyd

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/golang-jwt/jwt/v5"
)

// Bulk requests use root namespace authorization once per batch. A scoped
// token cannot turn a batch endpoint into an escape from its metric scope.
func (s *Service) bulk(w http.ResponseWriter, r *http.Request) {
	op := map[string]string{"/v1/transfer/export": "read", "/v1/transfer/import": "update", "/v1/transfer/pull": "update", "/v1/transfer/verify": "read", "/v1/transfer/delete": "delete", "/v1/transfer/stage-delete": "delete", "/v1/transfer/pending-delete": "read", "/v1/transfer/retire": "delete", "/v1/transfer/status": "read", "/v1/transfer/list": "read"}[r.URL.Path]
	if op == "" {
		http.NotFound(w, r)
		return
	}
	if (r.URL.Path == "/v1/transfer/status" || r.URL.Path == "/v1/transfer/list") && r.Method == http.MethodGet {
	} else if r.Method != http.MethodPost {
		writeError(w, errors.New("POST required"), 405)
		return
	}
	if err := s.allowedRoot(op, r); err != nil {
		writeError(w, err, 403)
		return
	}
	if op != "read" {
		s.copyMu.RLock()
		defer s.copyMu.RUnlock()
		if s.StoreCopyActive() {
			writeError(w, errors.New("administrative metric mutations are fenced by whole-store copy"), http.StatusConflict)
			return
		}
	}
	if r.URL.Path == "/v1/transfer/pull" {
		if err := s.allowedRoot("read", r); err != nil {
			writeError(w, err, 403)
			return
		}
	}
	if r.URL.Path == "/v1/transfer/import" || r.URL.Path == "/v1/transfer/pull" {
		stats := s.store.Stats()
		if stats.WriteStallActive || stats.L0Sublevels >= s.store.TransferL0StopWrites()*3/4 || stats.CompactionDebt > 8<<30 {
			writeError(w, errors.New("target storage pressure; import paused"), 429)
			return
		}
	}
	if r.URL.Path == "/v1/transfer/status" {
		w.Header().Set("Content-Type", "application/json")
		hostname, _ := os.Hostname()
		_ = json.NewEncoder(w).Encode(struct {
			chunkstore.Stats
			MaxTransfers int
			L0StopWrites int64
			MaxBodyBytes int64
			Protocol     int
			Hostname     string
			CPULimit     int
		}{s.store.Stats(), s.config.MaxTransfers, s.store.TransferL0StopWrites(), s.config.MaxBodyBytes, 1, hostname, runtime.GOMAXPROCS(0)})
		return
	}
	if !s.acquireTransfer() {
		writeError(w, errors.New("too many concurrent transfers"), 429)
		return
	}
	defer s.releaseTransfer()
	r.Body = http.MaxBytesReader(w, r.Body, s.config.MaxBodyBytes)
	reply := func(value any) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(value)
	}
	replyProofs := func(proofs []chunkstore.TransferProof) {
		if r.Header.Get("Accept") != chunkstore.TransferProofContentType {
			reply(proofs)
			return
		}
		data, err := chunkstore.EncodeTransferProofs(proofs)
		if err != nil {
			writeError(w, err, 500)
			return
		}
		w.Header().Set("Content-Type", chunkstore.TransferProofContentType)
		_, _ = w.Write(data)
	}
	decode := func(value any) bool {
		d := json.NewDecoder(r.Body)
		d.DisallowUnknownFields()
		if err := d.Decode(value); err != nil {
			code := 400
			var limit *http.MaxBytesError
			if errors.As(err, &limit) {
				code = 413
			}
			writeError(w, err, code)
			return false
		}
		var extra any
		if err := d.Decode(&extra); err != io.EOF {
			writeError(w, errors.New("trailing JSON data"), 400)
			return false
		}
		return true
	}
	fail := func(err error) {
		code := 500
		if errors.Is(err, chunkstore.ErrTransferLimit) {
			code = 413
		}
		if errors.Is(err, chunkstore.ErrConflict) {
			code = 409
		}
		if errors.Is(err, chunkstore.ErrNotFound) {
			code = 404
		}
		writeError(w, err, code)
	}
	switch r.URL.Path {
	case "/v1/transfer/status":
		reply(s.store.Stats())
	case "/v1/transfer/list":
		limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
		if limit == 0 {
			limit = 10000
		}
		page, err := s.store.ListTransferNames(r.Context(), r.URL.Query().Get("prefix"), r.URL.Query().Get("after"), limit)
		if err != nil {
			fail(err)
			return
		}
		reply(page)
	case "/v1/transfer/export":
		var names []string
		if !decode(&names) {
			return
		}
		budget := s.config.MaxBodyBytes
		if raw := r.URL.Query().Get("max_bytes"); raw != "" {
			n, err := strconv.ParseInt(raw, 10, 64)
			if err != nil || n < 65536 {
				writeError(w, errors.New("invalid transfer byte budget"), 400)
				return
			}
			budget = min(budget, n)
		}
		if s.flushTransfer != nil {
			if err := s.flushTransfer(r.Context(), names); err != nil {
				fail(err)
				return
			}
		}
		records, err := s.store.ExportTransferLimit(r.Context(), names, budget)
		if err != nil {
			fail(err)
			return
		}
		// Bound the encoded response as well as imports; a dense batch must be
		// split by the caller before it can consume arbitrary server memory.
		var size int64
		write := chunkstore.WriteTransfer
		wire2 := r.URL.Query().Get("wire") == "2"
		if wire2 {
			size = chunkstore.TransferWireSize2(records)
			write = chunkstore.WriteTransfer2
		} else {
			size, err = chunkstore.TransferWireSize(records)
		}
		if err != nil || size > budget {
			writeError(w, chunkstore.ErrTransferLimit, 413)
			return
		}
		w.Header().Set("Content-Type", "application/vnd.go-carbon.transfer-v1")
		if wire2 {
			w.Header().Set("Content-Type", "application/vnd.go-carbon.transfer-v2")
		}
		w.Header().Set("Content-Length", strconv.FormatInt(size, 10))
		out := bufio.NewWriterSize(w, 256<<10)
		if err := write(out, records); err != nil {
			return
		}
		_ = out.Flush()
	case "/v1/transfer/pull":
		var request struct {
			Source   string
			Names    []string
			MaxBytes int64
		}
		if !decode(&request) {
			return
		}
		buffer := s.transferBuffer()
		defer s.transferPool.Put(buffer)
		records, n, err := s.pullTransfer(r.Context(), request.Source, request.Names, request.MaxBytes, buffer)
		if err != nil {
			var upstream *transferUpstreamError
			if errors.As(err, &upstream) {
				writeError(w, err, upstream.status)
			} else {
				fail(err)
			}
			return
		}
		proofs, err := s.store.ImportTransfer(r.Context(), records)
		if err != nil {
			fail(err)
			return
		}
		s.changed()
		w.Header().Set("X-Transfer-Bytes", strconv.FormatInt(n, 10))
		replyProofs(proofs)
	case "/v1/transfer/import":
		buffer := s.transferBuffer()
		defer s.transferPool.Put(buffer)
		records, err := buffer.Read(r.Body, s.config.MaxBodyBytes, r.ContentLength)
		if err != nil {
			writeError(w, err, 400)
			return
		}
		proofs, err := s.store.ImportTransfer(r.Context(), records)
		if err != nil {
			fail(err)
			return
		}
		s.changed()
		replyProofs(proofs)
	case "/v1/transfer/verify":
		var proofs []chunkstore.TransferProof
		if r.Header.Get("Content-Type") == chunkstore.TransferProofContentType {
			var err error
			proofs, err = chunkstore.ReadTransferProofs(r.Body)
			if err != nil {
				writeError(w, err, 400)
				return
			}
		} else if !decode(&proofs) {
			return
		}
		if len(proofs) == 0 || len(proofs) > chunkstore.MaxTransferMetrics {
			writeError(w, errors.New("invalid proof count"), 400)
			return
		}
		if err := s.store.VerifyTransfer(r.Context(), proofs); err != nil {
			fail(err)
			return
		}
		reply(map[string]int{"verified": len(proofs)})
	case "/v1/transfer/pending-delete":
		var request struct{ Operation string }
		if !decode(&request) {
			return
		}
		names, err := s.store.PendingTransferDelete(r.Context(), request.Operation, 256)
		if err != nil {
			fail(err)
			return
		}
		reply(names)
	case "/v1/transfer/stage-delete":
		var request struct {
			Operation string
			Versions  []chunkstore.TransferVersion
		}
		if !decode(&request) {
			return
		}
		if err := s.store.StageTransferDelete(r.Context(), request.Operation, request.Versions); err != nil {
			fail(err)
			return
		}
		reply(map[string]int{"staged": len(request.Versions)})
	case "/v1/transfer/retire":
		var guard struct {
			Confirm   bool
			Operation string
		}
		if !decode(&guard) {
			return
		}
		if !guard.Confirm {
			writeError(w, errors.New("retirement confirmation missing"), 400)
			return
		}
		n, err := s.store.RetireTransferredStore(r.Context(), guard.Operation)
		if err != nil {
			fail(err)
			return
		}
		s.changed()
		reply(map[string]int64{"deleted": n})
	case "/v1/transfer/delete":
		var versions []chunkstore.TransferVersion
		if !decode(&versions) {
			return
		}
		stats := s.store.Stats()
		if stats.WriteStallActive || stats.L0Sublevels >= s.store.TransferL0StopWrites()*3/4 || stats.CompactionDebt > 8<<30 {
			writeError(w, errors.New("source storage pressure; deletion paused"), 429)
			return
		}
		if err := s.store.DeleteTransfer(r.Context(), versions); err != nil {
			fail(err)
			return
		}
		s.changed()
		reply(map[string]int{"deleted": len(versions)})
	}
}

// allowedTransferSource reports whether host is a ring node or a configured
// transfer source. Pulls send a signed read token, so they never go to an
// arbitrary caller-chosen host.
func (s *Service) allowedTransferSource(host string) bool {
	for _, node := range s.nodes {
		if strings.EqualFold(node.Server, host) {
			return true
		}
	}
	for _, source := range s.config.TransferSources {
		if name, _, err := net.SplitHostPort(source); err == nil {
			source = name
		}
		if strings.EqualFold(source, host) {
			return true
		}
	}
	return false
}

func (s *Service) transferBuffer() *chunkstore.TransferBuffer {
	if buffer := s.transferPool.Get(); buffer != nil {
		return buffer.(*chunkstore.TransferBuffer)
	}
	return &chunkstore.TransferBuffer{}
}

func (s *Service) pullTransfer(ctx context.Context, source string, names []string, maxBytes int64, buffer *chunkstore.TransferBuffer) ([]chunkstore.TransferMetric, int64, error) {
	u, err := url.Parse(source)
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil || strings.Trim(u.Path, "/") != "" || u.RawQuery != "" || u.Fragment != "" {
		return nil, 0, errors.New("invalid source origin")
	}
	if !s.allowedTransferSource(u.Hostname()) {
		return nil, 0, &transferUpstreamError{403, fmt.Sprintf("source %s is neither a ring node nor a configured transfer source", u.Hostname())}
	}
	body, err := json.Marshal(names)
	if err != nil {
		return nil, 0, err
	}
	u.Path = "/v1/transfer/export"
	if maxBytes <= 0 || maxBytes > s.config.MaxBodyBytes {
		maxBytes = s.config.MaxBodyBytes
	}
	query := u.Query()
	query.Set("max_bytes", strconv.FormatInt(maxBytes, 10))
	query.Set("wire", "2")
	u.RawQuery = query.Encode()
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, u.String(), bytes.NewReader(body))
	if err != nil {
		return nil, 0, err
	}
	request.Header.Set("Content-Type", "application/json")
	if len(s.secret) > 0 {
		signed, err := jwt.NewWithClaims(jwt.SigningMethodHS256, ACL{RegisteredClaims: jwt.RegisteredClaims{ExpiresAt: jwt.NewNumericDate(time.Now().Add(5 * time.Minute))}, Namespaces: []string{"*"}, Ops: []string{"read"}}).SignedString(s.secret)
		if err != nil {
			return nil, 0, err
		}
		request.Header.Set(authHeader, signed)
	}
	response, err := bulkTransferClient.Do(request)
	if err != nil {
		return nil, 0, err
	}
	defer response.Body.Close()
	if response.StatusCode != 200 {
		return nil, 0, &transferUpstreamError{response.StatusCode, fmt.Sprintf("source export: %s", response.Status)}
	}
	input := &transferCountReader{Reader: response.Body}
	records, err := buffer.Read(input, maxBytes, response.ContentLength)
	if err != nil {
		return nil, 0, err
	}
	return records, input.n, nil
}

type transferUpstreamError struct {
	status  int
	message string
}

func (e *transferUpstreamError) Error() string { return e.message }

var bulkTransferClient = &http.Client{Timeout: 10 * time.Minute, Transport: &http.Transport{MaxIdleConns: 256, MaxIdleConnsPerHost: 64, ResponseHeaderTimeout: 90 * time.Second}, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}

type transferCountReader struct {
	io.Reader
	n int64
}

func (r *transferCountReader) Read(p []byte) (int, error) {
	n, err := r.Reader.Read(p)
	r.n += int64(n)
	return n, err
}
