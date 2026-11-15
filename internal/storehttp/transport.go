// Package storehttp retires connections to store processes during handover.
package storehttp

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptrace"
	"strings"
	"sync"
	"syscall"
	"time"
)

// Transport keeps independent pools per origin. Replacing a pool also prevents
// active requests to the old process from returning usable idle connections.
// Operation-level retries still own pins, receipts and durable progress.
type Transport struct {
	*http.Transport
	mu      sync.Mutex
	origins map[string]*http.Transport
	// OnRestart invalidates caller-owned negotiation state. Set before use;
	// the callback must be safe for concurrent calls and must not block.
	OnRestart func(string)
}

func NewTransport(base *http.Transport) *Transport {
	base = base.Clone()
	// Short copy invocations and long controllers both need bounded pools.
	// Preserve an explicitly configured limit; the default leaves room for all
	// normal transfer streams without retaining unbounded idle sockets.
	if base.MaxConnsPerHost == 0 {
		base.MaxConnsPerHost = max(1024, base.MaxIdleConnsPerHost)
	}
	if base.IdleConnTimeout == 0 {
		base.IdleConnTimeout = 90 * time.Second
	}
	return &Transport{Transport: base, origins: make(map[string]*http.Transport)}
}

func (t *Transport) pool(origin string) *http.Transport {
	t.mu.Lock()
	defer t.mu.Unlock()
	if p := t.origins[origin]; p != nil {
		return p
	}
	p := t.Transport.Clone()
	t.origins[origin] = p
	return p
}

func (t *Transport) retire(origin string, p *http.Transport) {
	t.mu.Lock()
	current := t.origins[origin] == p
	if current {
		delete(t.origins, origin)
	}
	t.mu.Unlock()
	p.CloseIdleConnections()
	if current && t.OnRestart != nil {
		t.OnRestart(origin)
	}
}

// CloseOriginConnections retires one origin, including its future idle returns.
func (t *Transport) CloseOriginConnections(origin string) {
	t.mu.Lock()
	p := t.origins[origin]
	t.mu.Unlock()
	if p != nil {
		t.retire(origin, p)
	}
}

// CloseOriginConnections uses origin-scoped eviction when supported.
func CloseOriginConnections(transport http.RoundTripper, origin string) {
	if closer, ok := transport.(interface{ CloseOriginConnections(string) }); ok {
		closer.CloseOriginConnections(origin)
	} else if closer, ok := transport.(interface{ CloseIdleConnections() }); ok {
		closer.CloseIdleConnections()
	}
}

func (t *Transport) CloseIdleConnections() {
	t.mu.Lock()
	defer t.mu.Unlock()
	for origin, p := range t.origins {
		delete(t.origins, origin)
		p.CloseIdleConnections()
	}
}

func (t *Transport) RoundTrip(req *http.Request) (*http.Response, error) {
	origin := req.URL.Scheme + "://" + req.URL.Host
	for attempt := 0; ; attempt++ {
		pool := t.pool(origin)
		reused := false
		trace := &httptrace.ClientTrace{GotConn: func(info httptrace.GotConnInfo) { reused = info.Reused }}
		r := req.Clone(httptrace.WithClientTrace(req.Context(), trace))
		response, err := pool.RoundTrip(r)
		retry := readOnly(req) && err != nil && (errors.Is(err, syscall.ECONNREFUSED) || reused && (errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, syscall.ECONNRESET) || errors.Is(err, syscall.EPIPE)))
		if err != nil && !LocalError(err) {
			t.retire(origin, pool)
		} else if err == nil {
			response.Body = &restartBody{ReadCloser: response.Body, retire: func() { t.retire(origin, pool) }}
		}
		if err == nil && response.StatusCode == http.StatusServiceUnavailable {
			// Retire before reading to EOF can return the socket to the idle pool.
			t.retire(origin, pool)
			prefix, readErr := io.ReadAll(io.LimitReader(response.Body, 4096))
			response.Body = &prefixedBody{Reader: io.MultiReader(bytes.NewReader(prefix), response.Body), Closer: response.Body}
			retry = readErr == nil && strings.Contains(strings.ToLower(string(prefix)), "stopping")
		}
		if !retry || attempt != 0 || req.Context().Err() != nil || req.Body != nil && req.Body != http.NoBody && req.GetBody == nil {
			return response, err
		}
		// A stopping response rejects work before dispatch. Ambiguous failures
		// of mutations return to the journal/receipt owner for a safe retry.
		if response != nil {
			response.Body.Close()
		}
		req = req.Clone(req.Context())
		if req.GetBody != nil {
			req.Body, err = req.GetBody()
			if err != nil {
				return nil, err
			}
		}
	}
}

type prefixedBody struct {
	io.Reader
	io.Closer
}

// A reset can lose an acknowledgement. Only repeat reads here; operation-level
// code resolves mutating requests through durable receipts before retrying.
func readOnly(r *http.Request) bool {
	if r.Method == http.MethodGet || r.Method == http.MethodHead {
		return true
	}
	switch r.URL.Path {
	case "/info/", "/render/", "/metrics/find/", "/v1/native/export", "/v1/transfer/export", "/v1/transfer/digest", "/v1/transfer/verify":
		return true
	}
	return false
}

// Body failures happen after RoundTrip has returned. Retire the origin before
// the journal owner retries an interrupted export or receipt response.
type restartBody struct {
	io.ReadCloser
	once   sync.Once
	retire func()
}

func (b *restartBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	if err != nil && err != io.EOF && !LocalError(err) {
		b.once.Do(b.retire)
	}
	return n, err
}

// LocalError cannot be fixed by replacing a store connection generation.
// Evicting healthy sockets on cancellation or local port exhaustion amplifies
// the resource shortage and needlessly invalidates native source sessions.
func LocalError(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, syscall.EADDRNOTAVAIL) || errors.Is(err, syscall.EMFILE) || errors.Is(err, syscall.ENFILE)
}
