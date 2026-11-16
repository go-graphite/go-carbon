package downsize

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"net/http"
	"strings"
	"sync"
	"syscall"
	"time"
)

// hostWaiter is the shared wait/retry policy for copy commands that talk to
// buckyd origins (copy today, sync next): a host that does not answer its
// status endpoint is restarting and is waited for without limit; transient
// failures unrelated to a restart fail loudly after stallTimeout.
type hostWaiter struct {
	m                              *mover
	logMu                          sync.Mutex
	lastLog                        time.Time
	observeWait                    func(bool)
	tick, maxBackoff, stallTimeout time.Duration
	progress                       *copyProgress // optional
}

// now uses the same stall clock as the progress reporter.
func (w *hostWaiter) now() time.Time {
	if w.progress != nil {
		return w.progress.now()
	}
	return time.Now()
}

var errCopyStall = errors.New("copy stalled")

// retryTransient retries guard responses (429 left by request, 502/503/504),
// refused/reset connections and other transport errors with capped
// exponential backoff for as long as a source or target restart takes. Only
// non-restart --stall-timeout without a successful call ends it. Definitive API
// errors and idle timeouts return at once.
func (w *hostWaiter) retryTransient(ctx context.Context, path string, hosts []string, call func() error) error {
	delay := w.tick
	waiting := false
	defer func() {
		if waiting {
			observeStoreWait(-1, false)
			if w.progress != nil {
				w.progress.restartWaiting.Add(-1)
			}
			if w.observeWait != nil {
				w.observeWait(false)
			}
		}
	}()
	var since, first time.Time
	wasDown := false
	for attempt := 1; ; attempt++ {
		err := call()
		if err == nil || ctx.Err() != nil {
			if err == nil {
				w.progress.up(hosts...)
			}
			if err == nil && !first.IsZero() {
				event("guard_recovered", map[string]any{"path": path, "attempts": attempt, "retry_seconds": time.Since(first).Seconds()})
			}
			return err
		}
		if !transientCopyError(err) {
			return err
		}
		now := w.now()
		if first.IsZero() {
			first = now
		}
		// A host that does not answer its status endpoint is restarting:
		// wait for it without limit. The stall clock runs only while every
		// host involved answers and the call still fails.
		downHost := w.firstDown(ctx, hosts)
		down := downHost != ""
		if down {
			w.progress.down(downHost)
		} else {
			w.progress.up(hosts...)
		}
		w.progress.retried()
		// The stall clock starts at the first failure seen with every host
		// up: a call refused just before the host came back is not a stall.
		if since.IsZero() || down || wasDown {
			since = now
		}
		wasDown = down
		if stuck := w.now().Sub(since); !down && !restartCopyError(err) && stuck >= w.stallTimeout {
			event("copy_stalled", map[string]any{"level": "error", "path": path, "hosts": hosts, "attempts": attempt, "stuck_seconds": stuck.Seconds(), "stall_timeout": w.stallTimeout.String(), "error": err.Error()})
			return fmt.Errorf("%w: %s: no progress for %s with every host up (--stall-timeout %s): %w", errCopyStall, path, stuck.Round(time.Second), w.stallTimeout, err)
		}
		delta := int64(0)
		if !waiting && restartCopyError(err) {
			delta = 1
			waiting = true
			if w.progress != nil {
				w.progress.restartWaiting.Add(1)
			}
			if w.observeWait != nil {
				w.observeWait(true)
			}
		}
		observeStoreWait(delta, true)
		w.logMu.Lock()
		if time.Since(w.lastLog) >= time.Minute {
			w.lastLog = time.Now()
			event("guard_retry", map[string]any{"level": "warn", "path": path, "attempt": attempt, "delay_seconds": delay.Seconds(), "host_down": down, "retry_seconds": time.Since(first).Seconds(), "error": err.Error()})
		}
		w.logMu.Unlock()
		if attempt == 1 && restartCopyError(err) {
			continue
		}
		if err := sleepContext(ctx, delay/2+rand.N(delay/2+1)); err != nil {
			return err
		}
		delay = min(2*delay, w.maxBackoff)
	}
}

// restartCause matches a target 500 whose cause is the source export
// transport (refused, reset or cut while the source restarts), as the target
// buckyd reports it.
var restartCause = []string{"connection refused", "connection reset", "broken pipe", "unexpected EOF", ": EOF", "i/o timeout", "no route to host", "server closed idle connection", "timeout awaiting response headers", "application lifecycle is busy; retry native copy", "store recovery has not finished", "store recovery is not finished", "not ready", "native source session expired or daemon restarted", "context canceled"}

// transientCopyError reports errors a host restart produces: guard and
// gateway responses, a target 500 caused by a source transport error, and any
// non-API (transport, decode of a cut response) error.
func transientCopyError(err error) bool {
	var api *apiError
	var idle *transferIdleError
	var dns *net.DNSError
	switch {
	case errors.As(err, &idle), errors.Is(err, errCopyProof), errors.Is(err, errCopyStall):
		return false
	case errors.As(err, &dns) && dns.IsNotFound:
		// A misspelt origin fails at once instead of waiting forever.
		return false
	case errors.As(err, &api):
		switch api.Status {
		case http.StatusTooManyRequests, http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout, http.StatusRequestTimeout:
			return true
		case http.StatusInternalServerError:
			if nativeSourceUnavailable(api.Message) {
				return true
			}
			// Native readiness takes the application lifecycle lock. Other
			// concurrent imports can hold it briefly without a host restart.
			if strings.HasSuffix(strings.TrimSpace(api.Message), "HTTP 500 application lifecycle is busy; retry native copy") {
				return true
			}
			for _, cause := range restartCause {
				if strings.Contains(api.Message, cause) {
					return true
				}
			}
		}
		return false
	}
	return true
}

func (w *hostWaiter) status(ctx context.Context, endpoint string, out *copyStatus) error {
	return w.retryTransient(ctx, endpoint+"/v1/transfer/status", []string{endpoint}, func() error {
		*out = copyStatus{}
		_, err := w.m.request(ctx, endpoint, "/v1/transfer/status", nil, out)
		return err
	})
}

// awaitHosts probes both hosts once after a definitive-looking batch error.
// When either does not answer, it waits (bounded by --stall-timeout) until
// both do and reports true so the caller re-pulls the batch unchanged.
func (w *hostWaiter) awaitHosts(ctx context.Context, hosts []string, cause error) (bool, error) {
	waited := false
	for _, endpoint := range hosts {
		var probe copyStatus
		switch err := w.probe(ctx, endpoint); {
		case err == nil:
			continue
		case ctx.Err() != nil:
			return false, ctx.Err()
		case !transientCopyError(err):
			continue
		default:
			w.progress.down(endpoint)
			event("host_down", map[string]any{"level": "warn", "host": endpoint, "error": err.Error(), "batch_error": cause.Error()})
		}
		if err := w.status(ctx, endpoint, &probe); err != nil {
			return false, err
		}
		waited = true
	}
	return waited, nil
}

// probe asks one host for its status once, bounded to 10 s.
func (w *hostWaiter) probe(ctx context.Context, endpoint string) error {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	var status copyStatus
	_, err := w.m.requestOnce(ctx, endpoint, "/v1/transfer/status", nil, &status)
	return err
}

// firstDown returns the first host that does not answer its status
// endpoint, or "" when all answer.
func (w *hostWaiter) firstDown(ctx context.Context, hosts []string) string {
	for _, endpoint := range hosts {
		if w.probe(ctx, endpoint) != nil {
			return endpoint
		}
	}
	return ""
}

// restartCopyError excludes validation, authorization and storage failures.
func restartCopyError(err error) bool {
	var api *apiError
	var h *syncHTTPError
	if errors.As(err, &h) {
		api = &apiError{Status: h.status, Message: h.body}
	} else {
		_ = errors.As(err, &api)
	}
	if api != nil {
		if api.Status == 503 || api.Status == 502 || api.Status == 504 {
			return true
		}
		if api.Status == 500 || api.Status == 410 {
			if api.Status == 500 && nativeSourceUnavailable(api.Message) {
				return true
			}
			msg := strings.ToLower(api.Message)
			for _, cause := range restartCause {
				if strings.Contains(msg, strings.ToLower(cause)) {
					return true
				}
			}
		}
		return false
	}
	var dns *net.DNSError
	if errors.As(err, &dns) && dns.IsNotFound {
		return false
	}
	var ne net.Error
	return errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, syscall.ECONNREFUSED) || errors.Is(err, syscall.ECONNRESET) || errors.Is(err, syscall.EPIPE) || errors.As(err, &ne)
}

// Native pull forwards source export errors as target HTTP 500. Match the
// protocol envelope, never arbitrary status digits in a storage error.
func nativeSourceUnavailable(message string) bool {
	message = strings.ToLower(message)
	for _, status := range []string{"502", "503", "504"} {
		if strings.Contains(message, "native source export http"+status+":") {
			return true
		}
	}
	return false
}
