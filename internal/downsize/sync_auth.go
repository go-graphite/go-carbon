package downsize

import (
	"net/http"
	"net/url"
)

// syncJWTTransport signs at request time, including after long idle phases.
// It replaces legacy environment credentials without modifying process globals.
type syncJWTTransport struct {
	base    http.RoundTripper
	cache   *hostTokenCache
	origins map[string]bool
}

func (t *syncJWTTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if !t.origins[req.URL.Scheme+"://"+req.URL.Host] {
		clone := req.Clone(req.Context())
		clone.Header.Del("X-Buckyd-Authorization")
		return t.base.RoundTrip(clone)
	}
	token, err := t.cache.get("")
	if err != nil {
		return nil, err
	}
	clone := req.Clone(req.Context())
	clone.Header.Set("X-Buckyd-Authorization", token)
	return t.base.RoundTrip(clone)
}

func withSyncJWT(client *http.Client, cache *hostTokenCache, origins ...string) {
	if cache == nil {
		return
	}
	base := client.Transport
	if base == nil {
		base = http.DefaultTransport
	}
	allowed := map[string]bool{}
	for _, origin := range origins {
		if u, err := url.Parse(origin); err == nil {
			allowed[u.Scheme+"://"+u.Host] = true
		}
	}
	client.Transport = &syncJWTTransport{base: base, cache: cache, origins: allowed}
}

func (t *syncJWTTransport) CloseIdleConnections() {
	if closer, ok := t.base.(interface{ CloseIdleConnections() }); ok {
		closer.CloseIdleConnections()
	}
}
