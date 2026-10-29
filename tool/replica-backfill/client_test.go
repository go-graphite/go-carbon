package main

import (
	"net/http"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"golang.org/x/time/rate"
)

type rateTestTransport struct {
	times chan time.Time
}

func (tr rateTestTransport) RoundTrip(*http.Request) (*http.Response, error) {
	tr.times <- time.Now()
	return &http.Response{StatusCode: http.StatusOK, Body: http.NoBody}, nil
}

func TestReplicaRequestRateLimit(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		limiter := rate.NewLimiter(10, 1)
		times := make(chan time.Time, 3)
		clients := []*client{
			newClient("http://replica1", &options{}, limiter),
			newClient("http://replica2", &options{}, limiter),
		}
		for _, c := range clients {
			c.http.Transport = rateTestTransport{times: times}
		}
		start := time.Now()
		var wg sync.WaitGroup
		for i := range 3 {
			wg.Go(func() {
				if _, err := clients[i%len(clients)].do(t.Context(), http.MethodGet, "/render/", nil); err != nil {
					t.Error(err)
				}
			})
		}
		wg.Wait()
		for i := range 3 {
			if got, want := (<-times).Sub(start), time.Duration(i)*100*time.Millisecond; got != want {
				t.Fatalf("request %d started at %s, want %s", i+1, got, want)
			}
		}
	})
}
