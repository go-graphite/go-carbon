package httputil

import (
	"expvar"
	"net/http"
	"sync"
	"time"
)

// PublishTrackedConnections sets the name for the published versions of the currently executing requests list
func PublishTrackedConnections(name string) {
	expvar.Publish(name, expvar.Func(trackedConnections))
}

var connectionTimes = make(map[*http.Request]time.Time)
var connectionTimesLock sync.Mutex

func trackedConnections() interface{} {

	m := make(map[string][]string)

	connectionTimesLock.Lock()
	defer connectionTimesLock.Unlock()

	for k, v := range connectionTimes {
		u := k.URL.String()
		m[u] = append(m[u], time.Since(v).String())
	}

	return m
}

// TrackConnections exports via expvar a list of all currently executing requests
func TrackConnections(fn http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		connectionTimesLock.Lock()
		connectionTimes[req] = time.Now()
		connectionTimesLock.Unlock()

		fn(w, req)

		connectionTimesLock.Lock()
		delete(connectionTimes, req)
		connectionTimesLock.Unlock()
	}
}
