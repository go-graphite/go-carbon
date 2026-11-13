package carbonserver

import "net/http"

func (listener *CarbonserverListener) metricStoreReadError() error {
	if db := listener.getMetricStore(); db != nil {
		return db.ReadError()
	}
	return nil
}

func (listener *CarbonserverListener) rejectClosedStore(w http.ResponseWriter) bool {
	if err := listener.metricStoreReadError(); err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return true
	}
	return false
}
