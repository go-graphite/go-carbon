package carbonserver

import (
	"encoding/json"
	"net/http"
	"strconv"
)

func (listener *CarbonserverListener) sidecarsHandler(w http.ResponseWriter, r *http.Request) {
	accepted, locked := listener.beginSharedStoreRequest()
	if !accepted {
		http.Error(w, "Service unavailable", http.StatusServiceUnavailable)
		return
	}
	defer listener.endSharedStoreRequest(locked)
	db := listener.getMetricStore()
	if db == nil {
		http.Error(w, "shared store is not open", http.StatusServiceUnavailable)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	if r.Method == http.MethodGet {
		json.NewEncoder(w).Encode(db.Sidecars())
		return
	}
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", "GET, POST")
		http.Error(w, "method must be GET or POST", http.StatusMethodNotAllowed)
		return
	}
	q := r.URL.Query()
	limit := 1000
	var err error
	if q.Has("limit") {
		limit, err = strconv.Atoi(q.Get("limit"))
		if err != nil {
			http.Error(w, "invalid limit", http.StatusBadRequest)
			return
		}
	}
	dry := false
	if q.Has("dry-run") {
		dry, err = strconv.ParseBool(q.Get("dry-run"))
		if err != nil {
			http.Error(w, "invalid dry-run", http.StatusBadRequest)
			return
		}
	}
	var result any
	switch q.Get("action") {
	case "scan":
		result, err = db.ScanSidecars(r.Context(), q.Get("after"), limit, !dry)
	case "fold":
		rate := float64(0)
		if q.Has("rate") {
			rate, err = strconv.ParseFloat(q.Get("rate"), 64)
			if err != nil {
				http.Error(w, "invalid rate", http.StatusBadRequest)
				return
			}
		}
		result, err = db.FoldSidecars(r.Context(), q.Get("after"), limit, dry, rate)
	default:
		http.Error(w, "action must be scan or fold", http.StatusBadRequest)
		return
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusConflict)
		return
	}
	json.NewEncoder(w).Encode(result)
}
