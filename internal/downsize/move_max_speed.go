package downsize

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math"
	"os"
	"strings"
	"syscall"
	"time"
)

// Runtime timers are separate from the immutable pre-copy normal envelopes.
// Admission snapshots retain this optional state across controller resumes.
type maxSpeedHostState struct {
	WriteQuietUntil       float64
	ProcessDown           bool
	ReadHighSince         float64
	ReadLowSince          float64
	ReadBoostSince        float64
	ReadPaused            bool
	CachePaused           bool `json:",omitempty"`
	CacheLimited          bool `json:",omitempty"`
	CacheStreams          int  `json:",omitempty"`
	CacheGuardInitialized bool `json:",omitempty"`
	LastTime              float64
}

type maxSpeedGovernor struct {
	decisionSHA    string
	destinations   map[string]bool
	now            func() float64
	readBaselineMS float64
}

type maxSpeedDecision struct {
	Decision        string                              `json:"decision"`
	Owner           string                              `json:"owner"`
	UserInstruction string                              `json:"user_instruction"`
	HardStops       map[string]string                   `json:"hard_stops_only"`
	Throttle        map[string]string                   `json:"throttle_not_stop"`
	Amendments      []struct{ At, What, Detail string } `json:"amendments"`
}

// Only explicit per-metric proof/metadata failures may become durable skips.
// Transport and controller/journal failures retain their own classifications.
type integrityError struct{ err error }

func (e *integrityError) Error() string { return e.err.Error() }
func (e *integrityError) Unwrap() error { return e.err }

func copyIntegrityError(err error) error { return &integrityError{err: err} }

func transferIntegrityError(err error) error {
	var api *apiError
	if errors.As(err, &api) && (api.Status == 400 || api.Status == 409 || api.Status == 500 && explicitTransferIntegrity500(api.Message)) {
		return copyIntegrityError(err)
	}
	return err
}

func explicitTransferIntegrity500(message string) bool {
	body := strings.TrimSpace(message)
	const prefix = ": HTTP 500 "
	if index := strings.Index(body, prefix); index >= 0 {
		body = strings.TrimSpace(body[index+len(prefix):])
	}
	if strings.ContainsAny(body, "\r\n\x00") {
		return false
	}
	switch body {
	case "transfer checksum mismatch", "staged transfer key verification failed", "staged transfer value verification failed", "unexpected staged transfer key":
		return true
	}
	for _, shape := range [][2]string{{"source point verification ", " failed"}, {"staged point verification ", " failed"}, {"verify ", " receipt differs"}} {
		if strings.HasPrefix(body, shape[0]) && strings.HasSuffix(body, shape[1]) && len(body) > len(shape[0])+len(shape[1]) {
			return true
		}
	}
	return false
}

func maximumSpeedBackpressureHost(endpoint, path string, body any, api *apiError) string {
	if path != "/v1/transfer/pull" || api == nil || api.Status != 429 || !strings.HasSuffix(strings.TrimSpace(api.Message), ": HTTP 429 source export: 429 Too Many Requests") {
		return endpoint
	}
	request, ok := body.(struct {
		Source   string
		Names    []string
		MaxBytes int64
	})
	if !ok || request.Source == "" {
		return endpoint
	}
	return request.Source
}

func verifyMaxSpeedDecision(path, expectedSHA string) error {
	var decision maxSpeedDecision
	decoded, err := hex.DecodeString(expectedSHA)
	if err != nil || len(decoded) != sha256.Size {
		return errors.New("maximum-speed decision requires its explicit SHA256")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	sum := sha256.Sum256(raw)
	if hex.EncodeToString(sum[:]) != expectedSHA {
		return errors.New("maximum-speed raw decision SHA256 mismatch")
	}
	if json.Unmarshal(raw, &decision) != nil || decision.Decision != "resize-MAX-SPEED" || decision.Owner != "operator" || decision.UserInstruction == "" {
		return errors.New("missing explicit maximum-speed owner decision")
	}
	for key, text := range map[string]string{"write_errors": "auto-resume 2 min", "process": "process is up 2 min", "disk": "free < 15%", "read_p99": "1-min read p99 > max(25 ms, 2.5x the cluster's pre-resize 30-min p99) for 3 consecutive minutes", "integrity": "skip and record the name"} {
		if !strings.Contains(decision.HardStops[key], text) {
			return errors.New("maximum-speed owner hard-stop decision differs")
		}
	}
	for key, text := range map[string]string{"read_p99": "below max(8 ms, 1.0x baseline) for 2 min", "cpu": "> 85%", "unlogged": "REMOVED 15:45"} {
		if !strings.Contains(decision.Throttle[key], text) {
			return errors.New("maximum-speed owner throttle decision differs")
		}
	}
	if !strings.Contains(decision.Throttle["read_p99"], "> max(15 ms, 1.5x the cluster's pre-resize p99)") ||
		!strings.Contains(decision.HardStops["read_p99"], "auto-resume below max(15 ms, 1.5x baseline) for 2 min") ||
		!strings.Contains(decision.Throttle["unlogged"], "do not throttle on it") {
		return errors.New("maximum-speed amended read/unlogged decision differs")
	}
	for _, text := range []string{"cache_points > 12B -> halve", "> 16B or input_log_oldest_unpersisted_age_seconds > 3000 -> pause", "resume full speed below 10B and 2400 s"} {
		if !strings.Contains(decision.Throttle["cache_headroom"], text) {
			return errors.New("maximum-speed cache headroom decision differs")
		}
	}
	debt := decision.Throttle["compaction_debt"]
	if !strings.HasPrefix(debt, "REMOVED 14:50:") || !strings.Contains(debt, "compaction debt alone is not a throttle signal") || !strings.Contains(debt, "L0 sublevels >= half of the store's stop-writes or an active write stall") {
		return errors.New("maximum-speed debt removal requires the explicit amended owner decision")
	}
	amended, relativeRead, cacheHeadroom := false, false, false
	for _, amendment := range decision.Amendments {
		if amendment.At != "" && amendment.What == "survivor transfer limits + debt throttle removed" && strings.Contains(amendment.Detail, "drop the 'debt >2x normal -> halve' rule") && strings.Contains(amendment.Detail, "MaxTransfers 16") && strings.Contains(amendment.Detail, "HTTP 429 from a server = back off that server briefly, never stop") {
			amended = true
		}
		if strings.Contains(amendment.At, "T15:20:") && amendment.What == "read p99 thresholds relative to each cluster's pre-resize p99" && strings.Contains(amendment.Detail, "high-latency pool ~22 ms") && strings.Contains(amendment.Detail, "other pools 1-4 ms") {
			relativeRead = true
		}
		if strings.Contains(amendment.At, "T15:45:") && amendment.What == "unlogged throttle replaced by cache-headroom guard" && strings.Contains(amendment.Detail, "max copy speed is priority") && strings.Contains(amendment.Detail, "Guard on cache headroom and oldest unpersisted age, not on unlogged") {
			cacheHeadroom = true
		}
	}
	if !amended {
		return errors.New("maximum-speed debt removal lacks its exact amendment provenance")
	}
	if !relativeRead || !cacheHeadroom {
		return errors.New("maximum-speed requires the explicit 15:20 read and 15:45 cache-headroom amendments")
	}
	return nil
}

func (t *tuner) enableMaxSpeed(destinations []string, decisionSHA string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.maxSpeed = &maxSpeedGovernor{decisionSHA: decisionSHA, destinations: map[string]bool{}, readBaselineMS: 4, now: func() float64 { return float64(time.Now().UnixNano()) / 1e9 }}
	for _, host := range destinations {
		t.maxSpeed.destinations[host] = true
	}
	t.collecting, t.collectingDebt = false, false
	for _, h := range t.hosts {
		if h.maxSpeed == nil {
			h.maxSpeed = &maxSpeedHostState{}
		}
		h.hard, h.healthy = "", 0
		h.Streams = max(1, t.maximum.Streams)
		if h.maxSpeed.CacheLimited {
			h.Streams = min(h.Streams, max(1, h.maxSpeed.CacheStreams))
		}
		if h.maxSpeed.ProcessDown || h.maxSpeed.ReadPaused || h.maxSpeed.CachePaused || h.maxSpeed.WriteQuietUntil > 0 {
			h.Streams = 0
		}
		h.Batch, h.BatchBytes = max(1, t.maximum.Batch), max(1, t.maximum.BatchBytes)
	}
	close(t.changed)
	t.changed = make(chan struct{})
}

func (t *tuner) maxSpeedUnavailable(host string, cause error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.maxSpeed == nil {
		return
	}
	h := t.hosts[host]
	if h == nil {
		return
	}
	if h.maxSpeed == nil {
		h.maxSpeed = &maxSpeedHostState{}
	}
	reason := "helper/process unavailable"
	if cause != nil {
		reason = cause.Error()
	}
	confirmedDown := errors.Is(cause, syscall.ECONNREFUSED) && !errors.Is(cause, errGuards)
	for _, text := range []string{"worker process is not live", "cannot identify one live API/metrics worker", "worker has no live memory accounting"} {
		confirmedDown = confirmedDown || strings.Contains(reason, text)
	}
	decision, binding := "soft", "missing/stale telemetry"
	switch {
	case confirmedDown:
		h.maxSpeed.ProcessDown = true
		h.Streams = 0
		decision, binding = "paused", "confirmed pair process unavailable"
	case h.Streams > 0:
		h.Streams = max(1, h.Streams/2)
	default:
		decision, binding = "paused", "existing pair safety pause"
	}
	h.hard, h.healthy = "", 0
	h.maxSpeed.ReadHighSince, h.maxSpeed.ReadLowSince, h.maxSpeed.ReadBoostSince = 0, 0, 0
	event("tune", map[string]any{"host": host, "decision": decision, "binding_resource": binding, "reason": reason, "max_speed": true, "limits": h.capacity})
	close(t.changed)
	t.changed = make(chan struct{})
}

func finiteMaxSpeed(value float64) bool { return !math.IsNaN(value) && !math.IsInf(value, 0) }

// Called with t.mu held. A destination or source pause parks only admission
// involving that host and never exits the block's independent worker pools.
func (t *tuner) observeMaxSpeedLocked(host string, sample hostSample, store storageSample) error {
	h := t.hosts[host]
	if h.maxSpeed == nil {
		h.maxSpeed = &maxSpeedHostState{}
	}
	state := h.maxSpeed
	destination := t.maxSpeed.destinations[host]
	now := sample.Time
	validTime := finiteMaxSpeed(now) && now > 0 && (state.LastTime == 0 || now > state.LastTime) && math.Abs(t.maxSpeed.now()-now) < 30
	throttles, pauses := []string{}, []string{}
	if !validTime {
		throttles = append(throttles, "stale telemetry")
		now = state.LastTime
	}
	if validTime && state.LastTime > 0 && now-state.LastTime > 30 {
		state.ReadHighSince, state.ReadLowSince, state.ReadBoostSince = 0, 0, 0
		if state.WriteQuietUntil > state.LastTime {
			state.WriteQuietUntil = now + 120
		}
	}
	processUp := !sample.Offline && sample.Generation != "" && finiteMaxSpeed(sample.ProcessUptimeSeconds) && sample.ProcessUptimeSeconds >= 120
	if h.previous != nil && sample.Generation != h.previous.Generation {
		event("max_speed_process_generation", map[string]any{"host": host, "previous_generation": h.previous.Generation, "generation": sample.Generation, "uptime_seconds": sample.ProcessUptimeSeconds})
	}
	if !processUp {
		state.ProcessDown = true
	} else if validTime {
		state.ProcessDown = false
	}
	if h.previous != nil && sample.Generation == h.previous.Generation && validTime {
		if sample.WriteErrors > h.previous.WriteErrors {
			state.WriteQuietUntil = now + 120
			event("max_speed_write_errors", map[string]any{"host": host, "delta": sample.WriteErrors - h.previous.WriteErrors, "quiet_until": state.WriteQuietUntil})
		}
		if destination && sample.Unlogged > h.previous.Unlogged {
			event("max_speed_unlogged_growth", map[string]any{"host": host, "delta": sample.Unlogged - h.previous.Unlogged})
		}
	}
	if state.ProcessDown {
		pauses = append(pauses, "pair process uptime")
	}
	if now < state.WriteQuietUntil {
		pauses = append(pauses, "write-error quiet period")
	}
	readKnown := validTime && destination && sample.ReadRolling1mSamples > 0 && finiteMaxSpeed(sample.Read1mWindowSeconds) && sample.Read1mWindowSeconds > 0 && sample.Read1mWindowSeconds <= 60 && finiteMaxSpeed(sample.ReadP99Rolling1mMS) && sample.ReadP99Rolling1mMS >= 0
	readBoost := false
	readSoft, readHard, readBoostThreshold := t.maxSpeed.readThresholds()
	if readKnown {
		p99 := sample.ReadP99Rolling1mMS
		if p99 > readHard {
			if state.ReadHighSince == 0 {
				state.ReadHighSince = now
			}
			if now-state.ReadHighSince >= 180 {
				state.ReadPaused = true
			}
		} else {
			state.ReadHighSince = 0
		}
		if state.ReadPaused {
			if p99 < readSoft {
				if state.ReadLowSince == 0 {
					state.ReadLowSince = now
				}
				if now-state.ReadLowSince >= 120 {
					state.ReadPaused = false
					state.ReadLowSince = 0
				}
			} else {
				state.ReadLowSince = 0
			}
		}
		if p99 > readSoft {
			throttles = append(throttles, "destination read p99")
		}
		if p99 < readBoostThreshold {
			if state.ReadBoostSince == 0 {
				state.ReadBoostSince = now
			}
			readBoost = now-state.ReadBoostSince >= 120
		} else {
			state.ReadBoostSince = 0
		}
	} else {
		state.ReadHighSince, state.ReadLowSince, state.ReadBoostSince = 0, 0, 0
	}
	if state.ReadPaused {
		pauses = append(pauses, "destination sustained read p99")
	}
	cacheRecovered, cacheRecoveryWaiting := false, false
	if destination {
		if validTime {
			state.CacheGuardInitialized = true
		}
		age := sample.OldestUnpersistedAgeSeconds
		ageKnown := validTime && age != nil && finiteMaxSpeed(*age) && *age >= 0
		if !ageKnown {
			throttles = append(throttles, "missing oldest unpersisted age")
		}
		if validTime && (sample.CachePoints > 16_000_000_000 || ageKnown && *age > 3000) {
			state.CachePaused = true
		}
		if state.CachePaused || validTime && sample.CachePoints > 12_000_000_000 {
			state.CacheLimited = true
		}
		if state.CacheLimited && validTime && sample.CachePoints >= 0 && sample.CachePoints < 10_000_000_000 && ageKnown && *age < 2400 {
			state.CachePaused, state.CacheLimited, cacheRecovered = false, false, true
		}
		cacheRecoveryWaiting = state.CacheLimited
		if state.CachePaused {
			pauses = append(pauses, "destination cache headroom")
		}
		if validTime && sample.CachePoints > 12_000_000_000 {
			throttles = append(throttles, "destination cache above12B")
		}
	}
	if destination && sample.DiskBytes > 0 && float64(sample.DiskFreeBytes)/float64(sample.DiskBytes) < .15 {
		pauses = append(pauses, "destination disk free below15%")
	}
	raise := !destination
	if sample.PhysicalCores <= 0 || sample.NICCapacityBytes <= 0 || sample.MemoryBytes == 0 || sample.DiskBytes == 0 {
		throttles = append(throttles, "missing resource telemetry")
	}
	if destination && sample.PhysicalCores > 0 && finiteMaxSpeed(sample.CPUCores) {
		busy := sample.CPUCores / float64(sample.PhysicalCores)
		if busy > .85 {
			throttles = append(throttles, "destination CPU85%")
		}
		raise = busy < .70
	}
	if sample.NICCapacityBytes > 0 && (sample.NICIngressBytes >= sample.NICCapacityBytes*.70 || sample.NICEgressBytes >= sample.NICCapacityBytes*.70 || sample.NICIngressBytes == 0 && sample.NICEgressBytes == 0 && sample.NICBytesPerSecond >= sample.NICCapacityBytes*.70) {
		throttles = append(throttles, "NIC70%")
	}
	if sample.DiskWriteCapacity > 0 && sample.DiskWriteBytes >= sample.DiskWriteCapacity*.98 {
		throttles = append(throttles, "disk write saturation")
	}
	if sample.MemoryBytes > 0 && (sample.AvailableBytes < sample.MemoryBytes/20 || sample.RSS > sample.MemoryBytes*9/10) || sample.Threads >= 8000 {
		throttles = append(throttles, "memory/thread pressure")
	}
	if store.WriteStallActive || store.L0StopWrites > 0 && store.L0Sublevels >= store.L0StopWrites/2+store.L0StopWrites%2 {
		throttles = append(throttles, "storage pressure")
	}
	ceiling := t.maximum
	ceiling.Streams = max(1, ceiling.Streams)
	if store.MaxTransfers > 0 {
		ceiling.Streams = min(ceiling.Streams, store.MaxTransfers)
	}
	if store.MaxBodyBytes > 0 {
		ceiling.BatchBytes = min(ceiling.BatchBytes, store.MaxBodyBytes)
	}
	h.Batch, h.BatchBytes = max(1, ceiling.Batch), max(1, ceiling.BatchBytes)
	h.hard, h.healthy = "", 0
	decision, reason := "healthy", "measured headroom"
	if len(pauses) > 0 {
		h.Streams = 0
		decision, reason = "paused", strings.Join(pauses, ", ")
	} else {
		h.Streams = min(ceiling.Streams, max(1, h.Streams))
		if cacheRecovered {
			h.Streams = ceiling.Streams
		}
		switch {
		case len(throttles) > 0:
			h.Streams = max(1, h.Streams/2)
			decision, reason = "soft", strings.Join(throttles, ", ")
		case readBoost && !cacheRecoveryWaiting:
			h.Streams = min(ceiling.Streams, h.Streams*2)
			state.ReadBoostSince = now
		case raise && !cacheRecoveryWaiting:
			h.Streams = min(ceiling.Streams, h.Streams+max(1, h.Streams/2))
		}
	}
	if decision == "healthy" && h.Streams >= ceiling.Streams {
		if store.MaxTransfers > 0 && store.MaxTransfers < t.maximum.Streams {
			reason = "server transfer slots"
		} else {
			reason = "configured transfer concurrency"
		}
	}
	if validTime {
		state.LastTime = now
		h.previous, h.storePrevious = &sample, &store
	}
	state.CacheStreams = 0
	if state.CacheLimited {
		state.CacheStreams = h.Streams
	}
	event("tune", map[string]any{"host": host, "decision": decision, "reason": reason, "binding_resource": reason, "max_speed": true, "decision_sha256": t.maxSpeed.decisionSHA, "read_baseline_ms": t.maxSpeed.readBaselineMS, "read_baseline_source": "owner pre-resize 30-minute cluster evidence", "read_soft_ms": readSoft, "read_hard_ms": readHard, "read_boost_ms": readBoostThreshold, "limits": h.capacity, "ceiling": ceiling, "max_speed_state": state, "active_streams": h.active, "sample": sample, "storage": store})
	close(t.changed)
	t.changed = make(chan struct{})
	return nil
}

func (g *maxSpeedGovernor) readThresholds() (soft, hard, boost float64) {
	return math.Max(15, 1.5*g.readBaselineMS), math.Max(25, 2.5*g.readBaselineMS), math.Max(8, g.readBaselineMS)
}
