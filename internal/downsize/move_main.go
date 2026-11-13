package downsize

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/go-graphite/go-carbon/internal/chunkstore"
)

type options struct {
	from, until                                                                      string
	nativeWindow                                                                     *chunkstore.NativeWindow
	engine                                                                           string
	bulkOnly                                                                         bool
	bulkCombine                                                                      bool
	deferredOnly                                                                     bool
	deferredRange                                                                    string
	maxBatches                                                                       int64
	localSources, localTarget                                                        string
	plan, state, barrier, healthMap, tokenEnv, controlTokenEnv, replica, replicaSite string
	deleteMode                                                                       string
	apply, delete, auto, localLab, blockReads, reconcile                             bool
	streams, batch, deleteRate                                                       int
	maxBytes                                                                         int64
	tick                                                                             time.Duration
	observeOnly                                                                      time.Duration
	mbps                                                                             float64
	read                                                                             readPolicy
	backlog                                                                          backlogPolicy
	// explicit names the flags given on the command line; resumes override
	// saved journal thresholds only with these. Nil counts every option as given.
	explicit map[string]bool
}
type apiError struct {
	Status  int
	Message string
}

func (e *apiError) Error() string { return e.Message }

type mover struct {
	hostToken        hostTokenCache
	absentProofSlots sync.Map
	batchesSent      atomic.Int64
	localSources     map[string]*chunkstore.TransferSource
	localTargets     map[string]string
	binaryProofs     sync.Map
	options
	planData *manifest
	client   *http.Client
	tuner    *tuner
	helpers  map[string]string
	journal  *journal
	stateMu  sync.Mutex
}

var errCopyFence = errors.New("baseline staged; resume the same --reconcile run with a fresh writer-drain barrier")

var outputMu sync.Mutex

func event(kind string, fields map[string]any) {
	outputMu.Lock()
	defer outputMu.Unlock()
	fields["event"] = kind
	fields["time"] = time.Now().UTC().Format(time.RFC3339Nano)
	_ = json.NewEncoder(os.Stdout).Encode(fields)
}
func MoveMain(args []string) error {
	flags := flag.NewFlagSet("resize range", flag.ContinueOnError)
	o := options{backlog: defaultBacklogPolicy(), read: defaultReadPolicy()}
	flags.StringVar(&o.from, "from", "", "native copy window start, Unix seconds or RFC3339 (inclusive)")
	flags.StringVar(&o.until, "until", "", "native copy window end, Unix seconds or RFC3339 (exclusive)")
	flags.StringVar(&o.plan, "plan", "", "committed ds-ring plan directory")
	flags.StringVar(&o.engine, "engine", "metric", "metric or bulk (native absent-name SST engine; existing plan/journal)")
	flags.BoolVar(&o.bulkOnly, "bulk-only", false, "finish native absence pass and leave deferred combines in the same journal")
	flags.BoolVar(&o.bulkCombine, "combine-existing", false, "combine present names natively with optimistic publication (requires --engine bulk)")
	flags.BoolVar(&o.deferredOnly, "deferred-only", false, "combine only acknowledged native deferrals; never issue plan gaps")
	flags.StringVar(&o.deferredRange, "deferred-range", "", "restrict deferred work to one existing STREAM:START:END journal range")
	flags.StringVar(&o.localSources, "local-sources", "", "experimental checkpoint-local comparison: JSON source-origin to verified-copy directory map; retains existing import limits")
	flags.StringVar(&o.localTarget, "local-target", "", "loopback Buckyd origin for local checkpoint imports; checked against the plan's target identity")
	flags.Int64Var(&o.maxBatches, "max-batches", 0, "stop after producing this many journal batches (0 runs the full plan)")
	flags.StringVar(&o.state, "state", "move-state.jsonl", "durable checkpoint journal")
	flags.StringVar(&o.barrier, "barrier", "", "fresh cutover/drain/reconciliation proof; required for --delete")
	flags.StringVar(&o.healthMap, "health-map", "", "JSON map of buckyd origins to host.py origins")
	flags.StringVar(&o.tokenEnv, "token-env", "BUCKYD_TOKEN", "environment variable holding root buckyd JWT")
	flags.StringVar(&o.controlTokenEnv, "control-token-env", "MOVE_CONTROL_TOKEN", "environment variable holding host helper secret")
	flags.BoolVar(&o.apply, "apply", false, "execute transfers (default prints all planned batches)")
	flags.StringVar(&o.deleteMode, "delete-mode", "auto", "retire stages versions then deletes the whole retired node; batch deletes immediately")
	flags.BoolVar(&o.reconcile, "reconcile", false, "stage versions, gate on writer drain, reconcile and verify complete coverage while retaining sources")
	flags.BoolVar(&o.delete, "delete", false, "delete retired source copies after verification; requires barrier")
	flags.BoolVar(&o.auto, "auto", true, "adapt parallelism, batch size and deletion pacing to host headroom")
	flags.BoolVar(&o.localLab, "local-lab", false, "loopback-only synthetic host telemetry for local tests")
	flags.BoolVar(&o.blockReads, "block-reads", false, "renew expiring read-isolation leases on helper hosts")
	flags.StringVar(&o.replica, "replica-url", "", "carbonserver or carbonapi origin serving the same cluster in another site")
	flags.StringVar(&o.replicaSite, "replica-site", "", "site of read replica; must differ from plan site")
	flags.IntVar(&o.streams, "streams", 0, "maximum active transfers per host; zero uses available host CPUs (up to 64)")
	flags.IntVar(&o.batch, "batch", 8192, "maximum metrics per transfer")
	flags.IntVar(&o.deleteRate, "delete-rate", 65536, "maximum deleted metrics/s per source")
	flags.Int64Var(&o.maxBytes, "batch-bytes", 64<<20, "maximum estimated bytes per transfer; server max_body_bytes still applies")
	flags.DurationVar(&o.tick, "tick", 10*time.Second, "host guard and tuner tick")
	flags.DurationVar(&o.observeOnly, "observe-only", 0, "run admission guards for this duration without starting transfer workers; also learn missing debt baselines with copying off")
	flags.Float64Var(&o.mbps, "max-mbps", 0, "optional transfer MB/s ceiling per host direction; zero uses physical host guards without a fixed rate cap")
	flags.Float64Var(&o.read.CeilingMS, "read-p99-ms", o.read.CeilingMS, "absolute read p99 ceiling (ms) while reads are enabled; 0 leaves only the relative limit; saved with run, a resume flag overrides")
	flags.Float64Var(&o.read.FloorMS, "read-p99-floor-ms", o.read.FloorMS, "read p99 pause never below this (ms): limit = min(read-p99-ms, max(floor, ratio x pre-copy median p99)); saved with run, a resume flag overrides")
	flags.Float64Var(&o.read.Ratio, "read-p99-ratio", o.read.Ratio, "read p99 pause above this multiple of the pre-copy median read p99 (>= 1); saved with run, a resume flag overrides")
	flags.DurationVar(&o.backlog.Window, "backlog-baseline-window", o.backlog.Window, "pre-copy backlog observation window (minimum 3m); persisted with run and fixed on resume")
	flags.Float64Var(&o.backlog.SoftRatio, "backlog-soft-ratio", o.backlog.SoftRatio, "pause admission above this multiple of pre-copy maximum backlog; saved with run, a resume flag overrides")
	flags.Float64Var(&o.backlog.HardRatio, "backlog-hard-ratio", o.backlog.HardRatio, "abort above this multiple of pre-copy maximum backlog; saved with run, a resume flag overrides")
	flags.IntVar(&o.backlog.GrowthSamples, "backlog-growth-samples", o.backlog.GrowthSamples, "pause on consecutive rising samples beyond baseline maximum plus observed swing; saved with run, a resume flag overrides")
	flags.Float64Var(&o.backlog.BytesPerPoint, "cache-bytes-per-point", o.backlog.BytesPerPoint, "estimated cache bytes/point for 90% host-memory safety ceiling (minimum 16); saved with run, a resume flag overrides")
	if err := flags.Parse(args); err != nil {
		return err
	}
	o.explicit = map[string]bool{}
	flags.Visit(func(f *flag.Flag) { o.explicit[f.Name] = true })
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if err := run(ctx, o); err != nil {
		event("abort", map[string]any{"error": err.Error()})
		return err
	}
	return nil
}
func run(ctx context.Context, o options) error {
	if (o.from != "" || o.until != "") && (o.engine != "bulk" || !o.bulkOnly) {
		return errors.New("time windows require --engine bulk --bulk-only")
	}
	if (o.deferredOnly || o.deferredRange != "") && (o.engine != "bulk" || !o.bulkCombine || !o.bulkOnly) {
		return errors.New("deferred scope requires --engine bulk --combine-existing --bulk-only")
	}
	if o.engine == "bulk" {
		return runNative(ctx, o)
	}
	if o.bulkCombine {
		return errors.New("--combine-existing requires --engine bulk")
	}
	if o.engine != "" && o.engine != "metric" {
		return errors.New("unknown copy engine")
	}
	if o.maxBatches < 0 {
		return errors.New("negative batch limit")
	}
	if o.observeOnly < 0 || o.observeOnly > 0 && (!o.apply || o.delete || o.reconcile || o.blockReads) {
		return errors.New("--observe-only requires --apply and copy-only flags without --block-reads")
	}
	if o.backlog == (backlogPolicy{}) {
		o.backlog = defaultBacklogPolicy()
	}
	if err := o.backlog.validate(); err != nil {
		return err
	}
	if o.read == (readPolicy{}) {
		o.read = defaultReadPolicy()
	}
	if err := o.read.validate(); err != nil {
		return err
	}
	if o.deleteMode == "" {
		o.deleteMode = "auto"
	}
	if o.deleteMode != "auto" && o.deleteMode != "retire" && o.deleteMode != "batch" {
		return errors.New("delete-mode must be auto, retire or batch")
	}
	if o.streams == 0 {
		o.streams = 64
	}
	if o.streams < 1 || o.streams > 64 || o.batch < 1 || o.batch > 10000 || o.maxBytes < 65536 || o.maxBytes > 160<<20 || o.deleteRate < 1 || o.tick < time.Second || o.tick > 20*time.Second || o.mbps < 0 || math.IsNaN(o.mbps) || math.IsInf(o.mbps, 0) {
		return errors.New("invalid resource limit")
	}
	if o.reconcile && (o.delete || o.deleteMode == "batch") {
		return errors.New("--reconcile retains sources; do not combine it with --delete or batch deletion")
	}
	p, err := readPlan(o.plan)
	if err != nil {
		return err
	}
	if p.Spec.NativeWindow != nil {
		return errors.New("windowed plans require the native bulk engine")
	}
	if o.reconcile && (len(p.Whole) > 0 || p.Spec.AllowUnowned || p.Spec.Old.Replicas > 1 || p.Spec.New.Replicas > 1) {
		return errors.New("--reconcile requires a single-copy retired-source range plan")
	}
	hosts := map[string]bool{}
	for _, r := range p.Streams {
		hosts[r.sourceURL] = true
		hosts[r.targetURL] = true
	}
	for _, job := range p.Whole {
		hosts[job.SourceURL] = true
		hosts[job.TargetURL] = true
	}
	var origins []string
	for h := range hosts {
		origins = append(origins, h)
	}
	m := &mover{options: o, planData: p, helpers: map[string]string{}, client: &http.Client{Timeout: 10 * time.Minute, Transport: &http.Transport{MaxIdleConnsPerHost: 2 * o.streams, MaxIdleConns: 256, ResponseHeaderTimeout: 90 * time.Second}, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}
	defer m.client.CloseIdleConnections()
	if o.delete && len(p.Whole) == 0 {
		if err := m.checkBarrier(); err != nil {
			return err
		}
	}
	if o.blockReads && (o.replica == "" || o.replicaSite == "" || o.replicaSite == p.Spec.Site) {
		return errors.New("read isolation requires a replica in a different site")
	}
	if o.reconcile {
		planned := make(map[string]bool)
		for _, r := range p.Streams {
			planned[r.Source] = true
			if !m.stagedDelete(r.sourceURL) {
				return errors.New("--reconcile requires physically retired sources")
			}
			for _, member := range p.Spec.New.Nodes {
				if member.ID == r.Source {
					return errors.New("--reconcile cannot use a retained physical source")
				}
			}
		}
		for _, old := range p.Spec.Old.Nodes {
			retained := false
			for _, member := range p.Spec.New.Nodes {
				retained = retained || member.ID == old.ID
			}
			if !retained && !planned[old.ID] {
				return errors.New("--reconcile requires planned routes for every retired source; verify empty sources separately")
			}
		}
	}
	event("plan", map[string]any{"plan_id": p.PlanID, "streams": len(p.Streams), "apply": o.apply, "delete": o.delete, "limits": capacity{o.streams, o.batch, o.maxBytes, o.deleteRate}, "block_reads": o.blockReads, "reconcile": o.reconcile})
	if !o.apply {
		for _, job := range p.Whole {
			event("would_replace", map[string]any{"source": job.SourceURL, "target": job.TargetURL, "worker": job.Placement.Worker, "directory": job.Placement.Directory, "then": "checkpoint; baseline; fenced input-log seal; verified prepare; replay/readiness; dependent metric ranges"})
		}
		for _, r := range p.Streams {
			event("would_transfer", map[string]any{"stream": r.Path, "sha256": r.SHA256, "source": r.sourceURL, "target": r.targetURL, "metrics": r.Metrics, "estimated_bytes": r.EstimatedBytes, "then": m.previewActions(r.sourceURL)})
		}
		return errors.New("dry run has no pre-copy backlog baseline; admission is unverified; --apply observes and persists a baseline before copying")
	}
	if o.localLab {
		for h := range hosts {
			if !loopbackOrigin(h) {
				return errors.New("--local-lab requires loopback endpoints")
			}
		}
	} else {
		if o.healthMap != "" {
			data, err := os.ReadFile(o.healthMap)
			if err != nil {
				return err
			}
			if err := json.Unmarshal(data, &m.helpers); err != nil {
				return err
			}
		}
		for _, ring := range []topology{p.Spec.Old, p.Spec.New} {
			for _, node := range ring.Nodes {
				if placement, ok := p.Spec.Placements[node.ID]; ok {
					endpoint, _ := origin(node.Endpoint)
					if m.helpers[endpoint] == "" {
						m.helpers[endpoint] = placement.Worker
					}
				}
			}
		}
	}
	for _, job := range p.Whole {
		if o.localLab && !loopbackOrigin(job.Placement.Worker) {
			return errors.New("local-lab replacement worker must be loopback")
		}
		m.helpers[job.TargetURL] = job.Placement.Worker
	}
	if !o.localLab {
		for h := range hosts {
			if _, err := origin(m.helpers[h]); err != nil {
				return fmt.Errorf("missing host helper for %s", h)
			}
		}
	}

	if o.blockReads && o.localLab {
		return errors.New("use the lab read workload toggle to benchmark isolation")
	}
	m.tuner = newTuner(origins, capacity{o.streams, o.batch, o.maxBytes, o.deleteRate}, o.auto, o.mbps, o.read.CeilingMS)
	m.tuner.backlog, m.tuner.read = o.backlog, o.read
	journalID := p.PlanID + fmt.Sprintf(":delete=%t:mode=%s", o.delete, o.deleteMode)
	if o.reconcile {
		journalID += ":reconcile=true"
	}
	j, err := openJournal(o.state, journalID)
	if err != nil {
		return err
	}
	defer j.close()
	m.journal = j
	if err := m.openLocalSources(ctx); err != nil {
		return err
	}
	defer m.closeLocalSources()
	event("operation", map[string]any{"operation": j.operation, "state": o.state})
	maximumSpeed, err := m.configureMaximumSpeed(ctx, origins)
	if err != nil {
		return err
	}
	if !maximumSpeed {
		if err := m.prepareAdmission(ctx, origins); err != nil {
			return err
		}
	}
	if o.observeOnly > 0 {
		event("observation_started", map[string]any{"duration": o.observeOnly.String(), "transfers": false})
		deadline := time.Now().Add(o.observeOnly)
		for time.Now().Before(deadline) {
			if err := sleepContext(ctx, min(o.tick, time.Until(deadline))); err != nil {
				return err
			}
			if err := m.poll(ctx, origins); err != nil {
				return err
			}
		}
		event("observation_complete", map[string]any{"transfers": false})
		return nil
	}
	if o.reconcile {
		m.tuner.mu.Lock()
		for _, r := range p.Streams {
			store := m.tuner.hosts[r.sourceURL].storePrevious
			if store == nil || !store.PendingDeleteBatches || !store.StagedCopyVerification {
				m.tuner.mu.Unlock()
				return errors.New("--reconcile requires updated source batch/coverage capabilities")
			}
		}
		m.tuner.mu.Unlock()
	}
	if o.blockReads {
		if err := m.readLeases(ctx, origins, false); err != nil {
			cleanup, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			_ = m.readLeases(cleanup, origins, true)
			return err
		}
		defer func() {
			cleanup, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			_ = m.readLeases(cleanup, origins, true)
		}()
		// Measure the foreground CPU left after read isolation, before transfer
		// work starts. The pre-isolation read load must not cap the new ramp.
		if err := sleepContext(ctx, o.tick); err != nil {
			return err
		}
		if err := m.poll(ctx, origins); err != nil {
			return err
		}
		m.tuner.captureReadBaselines()
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	errorsOut := make(chan error, 1)
	report := func(err error) {
		if err != nil {
			select {
			case errorsOut <- err:
			default:
			}
			cancel()
		}
	}
	var monitoring sync.WaitGroup
	monitoring.Add(1)
	defer func() { cancel(); monitoring.Wait() }()
	go func() {
		defer monitoring.Done()
		ticker := time.NewTicker(o.tick)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := m.poll(ctx, origins); err != nil {
					report(err)
					return
				}
				if o.blockReads {
					if err := m.readLeases(ctx, origins, false); err != nil {
						report(err)
						return
					}
				}
			}
		}
	}()
	if err := m.replacements(ctx); err != nil {
		return err
	}
	if len(p.Whole) > 0 {
		if o.delete {
			if err := m.checkBarrier(); err != nil {
				return err
			}
		}
		if err := m.poll(ctx, origins); err != nil {
			return err
		}
		m.tuner.captureReadBaselines()
	}
	produceErr := m.runCopyWorkers(ctx, report)
	select {
	case err := <-errorsOut:
		return err
	default:
	}
	if produceErr != nil {
		return produceErr
	}
	if o.reconcile {
		if err := m.checkBarrier(); err != nil {
			event("needs_writer_fence", map[string]any{"state": o.state, "action": "switch writers, drain old input, reconcile catalogs and resume with --barrier"})
			return fmt.Errorf("%w: %w", errCopyFence, err)
		}
	}
	if o.reconcile || o.delete && o.deleteMode != "batch" {
		if err := m.retireSources(ctx); err != nil {
			return err
		}
	}
	if o.maxBatches > 0 {
		event("bounded_run_complete", map[string]any{"plan_id": p.PlanID, "max_batches": o.maxBatches})
	} else {
		event("complete", map[string]any{"plan_id": p.PlanID})
	}
	return nil
}
func loopbackOrigin(raw string) bool {
	h := strings.TrimPrefix(strings.TrimPrefix(raw, "http://"), "https://")
	host, _, err := net.SplitHostPort(h)
	if err != nil {
		host = h
	}
	ip := net.ParseIP(host)
	return host == "localhost" || (ip != nil && ip.IsLoopback())
}
func (m *mover) request(ctx context.Context, endpoint, path string, body any, out any) (http.Header, error) {
	if m.delete && (path == "/v1/transfer/delete" || path == "/v1/transfer/retire") {
		data, err := os.ReadFile(m.barrier)
		if err != nil {
			return nil, err
		}
		var barrier barrier
		if err := json.Unmarshal(data, &barrier); err != nil {
			return nil, err
		}
		var cancel context.CancelFunc
		ctx, cancel = context.WithDeadline(ctx, time.Unix(barrier.ExpiresAt, 0))
		defer cancel()
	}
	for attempt := 0; ; attempt++ {
		headers, err := m.requestOnce(ctx, endpoint, path, body, out)
		var api *apiError
		if !errors.As(err, &api) || api.Status != 429 {
			return headers, err
		}
		backoffHost := endpoint
		maximumSpeed := m.tuner != nil && m.tuner.maxSpeed != nil
		if maximumSpeed {
			backoffHost = maximumSpeedBackpressureHost(endpoint, path, body, api)
		}
		event("backpressure", map[string]any{"host": backoffHost, "via_endpoint": endpoint, "path": path, "attempt": attempt + 1})
		if m.tuner != nil {
			m.tuner.backoff(backoffHost)
		}
		if maximumSpeed {
			return headers, err
		} // Release pair admission before outer retry pacing.
		if err := sleepContext(ctx, m.tick); err != nil {
			return nil, err
		}
	}
}
func (m *mover) requestOnce(ctx context.Context, endpoint, path string, body any, out any) (http.Header, error) {
	if local := m.localTargets[endpoint]; local != "" && (path == "/v1/transfer/import" || path == "/v1/transfer/verify") {
		endpoint = local
	}
	var reader io.Reader
	contentType := "application/json"
	if body != nil {
		var data []byte
		var err error
		if raw, ok := body.(transferBody); ok {
			data, contentType = raw, "application/vnd.go-carbon.transfer-v2"
		} else if proofs, ok := body.([]chunkstore.TransferProof); ok && path == "/v1/transfer/verify" {
			if _, negotiated := m.binaryProofs.Load(endpoint); negotiated {
				data, err = chunkstore.EncodeTransferProofs(proofs)
				contentType = chunkstore.TransferProofContentType
			} else {
				data, err = json.Marshal(body)
			}
		} else {
			data, err = json.Marshal(body)
		}
		if err != nil {
			return nil, err
		}
		reader = bytes.NewReader(data)
	}
	method := http.MethodGet
	if body != nil {
		method = http.MethodPost
	}
	r, err := http.NewRequestWithContext(ctx, method, endpoint+path, reader)
	if err != nil {
		return nil, err
	}
	r.Header.Set("Content-Type", contentType)
	r.Header.Set("Accept", chunkstore.TransferProofContentType)
	token := os.Getenv(m.tokenEnv)
	if m.engine != "bulk" && m.tuner != nil {
		var err error
		token, err = m.authorizationToken()
		if err != nil {
			return nil, err
		}
	}
	if token != "" {
		r.Header.Set("X-Buckyd-Authorization", token)
	}
	response, err := m.client.Do(r)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	if response.StatusCode == http.StatusUnauthorized && m.engine != "bulk" {
		m.invalidateHostToken()
	}
	if response.StatusCode != 200 {
		if contentType == chunkstore.TransferProofContentType && (response.StatusCode == 400 || response.StatusCode == 415) {
			// Verification is read-only. A target downgraded after negotiation
			// can safely receive the identical proof again through JSON.
			m.binaryProofs.Delete(endpoint)
			_ = response.Body.Close()
			return m.requestOnce(ctx, endpoint, path, body, out)
		}
		message, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
		return nil, &apiError{response.StatusCode, fmt.Sprintf("%s %s: HTTP %d %s", endpoint, path, response.StatusCode, message)}
	}
	if out != nil {
		if proofs, ok := out.(*[]chunkstore.TransferProof); ok && response.Header.Get("Content-Type") == chunkstore.TransferProofContentType {
			var err error
			*proofs, err = chunkstore.ReadTransferProofs(response.Body)
			if err != nil {
				return nil, err
			}
			m.binaryProofs.Store(endpoint, true)
		} else if err := json.NewDecoder(io.LimitReader(response.Body, 32<<20)).Decode(out); err != nil {
			return nil, err
		}
	}
	return response.Header, nil
}
func (m *mover) checkBarrier() error {
	data, err := os.ReadFile(m.barrier)
	if err != nil {
		return err
	}
	var b barrier
	if err := json.Unmarshal(data, &b); err != nil {
		return err
	}
	if b.PlanID != m.planData.PlanID || !b.RelaySwitched || !b.OldWritersDrained || !b.Reconciled || b.ExpiresAt <= time.Now().Unix() || b.ExpiresAt > time.Now().Add(24*time.Hour).Unix() {
		return errors.New("fresh relay cutover, writer drain and final reconciliation barrier required")
	}
	if m.planData.Spec.AllowUnowned {
		return errors.New("deletion forbidden for allow_unowned plan")
	}
	if m.planData.Spec.Old.Replicas > 1 || m.planData.Spec.New.Replicas > 1 {
		return errors.New("delete currently requires one copy per site; copy-only supports replicated rings")
	}
	sources := map[string]bool{}
	for _, s := range b.Sources {
		sources[s] = true
	}
	retained := map[string]bool{}
	for _, node := range m.planData.Spec.New.Nodes {
		retained[node.ID] = true
	}
	for _, r := range m.planData.Streams {
		if (m.deleteMode == "retire" && retained[r.Source]) || !sources[r.Source] {
			return errors.New("barrier must name each source; explicit retire mode also requires a physically retired source")
		}
	}
	return nil
}
func (m *mover) produce(ctx context.Context, tasks chan<- task, onlySource string) error {
	return m.produceFiltered(ctx, tasks, onlySource, onlySource != "")
}
func (m *mover) produceFiltered(ctx context.Context, tasks chan<- task, onlySource string, reconcile bool) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	routes := make(chan route)
	var workers sync.WaitGroup
	var first error
	var once sync.Once
	for range min(16, len(m.planData.Streams)) {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for r := range routes {
				if err := m.produceRoute(ctx, r, tasks, reconcile); err != nil {
					if errors.Is(err, errBatchLimit) {
						continue
					}
					once.Do(func() { first = err; cancel() })
					return
				}
			}
		}()
	}
	for _, r := range m.planData.Streams {
		if onlySource != "" && r.sourceURL != onlySource {
			continue
		}
		select {
		case routes <- r:
		case <-ctx.Done():
			break
		}
	}
	close(routes)
	workers.Wait()
	if first != nil {
		return first
	}
	return ctx.Err()
}
func (m *mover) produceRoute(ctx context.Context, r route, tasks chan<- task, reconcile bool) error {
	send := func(t task) error {
		if m.maxBatches > 0 && m.batchesSent.Add(1) > m.maxBatches {
			return errBatchLimit
		}
		select {
		case tasks <- t:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	var completed []task
	if !reconcile {
		m.stateMu.Lock()
		completed = m.journal.ranges(r.SHA256)
		m.stateMu.Unlock()
	}
	for _, old := range completed {
		if old.Phase != "done" {
			if old.Source != r.sourceURL || old.Target != r.targetURL {
				return errors.New("checkpoint endpoint mismatch")
			}
			if err := send(old); err != nil {
				return err
			}
		}
	}
	skip := 0
	var pending task
	var estimated int64
	flush := func() error {
		if len(pending.Names) == 0 {
			return nil
		}
		pending.Bytes = estimated
		if err := send(pending); err != nil {
			return err
		}
		pending = task{}
		estimated = 0
		return nil
	}
	err := m.planData.walk(r, func(ordinal int64, row planRow) error {
		for skip < len(completed) && completed[skip].End <= ordinal {
			skip++
		}
		if skip < len(completed) && completed[skip].Start <= ordinal {
			if err := flush(); err != nil {
				return err
			}
			return nil
		}
		count, bytes := m.tuner.batch(r.sourceURL, r.targetURL)
		estimatedSize := max(row.EstimatedBytes, int64(512+len(row.Metric)*2))
		if len(pending.Names) > 0 && (len(pending.Names) >= count || estimated+estimatedSize > bytes) {
			if err := flush(); err != nil {
				return err
			}
		}
		if len(pending.Names) == 0 {
			pending = task{Stream: r.SHA256, Start: ordinal, Source: r.sourceURL, Target: r.targetURL}
		}
		pending.End = ordinal + 1
		pending.Names = append(pending.Names, row.Metric)
		estimated += estimatedSize
		return nil
	})
	if err != nil {
		return err
	}
	if err := flush(); err != nil {
		return err
	}
	return nil
}
func (m *mover) execute(ctx context.Context, t task) error {
	if t.Kind == "native-range" && t.Native != nil {
		return m.executeNativeFallback(ctx, t)
	}
	return m.executeBatch(ctx, t, t)
}
func (m *mover) executeBatch(ctx context.Context, t, batch task) error {
	if t.Kind == "bulk-intent" && len(t.Proofs) == 0 {
		return errors.New("uncertain bulk import: durable intent has no response; refusing to import twice (retain journal and inspect target receipts before recovery)")
	}
	names, err := m.sourceNames(t)
	if err != nil {
		return err
	}
	if len(names) == 0 && t.Phase != "verified" {
		t.Phase = "done"
		t.Names = nil
		m.stateMu.Lock()
		defer m.stateMu.Unlock()
		return m.journal.save(t)
	}
	release, err := m.tuner.admit(ctx, t.Source, t.Target)
	if err != nil {
		return err
	}
	defer func() { release() }()
	if t.Phase != "verified" {
		event("transfer", map[string]any{"stream": t.Stream, "start": t.Start, "end": t.End, "source": t.Source, "target": t.Target, "metrics": len(t.Names), "estimated_bytes": t.Bytes})
		start := time.Now()
		var proofs []chunkstore.TransferProof
		_, byteBudget := m.tuner.batch(t.Source, t.Target)
		if t.NativeParent != "" {
			intent := t
			intent.Kind = "bulk-intent"
			intent.Phase = "verified"
			intent.Proofs = nil
			m.stateMu.Lock()
			intentErr := m.journal.save(intent)
			m.stateMu.Unlock()
			if intentErr != nil {
				return intentErr
			}
		}
		headers, err := m.pull(ctx, t, names, byteBudget, &proofs)
		if err != nil {
			var api *apiError
			if errors.As(err, &api) && t.NativeParent != "" && nativeDefinitivePullRejection(api) {
				// These responses occur before import in the reviewed Buckyd
				// handler: auth, source absence, size rejection, or admission.
				// Keep the pending child retryable after a definitive rejection.
				// Transport failures and generic500 leave the uncertain intent.
				if saveErr := m.saveNativeDeclinedPull(t); saveErr != nil {
					return saveErr
				}
			}
			if errors.As(err, &api) && api.Status == 404 {
				if skipErr := m.skipAbsentSource(ctx, batch, names); skipErr != nil {
					return skipErr
				}
				release()
				release = func() {}
				return m.executeBatch(ctx, t, batch)
			}
			if errors.As(err, &api) && api.Status == 413 && len(t.Names) == 1 {
				release()
				release = func() {}
				larger, waitErr := m.tuner.waitLargerBatch(ctx, t.Source, t.Target, byteBudget)
				if waitErr != nil {
					return waitErr
				}
				if larger {
					event("retry_single_metric", map[string]any{"metric": t.Names[0], "previous_byte_budget": byteBudget})
					return m.executeBatch(ctx, t, batch)
				}
			}
			if errors.As(err, &api) && api.Status == 413 && len(t.Names) > 1 {
				release()
				release = func() {}
				middle := len(t.Names) / 2
				left, right := t, t
				left.End = t.Start + int64(middle)
				left.Names = t.Names[:middle]
				right.Start = left.End
				right.Names = t.Names[middle:]
				if len(t.NativeOrdinals) > 0 {
					left.NativeOrdinals = t.NativeOrdinals[:middle]
					right.NativeOrdinals = t.NativeOrdinals[middle:]
				}
				if err := m.executeBatch(ctx, left, batch); err != nil {
					return err
				}
				return m.executeBatch(ctx, right, batch)
			}
			return transferIntegrityError(err)
		}
		if len(proofs) != len(names) {
			return copyIntegrityError(errors.New("import proof count mismatch"))
		}
		for i, p := range proofs {
			if p.Source.Name != names[i] || p.Target.Name != names[i] || p.Digest == "" {
				return copyIntegrityError(errors.New("import proof name/digest mismatch"))
			}
		}
		t.Bytes, _ = strconv.ParseInt(headers.Get("X-Transfer-Bytes"), 10, 64)
		m.tuner.charge(t.Source, t.Target, t.Bytes, start)
		t.Proofs = proofs
		t.Phase = "verified"
		m.stateMu.Lock()
		err = m.journal.save(t)
		m.stateMu.Unlock()
		if err != nil {
			return err
		}
		if _, err := m.request(ctx, t.Target, "/v1/transfer/verify", proofs, &map[string]int{}); err != nil {
			return transferIntegrityError(err)
		}
		event("verified", map[string]any{"stream": t.Stream, "start": t.Start, "end": t.End, "bytes": t.Bytes, "seconds": time.Since(start).Seconds()})
	} else {
		// A resumed cleanup must still find its durable target generation.
		if _, err := m.request(ctx, t.Target, "/v1/transfer/verify", t.Proofs, &map[string]int{}); err != nil {
			return transferIntegrityError(err)
		}
	}
	if m.delete || m.reconcile {
		if m.delete {
			if err := m.checkBarrier(); err != nil {
				return err
			}
		}
		versions := make([]chunkstore.TransferVersion, len(t.Proofs))
		for i, p := range t.Proofs {
			versions[i] = p.Source
		}
		staged := m.reconcile || m.stagedDelete(t.Source)
		step := 128
		if staged {
			step = len(versions)
		}
		for start := 0; start < len(versions); start += step {
			end := min(start+step, len(versions))
			kind := "delete"
			if staged {
				kind = "stage_delete"
				if m.reconcile {
					kind = "stage_copy"
				}
			}
			event(kind, map[string]any{"source": t.Source, "stream": t.Stream, "start": t.Start + int64(start), "end": t.Start + int64(end), "conditional_versions": len(versions[start:end])})
			path := "/v1/transfer/delete"
			if staged {
				path = "/v1/transfer/stage-delete"
				if m.reconcile {
					path = "/v1/transfer/stage-copy"
				}
			}
			// Version markers are bounded copy bookkeeping, not deletion. A
			// name/s delete cap here silently limits sparse copy throughput.
			if !staged {
				if err := m.tuner.paceDelete(ctx, t.Source, end-start); err != nil {
					return err
				}
			}
			var body any = versions[start:end]
			if staged {
				body = struct {
					Operation string
					Versions  []chunkstore.TransferVersion
				}{m.journal.operation, versions[start:end]}
			}
			if _, err := m.request(ctx, t.Source, path, body, &map[string]int{}); err != nil {
				var api *apiError
				if staged && errors.As(err, &api) && api.Status == 409 {
					// Nothing has been deleted in retire mode: refresh the final delta
					// from the still-present source rather than replaying a stale receipt.
					if t.Retries >= 3 {
						return err
					}
					t.Retries++
					t.Phase = ""
					t.Proofs = nil
					if len(t.Names) == 0 {
						for _, v := range versions {
							t.Names = append(t.Names, v.Name)
						}
					}
					release()
					release = func() {}
					return m.execute(ctx, t)
				}
				return err
			}

		}
	}
	t.Phase = "done"
	t.Names = nil
	t.Proofs = nil
	m.stateMu.Lock()
	err = m.journal.save(t)
	m.stateMu.Unlock()
	return err
}
func (m *mover) poll(ctx context.Context, hosts []string) error {
	for _, h := range hosts {
		if err := m.pollHost(ctx, h); err != nil {
			if m.tuner.maxSpeed == nil {
				return err
			}
			if ctx.Err() != nil {
				return ctx.Err()
			}
			m.tuner.maxSpeedUnavailable(h, err)
		}
	}
	if !m.tuner.collecting && !m.tuner.collectingDebt {
		return m.saveAdmission()
	}
	return nil
}
func (m *mover) pollHost(ctx context.Context, h string) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	{
		var store storageSample
		if m.wholeOffline(h) {
			if err := m.helper(ctx, h, "/storage", nil, &store); err != nil {
				return err
			}
		} else if _, err := m.request(ctx, h, "/v1/transfer/status", nil, &store); err != nil {
			return err
		}
		if store.Protocol != 1 {
			return errors.New("unsupported bulk transfer protocol")
		}
		var sample hostSample
		if m.localLab {
			hostname, _ := os.Hostname()
			if store.Hostname != hostname {
				return errors.New("local-lab backend is not on this machine")
			}
			age := float64(0)
			sample = hostSample{Time: float64(time.Now().UnixNano()) / 1e9, Generation: "local-lab", StoragePhase: "shared", DiskFreeBytes: 500 << 30, DiskBytes: 1 << 40, ReadSamples: 100, PhysicalCores: 16, MemoryBytes: 16 << 30, AvailableBytes: 8 << 30, NICCapacityBytes: 1e10, ProcessUptimeSeconds: 3600, OldestUnpersistedAgeSeconds: &age}
		} else {
			if err := m.helper(ctx, h, "/sample", nil, &sample); err != nil {
				return fmt.Errorf("%w: %s: %w", errGuards, h, err)
			}
			if m.tuner.maxSpeed == nil && !sample.Offline && !sample.ForegroundMeasured {
				return fmt.Errorf("%w: %s: helper must report committed foreground points", errGuards, h)
			}
		}
		if m.tuner.maxSpeed == nil && sample.Offline && !m.wholeOffline(h) {
			return fmt.Errorf("%w: ready target requires live application telemetry", errGuards)
		}
		if err := m.tuner.observe(h, sample, store); err != nil {
			return err
		}
	}
	return nil
}
func (m *mover) helper(ctx context.Context, host, path string, body any, out any) error {
	var reader io.Reader
	method := http.MethodGet
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return err
		}
		reader = bytes.NewReader(data)
		method = http.MethodPost
	}
	request, err := http.NewRequestWithContext(ctx, method, m.helpers[host]+path, reader)
	if err != nil {
		return err
	}
	request.Header.Set("X-Move-Control", os.Getenv(m.controlTokenEnv))
	response, err := m.client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode != 200 {
		message, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
		return fmt.Errorf("helper %s: %s %s", host, response.Status, message)
	}
	if out != nil {
		return json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(out)
	}
	return nil
}
func (m *mover) readLeases(ctx context.Context, hosts []string, unblock bool) error {
	var names []string
	if !unblock {
		for _, r := range m.planData.Streams {
			err := m.planData.walk(r, func(_ int64, row planRow) error {
				names = append(names, row.Metric)
				if len(names) >= 3 {
					return io.EOF
				}
				return nil
			})
			if err != nil && !errors.Is(err, io.EOF) {
				return err
			}
			if len(names) >= 3 {
				break
			}
		}
	}
	if !unblock && len(names) < 3 && len(m.planData.Whole) > 0 {
		var page []string
		if _, err := m.request(ctx, m.planData.Whole[0].SourceURL, "/v1/transfer/list?limit=3", nil, &page); err != nil {
			return err
		}
		names = page
	}
	var all error
	for _, h := range hosts {
		event("read_isolation", map[string]any{"host": h, "unblock": unblock, "replica": m.replica, "replica_site": m.replicaSite, "probe_metrics": names, "lease_seconds": 90})
		request := map[string]any{"plan_id": m.planData.PlanID, "cluster": m.planData.Spec.Cluster, "site": m.planData.Spec.Site, "replica_site": m.replicaSite, "replica_url": m.replica, "metrics": names, "expires_at": time.Now().Add(90 * time.Second).Unix(), "unblock": unblock}
		result := map[string]any{}
		err := m.helper(ctx, h, "/reads", request, &result)
		if err == nil && !unblock && result["blocked"] != true {
			err = errors.New("host helper did not enable read isolation")
		}
		all = errors.Join(all, err)
	}
	return all
}

func (m *mover) retireSources(ctx context.Context) error {
	sources := map[string]bool{}
	for _, r := range m.planData.Streams {
		if m.stagedDelete(r.sourceURL) {
			sources[r.sourceURL] = true
		}
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var workers sync.WaitGroup
	var once sync.Once
	var first error
	for source := range sources {
		workers.Add(1)
		go func() {
			defer workers.Done()
			if err := m.retireSource(ctx, source); err != nil {
				once.Do(func() { first = err; cancel() })
			}
		}()
	}
	workers.Wait()
	return first
}

func (m *mover) retireSource(ctx context.Context, source string) error {
	m.tuner.mu.Lock()
	previous := m.tuner.hosts[source].storePrevious
	batched := previous != nil && previous.PendingDeleteBatches
	m.tuner.mu.Unlock()
	if batched {
		if err := m.reconcileSource(ctx, source); err != nil {
			return err
		}
	}
	for attempt := 0; !batched; attempt++ {
		if err := m.checkBarrier(); err != nil {
			return err
		}
		var pending []string
		if _, err := m.request(ctx, source, "/v1/transfer/pending-delete", struct{ Operation string }{m.journal.operation}, &pending); err != nil {
			return err
		}
		if len(pending) == 0 {
			break
		}
		if attempt >= 64 {
			return errors.New("source continues changing at retirement barrier")
		}
		needed := map[string]bool{}
		for _, name := range pending {
			needed[name] = true
		}
		for _, r := range m.planData.Streams {
			if r.sourceURL != source {
				continue
			}
			if err := m.planData.walk(r, func(ordinal int64, row planRow) error {
				if !needed[row.Metric] {
					return nil
				}
				delete(needed, row.Metric)
				return m.execute(ctx, task{Stream: r.SHA256, Start: ordinal, End: ordinal + 1, Source: r.sourceURL, Target: r.targetURL, Names: []string{row.Metric}})
			}); err != nil {
				return err
			}
		}
		if len(needed) > 0 {
			return fmt.Errorf("source has %d unplanned metrics; refusing retirement", len(needed))
		}
	}
	if m.reconcile {
		if err := m.checkBarrier(); err != nil {
			return err
		}
		event("verify_source", map[string]any{"source": source, "action": "recheck full catalog/version coverage; retain source history"})
		_, err := m.request(ctx, source, "/v1/transfer/check-staged", struct{ Operation string }{m.journal.operation}, &map[string]int64{})
		return err
	}
	event("retire_source", map[string]any{"source": source, "action": "atomically recheck every catalog version, then range-delete the verified whole store"})
	if _, err := m.request(ctx, source, "/v1/transfer/retire", struct {
		Confirm   bool
		Operation string
	}{true, m.journal.operation}, &map[string]int64{}); err != nil {
		return err
	}
	return nil
}

// Follow the immutable plan once, checking current source revisions in bounded
// batches. Only changed names are recopied; the final retirement transaction
// rejects new/unplanned names or a source that was not actually fenced.
func (m *mover) reconcileSource(ctx context.Context, source string) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	tasks := make(chan task, 2*m.streams)
	var workers sync.WaitGroup
	var first error
	var once sync.Once
	fail := func(err error) {
		if err != nil {
			once.Do(func() { first = err; cancel() })
		}
	}
	for range min(64, max(1, 2*m.streams)) {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for batch := range tasks {
				if ctx.Err() != nil {
					return
				}
				if err := m.reconcileBatch(ctx, batch); err != nil {
					fail(err)
					return
				}
			}
		}()
	}
	fail(m.produce(ctx, tasks, source))
	close(tasks)
	workers.Wait()
	return first
}

func (m *mover) reconcileBatch(ctx context.Context, batch task) error {
	if err := m.checkBarrier(); err != nil {
		return err
	}
	release, err := m.tuner.admit(ctx, batch.Source, batch.Target)
	if err != nil {
		return err
	}
	var pending []string
	_, err = m.request(ctx, batch.Source, "/v1/transfer/pending-delete", struct {
		Operation string
		Names     []string
	}{m.journal.operation, batch.Names}, &pending)
	release()
	if err != nil {
		return err
	}
	needed := make(map[string]bool, len(pending))
	for _, name := range pending {
		needed[name] = true
	}
	var changed []string
	for _, name := range batch.Names {
		if needed[name] {
			changed = append(changed, name)
			delete(needed, name)
		}
	}
	if len(needed) != 0 {
		return errors.New("source returned an unrequested delta name")
	}
	if len(changed) == 0 {
		return nil
	}
	event("delta", map[string]any{"source": batch.Source, "target": batch.Target, "checked": len(batch.Names), "changed": len(changed)})
	batch.Names = changed
	// Filtered deltas need not have contiguous ordinals. Keep their proofs
	// outside baseline ranges: resume must never skip plan rows on this basis.
	batch.Stream += ":delta"
	return m.execute(ctx, batch)
}

func (m *mover) stagedDelete(source string) bool {
	for _, job := range m.planData.Whole {
		if job.SourceURL == source {
			return false
		}
	}
	if m.deleteMode == "batch" {
		return false
	}
	for _, node := range m.planData.Spec.New.Nodes {
		endpoint, _ := origin(node.Endpoint)
		if endpoint == source {
			return false
		}
	}
	return true
}

func (m *mover) previewActions(source string) string {
	actions := "copy raw history; target-wins merge; verify; checkpoint"
	if m.reconcile {
		actions += "; stage versions; require writer fence; reconcile and verify full coverage; retain source history"
	}
	if m.delete {
		if m.stagedDelete(source) {
			actions += "; stage exported versions; recheck full catalog; retire entire verified store"
		} else {
			actions += "; conditionally delete coalesced metric ranges"
		}
	}
	return actions
}
