package downsize

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-graphite/go-carbon/internal/chunkstore"
	hashing "github.com/go-graphite/go-carbon/internal/downsize/buckyhash"
)

type nativeController struct {
	*mover
	mu       sync.Mutex
	sessions map[string]*nativeRemoteSession
	issued   atomic.Int64
}

func runNative(ctx context.Context, o options) error {
	if o.delete || o.reconcile || o.blockReads || o.observeOnly != 0 || o.localSources != "" || o.localTarget != "" {
		return errors.New("native phase1 requires a copy-only range plan without replacement/legacy local-source modes")
	}
	if o.streams < 0 || o.maxBatches < 0 {
		return errors.New("invalid native concurrency/scope")
	}
	p, err := readPlanStructure(o.plan, false)
	if err != nil {
		return err
	}
	if len(p.Whole) > 0 {
		return errors.New("native phase1 is combine/resize, not whole-store replacement")
	}
	if !o.apply {
		for _, r := range p.Streams {
			event("would_native_copy", map[string]any{"plan_id": p.PlanID, "stream": r.Path, "source": r.sourceURL, "target": r.targetURL, "metrics": r.Metrics, "then": "native snapshot; streaming SST; create-if-absent; durable receipt; same-journal deferred combine"})
		}
		return nil
	}
	if o.localLab {
		for _, r := range p.Streams {
			if !loopbackOrigin(r.sourceURL) || !loopbackOrigin(r.targetURL) {
				return errors.New("local lab requires loopback endpoints")
			}
		}
	}
	if o.deleteMode == "" {
		o.deleteMode = "auto"
	}
	j, err := openJournal(o.state, p.PlanID+fmt.Sprintf(":delete=false:mode=%s", o.deleteMode))
	if err != nil {
		return err
	}
	defer j.close()
	j.preserveRows = true
	client := &http.Client{Transport: &http.Transport{MaxIdleConns: runtime.NumCPU() * 4, MaxIdleConnsPerHost: runtime.NumCPU() * 2}, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	defer client.CloseIdleConnections()
	m := &mover{options: o, planData: p, journal: j, client: client}
	c := &nativeController{mover: m, sessions: make(map[string]*nativeRemoteSession)}
	defer c.release()
	hosts := make(map[string]bool)
	targets := make(map[string]bool)
	for _, r := range p.Streams {
		hosts[r.sourceURL] = true
		hosts[r.targetURL] = true
		targets[r.targetURL] = true
	}
	workers := o.streams
	for host := range hosts {
		var capability struct {
			Protocol                              int
			NativeAbsentOnly, DurableRangeReceipt bool
			NativeExactCombine                    bool
			CPULimit                              int
			Node                                  string
		}
		if _, err := m.requestOnce(ctx, host, "/v1/native/capabilities", nil, &capability); err != nil {
			return fmt.Errorf("native build required on %s: %w", host, err)
		}
		if capability.Protocol != 1 || !capability.NativeAbsentOnly || !capability.DurableRangeReceipt {
			return fmt.Errorf("unsupported native capability on %s", host)
		}
		if o.bulkCombine && targets[host] && !capability.NativeExactCombine {
			return fmt.Errorf("native exact combine build required on %s", host)
		}
		if !o.localLab {
			matched := false
			for _, ring := range []topology{p.Spec.Old, p.Spec.New} {
				for _, member := range ring.Nodes {
					endpoint, e := origin(member.Endpoint)
					if e != nil {
						return e
					}
					if endpoint != host {
						continue
					}
					node, e := hashing.NewNodeParser(member.Node)
					if e != nil {
						return e
					}
					if capability.Node != node.Server {
						return fmt.Errorf("native physical identity mismatch on %s: %s != %s", host, capability.Node, node.Server)
					}
					matched = true
				}
			}
			if !matched {
				return fmt.Errorf("native host absent from plan: %s", host)
			}
		}
		if o.streams == 0 && targets[host] {
			workers += max(1, capability.CPULimit)
		}
	}
	if workers == 0 {
		workers = runtime.NumCPU()
	}
	event("native_operation", map[string]any{"plan_id": p.PlanID, "operation": j.operation, "state": o.state, "workers": workers, "hardware_sized": o.streams == 0})
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	jobs := make(chan route)
	var wg sync.WaitGroup
	var first error
	var once sync.Once
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for r := range jobs {
				if err := c.route(ctx, r); err != nil {
					once.Do(func() { first = err; cancel() })
					return
				}
			}
		}()
	}
enqueue:
	for _, r := range p.Streams {
		select {
		case jobs <- r:
		case <-ctx.Done():
			break enqueue
		}
	}
	close(jobs)
	wg.Wait()
	if first != nil {
		return first
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	var deferred int64
	for _, t := range j.tasks {
		if t.Native != nil {
			for i := range t.Native.Receipt.Count {
				if !t.Native.done(i) {
					deferred++
				}
			}
		} else if t.Phase != "done" {
			deferred += t.End - t.Start
		}
	}
	event("native_baseline_complete", map[string]any{"plan_id": p.PlanID, "deferred_names": deferred, "max_batches": o.maxBatches, "same_journal": true})
	if o.bulkOnly || o.maxBatches > 0 || deferred == 0 {
		return nil
	}
	// The native pass has no old governor. Only its explicit deferred work now
	// enters the unchanged reviewed metric engine and its existing safety policy.
	c.release()
	j.close()
	o.engine = "metric"
	o.bulkCombine = false
	return run(ctx, o)
}

type nativeRemoteSession struct {
	mu sync.Mutex
	id string
}

func (c *nativeController) session(ctx context.Context, source string) (string, error) {
	c.mu.Lock()
	entry := c.sessions[source]
	if entry == nil {
		entry = &nativeRemoteSession{}
		c.sessions[source] = entry
	}
	c.mu.Unlock()
	entry.mu.Lock()
	defer entry.mu.Unlock()
	if entry.id != "" {
		return entry.id, nil
	}
	var response struct{ ID string }
	if _, err := c.requestOnce(ctx, source, "/v1/native/source", struct{}{}, &response); err != nil {
		return "", err
	}
	if len(response.ID) != 32 {
		return "", errors.New("invalid native source session")
	}
	entry.id = response.ID
	return response.ID, nil
}
func (c *nativeController) release() {
	c.mu.Lock()
	sessions := c.sessions
	c.sessions = make(map[string]*nativeRemoteSession)
	c.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	for source, entry := range sessions {
		wg.Add(1)
		go func() {
			defer wg.Done()
			entry.mu.Lock()
			id := entry.id
			entry.id = ""
			entry.mu.Unlock()
			if id == "" {
				return
			}
			r, err := http.NewRequestWithContext(ctx, http.MethodDelete, source+"/v1/native/source/"+id, nil)
			if err != nil {
				return
			}
			if token := os.Getenv(c.tokenEnv); token != "" {
				r.Header.Set("X-Buckyd-Authorization", token)
			}
			response, err := c.client.Do(r)
			if err == nil {
				response.Body.Close()
			}
		}()
	}
	wg.Wait()
}

func (c *nativeController) route(ctx context.Context, r route) error {
	if c.maxBatches > 0 && c.issued.Load() >= c.maxBatches {
		return nil
	}
	c.stateMu.Lock()
	completed := c.journal.ranges(r.SHA256)
	c.stateMu.Unlock()
	// A prior importer already verified these immutable plan ordinals. No
	// new data operation needs their names, and parsing tens of millions of
	// completed rows delays a resumed native canary without verifying new work.
	if nativeStreamCovered(completed, r.Metrics) {
		return nil
	}
	var names []string
	if err := c.planData.walk(r, func(_ int64, row planRow) error { names = append(names, row.Metric); return nil }); err != nil {
		return err
	}
	for _, pending := range completed {
		if pending.Native != nil && pending.Native.Intent {
			if pending.Start < 0 || pending.End > int64(len(names)) {
				return errors.New("native intent outside plan stream")
			}
			if err := c.executeRange(ctx, pending, names[pending.Start:pending.End]); err != nil {
				return err
			}
		}
	}
	var cursor int64
	skip := 0
	for cursor < int64(len(names)) {
		if err := ctx.Err(); err != nil {
			return err
		}
		for skip < len(completed) && completed[skip].End <= cursor {
			skip++
		}
		if skip < len(completed) && completed[skip].Start <= cursor {
			cursor = completed[skip].End
			continue
		}
		end := int64(len(names))
		if skip < len(completed) {
			end = min(end, completed[skip].Start)
		}
		if c.maxBatches > 0 && c.issued.Add(1) > c.maxBatches {
			return nil
		}
		t := task{Kind: "native-range", Stream: r.SHA256, Start: cursor, End: end, Source: r.sourceURL, Target: r.targetURL}
		if err := c.executeRange(ctx, t, names[cursor:end]); err != nil {
			return err
		}
		cursor = end
	}
	return nil
}
func (c *nativeController) executeRange(ctx context.Context, t task, names []string) error {
	request := chunkstore.NativeRange{Operation: c.journal.operation, Plan: c.planData.PlanID, Stream: t.Stream, Source: t.Source, Start: t.Start, End: t.End, Names: names, Combine: c.bulkCombine}
	session, err := c.session(ctx, t.Source)
	if err != nil {
		return err
	}
	identity, namesHash, err := request.Identity()
	if err != nil {
		return err
	}
	intent := t
	intent.Kind = "native-range"
	intent.Phase = "verified"
	intent.Proofs = nil
	intent.Names = nil
	intent.Native = &nativeRangeState{Intent: true, Receipt: chunkstore.NativeReceipt{Identity: identity, NamesSHA256: namesHash, Count: len(names)}}
	c.stateMu.Lock()
	err = c.journal.save(intent)
	c.stateMu.Unlock()
	if err != nil {
		return err
	}
	var receipt chunkstore.NativeReceipt
	event("native_transfer", map[string]any{"source": t.Source, "target": t.Target, "stream": t.Stream, "start": t.Start, "end": t.End})
	if _, err := c.requestOnce(ctx, t.Target, "/v1/native/pull", struct {
		Range   chunkstore.NativeRange
		Session string
	}{request, session}, &receipt); err != nil {
		return err
	}
	if err := receipt.Validate(); err != nil {
		return err
	}
	if receipt.Identity != identity || receipt.NamesSHA256 != namesHash || receipt.Count != len(names) || receipt.CopiedCount+receipt.DeferredCount != len(names) || len(receipt.Copied) != (len(names)+7)/8 {
		return errors.New("native durable ack differs from plan range")
	}
	t.Bytes = receipt.Bytes
	t.Native = &nativeRangeState{Receipt: receipt, Done: bytes.Clone(receipt.Copied)}
	t.Phase = "verified"
	if t.Native.complete() {
		t.Phase = "done"
		t.Native = nil
	}
	c.stateMu.Lock()
	err = c.journal.save(t)
	c.stateMu.Unlock()
	if err != nil {
		return err
	}
	event("native_verified", map[string]any{"stream": t.Stream, "start": t.Start, "end": t.End, "bytes": receipt.Bytes, "copied_names": receipt.CopiedCount, "deferred_names": receipt.DeferredCount, "receipt": receipt.Identity})
	return nil
}

// ranges is sorted by journal.ranges; acknowledged native deferrals are also
// covered for this baseline. An unresolved native intent still needs its names
// to query the durable receiver receipt and must not be skipped.
func nativeStreamCovered(ranges []task, count int64) bool {
	var end int64
	for _, t := range ranges {
		if t.Native != nil && t.Native.Intent {
			return false
		}
		if t.Start > end {
			return false
		}
		if t.End > end {
			end = t.End
		}
	}
	return end >= count
}
