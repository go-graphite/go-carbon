package downsize

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-graphite/go-carbon/internal/chunkstore"
)

// Names absent on the target are copied whole through the native bulk path:
// the target pulls a pinned donor snapshot (/v1/native/pull) and publishes
// each name's blocks as SSTs, but only for names it does not hold
// (create-if-absent). Names the target holds by then (live writes, an earlier
// copy, a pending import) come back deferred and stay with the metric engine,
// whose target-wins import is unchanged. Each run of copied names becomes a
// done range in the same copy journal, so the metric engine skips it and a
// resume never copies it again. Any native failure leaves its names to the
// metric engine.

// syncAbsentCopyKey marks a Copy whose names were absent on the target at
// discovery. Only those may take the native path; fills and windows never do.
type syncAbsentCopyKey struct{}

const (
	syncCopyEngineNative = "native"
	syncCopyEngineMetric = "metric"
	// syncNativeWaveRanges is the minimum native ranges per absent-name wave.
	syncNativeWaveRanges = 8
)

// syncNativeOperation and the journal identity name every native range, so a
// lost acknowledgement is answered from the target's durable receipt.
var syncNativeOperation = sha256Hex("storectl sync: native absent-name copy v1")

// syncNativeMergeSession pins donors in merge mode: the export carries the
// committed history of names the donor is still writing (cache-queued or
// revised since the pin) instead of deferring them. A plain pin defers every
// live name, which on a donor still taking writes sent most names back to the
// metric engine. Sync owns the delta: verification compares every copied name
// with the donor after --settle and the fill phase copies what is missing,
// exactly as for the metric engine's point-in-time export.
const syncNativeMergeSession = "storectl-sync-absent-v1"

func sha256Hex(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

type syncNativeCapability struct {
	ok      bool
	imports int
	reason  string
}

// nativeCapability caches definite answers per host. Transport errors and
// 5xx are not cached: the wave uses the metric engine and the next wave asks
// again, so a restarting store does not disable the native path for the run.
func (b *syncLiveBackend) nativeCapability(ctx context.Context, host string) syncNativeCapability {
	if cached, ok := b.nativeCaps.Load(host); ok {
		return cached.(syncNativeCapability)
	}
	var capability struct {
		Protocol                              int
		NativeAbsentOnly, DurableRangeReceipt bool
		NativeImportConcurrency               int
		StoreFormat, AcceptStoreFormat        int
	}
	_, err := b.m.requestAttempt(ctx, host, "/v1/native/capabilities", nil, &capability)
	var api *apiError
	switch {
	case err == nil && capability.Protocol == 1 && capability.NativeAbsentOnly && capability.DurableRangeReceipt:
		c := syncNativeCapability{ok: true, imports: max(1, capability.NativeImportConcurrency)}
		b.nativeCaps.Store(host, c)
		return c
	case err == nil:
		c := syncNativeCapability{reason: "native protocol 1 with absent-only receipts not advertised"}
		b.nativeCaps.Store(host, c)
		return c
	case errors.As(err, &api) && api.Status >= 400 && api.Status < 500 && api.Status != http.StatusTooManyRequests:
		c := syncNativeCapability{reason: fmt.Sprintf("no native endpoint (HTTP %d)", api.Status)}
		b.nativeCaps.Store(host, c)
		return c
	}
	return syncNativeCapability{reason: "capability probe failed: " + err.Error()}
}

// nativeRanges cuts names into receiver-sized ranges. The cut depends only on
// the full wave list, so a resumed wave sends the same range identities.
func nativeRanges(names []string) [][2]int {
	var out [][2]int
	for start := 0; start < len(names); {
		end, size := start, 0
		for end < len(names) && int64(end-start) < nativeRangeNames && (end == start || size+len(names[end]) <= chunkstore.NativeMaxRangeNameBytes) {
			size += len(names[end])
			end++
		}
		out = append(out, [2]int{start, end})
		start = end
	}
	return out
}

// nativeCopy copies the wave's uncovered names through /v1/native/pull and
// credits copied runs in the copy journal. It returns the names it copied.
func (b *syncLiveBackend) nativeCopy(ctx context.Context, source, target string, names []string, namesPath, journal string, progress func(int64, bool)) (int, error) {
	if b.o.copyEngine != syncCopyEngineNative || len(names) == 0 {
		return 0, nil
	}
	if absent, _ := ctx.Value(syncAbsentCopyKey{}).(bool); !absent {
		return 0, nil
	}
	targetCap := b.nativeCapability(ctx, target)
	sourceCap := b.nativeCapability(ctx, source)
	if !targetCap.ok || !sourceCap.ok {
		if _, logged := b.nativeFallbackLogged.LoadOrStore(source+" "+target, true); !logged {
			event("sync_native_fallback", map[string]any{"level": "warn", "source": source, "target": target, "source_reason": sourceCap.reason, "target_reason": targetCap.reason, "engine": "metric"})
		}
		return 0, ctx.Err()
	}
	options := b.copyOptions(source, target, namesPath, journal, len(names))
	identity, err := copyIdentity(options)
	if err != nil {
		return 0, err
	}
	j, err := openCopyJournal(journal, identity)
	if err != nil {
		return 0, err
	}
	defer j.close()
	j.onProgress = progress
	cover := j.cover(source)
	covered := make([]bool, len(names))
	pending := 0
	for i, n := range names {
		covered[i] = cover(n)
		if !covered[i] {
			pending++
		}
	}
	if pending == 0 {
		return 0, nil
	}
	var work [][2]int
	for _, r := range nativeRanges(names) {
		for i := r[0]; i < r[1]; i++ {
			if !covered[i] {
				work = append(work, r)
				break
			}
		}
	}
	session, release, err := b.nativeSession(ctx, source)
	if err != nil {
		if ctx.Err() != nil {
			return 0, ctx.Err()
		}
		event("sync_native_fallback", map[string]any{"level": "warn", "source": source, "target": target, "reason": "source snapshot: " + err.Error(), "engine": "metric"})
		return 0, nil
	}
	defer release()
	committed, _ := ctx.Value(syncCopyProgressKey{}).(func(int))
	plan := sha256Hex(identity)
	stream := sha256Hex(source + "\n" + target)
	// One pull imports while the next waits in the target's admission queue,
	// so the target never idles between ranges. Queued pulls hold no donor data.
	workers := min(len(work), targetCap.imports+1)
	var (
		next   atomic.Int64
		copied atomic.Int64
		wg     sync.WaitGroup
		mu     sync.Mutex
		first  error
	)
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				i := int(next.Add(1) - 1)
				if i >= len(work) || ctx.Err() != nil {
					return
				}
				n, err := b.nativeRange(ctx, j, source, target, session, plan, stream, names, work[i], committed)
				copied.Add(int64(n))
				if err != nil {
					mu.Lock()
					first = errors.Join(first, err)
					mu.Unlock()
					return
				}
			}
		}()
	}
	wg.Wait()
	if err := ctx.Err(); err != nil {
		return int(copied.Load()), err
	}
	return int(copied.Load()), first
}

// nativeSession pins a donor snapshot for one wave and releases it after.
func (b *syncLiveBackend) nativeSession(ctx context.Context, source string) (string, func(), error) {
	var response struct{ ID, MergeSession string }
	if _, err := b.m.requestAttempt(ctx, source, "/v1/native/source", struct{ MergeSession string }{syncNativeMergeSession}, &response); err != nil {
		return "", nil, err
	}
	if len(response.ID) != 32 || response.MergeSession != syncNativeMergeSession {
		if len(response.ID) == 32 {
			b.releaseNativeSession(ctx, source, response.ID)
		}
		return "", nil, errors.New("invalid native merge source session")
	}
	return response.ID, func() { b.releaseNativeSession(ctx, source, response.ID) }, nil
}

func (b *syncLiveBackend) releaseNativeSession(ctx context.Context, source, id string) {
	cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), stopCleanupTimeout)
	defer cancel()
	request, err := http.NewRequestWithContext(cleanup, http.MethodDelete, source+"/v1/native/source/"+id, nil)
	if err != nil {
		return
	}
	if response, err := b.m.client.Do(request); err == nil {
		response.Body.Close()
	}
}

// nativeRange pulls one range. Only infrastructure failures (cancellation,
// journal I/O) are returned; a refused or failed pull leaves its names to the
// metric engine.
func (b *syncLiveBackend) nativeRange(ctx context.Context, j *copyJournal, source, target, session, plan, stream string, names []string, r [2]int, committed func(int)) (int, error) {
	if err := b.health.wait(ctx); err != nil {
		return 0, err
	}
	chunk := names[r[0]:r[1]]
	request := chunkstore.NativeRange{Operation: syncNativeOperation, Plan: plan, Stream: stream, Source: source, Start: int64(r[0]), End: int64(r[1]), Names: chunk, MergeSession: syncNativeMergeSession}
	identity, namesHash, err := request.Identity()
	if err != nil {
		event("sync_native_range_skipped", map[string]any{"level": "warn", "source": source, "first": chunk[0], "names": len(chunk), "error": err.Error()})
		return 0, nil
	}
	started := time.Now()
	var receipt chunkstore.NativeReceipt
	_, err = b.m.requestAttempt(ctx, target, "/v1/native/pull", struct {
		Range   chunkstore.NativeRange
		Session string
	}{request, session}, &receipt)
	if err == nil {
		err = receipt.Validate()
	}
	if err == nil && (receipt.Identity != identity || receipt.NamesSHA256 != namesHash || receipt.Count != len(chunk) || receipt.CopiedCount+receipt.DeferredCount != len(chunk) || len(receipt.Copied) != (len(chunk)+7)/8) {
		err = errors.New("native receipt differs from the requested range")
	}
	if err != nil {
		if ctx.Err() != nil {
			return 0, ctx.Err()
		}
		event("sync_native_range_failed", map[string]any{"level": "warn", "source": source, "target": target, "first": chunk[0], "names": len(chunk), "error": err.Error(), "then": "metric engine"})
		return 0, nil
	}
	var rows []copyRow
	took := time.Since(started).Seconds()
	for i := 0; i < len(chunk); {
		if !receipt.IsCopied(i) {
			i++
			continue
		}
		start := i
		for i < len(chunk) && receipt.IsCopied(i) {
			i++
		}
		row := copyRow{Kind: "done", First: chunk[start], Last: chunk[i-1], Names: i - start, Seconds: took}
		if len(rows) == 0 {
			row.Bytes = receipt.Bytes
		}
		rows = append(rows, row)
	}
	if len(rows) > 0 {
		if err := j.appendRows(rows); err != nil {
			return 0, err
		}
	}
	if committed != nil && receipt.CopiedCount > 0 {
		committed(receipt.CopiedCount)
	}
	for _, skip := range receipt.Skipped {
		nativeSkipEvent(receipt.Identity, skip)
	}
	event("sync_native_range", map[string]any{"source": source, "first": chunk[0], "last": chunk[len(chunk)-1], "names": len(chunk), "copied": receipt.CopiedCount, "deferred": receipt.DeferredCount, "bytes": receipt.Bytes, "seconds": took, "receipt": receipt.Identity})
	if err := b.o.rate.wait(ctx, receipt.Bytes); err != nil {
		return receipt.CopiedCount, err
	}
	return receipt.CopiedCount, nil
}
