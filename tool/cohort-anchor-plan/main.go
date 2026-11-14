// cohort-anchor-plan evaluates one captured native wire entirely in memory.
// It has no store path, listener, HTTP client or production mutation operation.
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"slices"
	"strings"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/whisperio"
)

type memory map[string][]byte

func (m memory) Get(k []byte) ([]byte, error) {
	v, ok := m[string(k)]
	if !ok {
		return nil, store.ErrNotFound
	}
	return bytes.Clone(v), nil
}
func (m memory) Set(k, v []byte) error { m[string(k)] = bytes.Clone(v); return nil }
func (m memory) Delete(k []byte) error { delete(m, string(k)); return nil }
func (m memory) DeletePrefix(prefix []byte) error {
	for k := range m {
		if strings.HasPrefix(k, string(prefix)) {
			delete(m, k)
		}
	}
	return nil
}
func (m memory) Visit(prefix []byte, fn func([]byte, []byte) error) error {
	var keys []string
	for k := range m {
		if strings.HasPrefix(k, string(prefix)) {
			keys = append(keys, k)
		}
	}
	slices.Sort(keys)
	for _, k := range keys {
		if err := fn([]byte(k), bytes.Clone(m[k])); err != nil {
			return err
		}
	}
	return nil
}

type result struct {
	Name             string                `json:"name"`
	Version          store.TransferVersion `json:"version"`
	WireSHA          string                `json:"native_wire_sha256"`
	Archives         []store.AnchorArchive `json:"archives"`
	Status           string                `json:"status"`
	Applied          bool                  `json:"applied"`
	RuntimeQualified bool                  `json:"runtime_qualified"`
	Points           int                   `json:"physical_points"`
	BeforeSHA        string                `json:"before_multiset_sha256,omitempty"`
	AfterSHA         string                `json:"after_multiset_sha256,omitempty"`
	FetchPoints      int                   `json:"selected_fetch_points"`
	Nonselected      int                   `json:"unselected_archive_points"`
	ExpiredOrFuture  int                   `json:"unqueryable_time_points"`
	Writes           int                   `json:"model_writes"`
	Deletes          int                   `json:"model_deletes"`
}

func multiset(s store.Snapshot) (string, int) {
	h := sha256.New()
	n := 0
	for i, a := range s.Archives {
		fmt.Fprintf(h, "archive=%d step=%d count=%d\n", i, a.Retention.Step, a.Retention.Count)
		points := slices.Clone(a.Points)
		slices.SortFunc(points, func(a, b store.Point) int {
			if a.Timestamp < b.Timestamp {
				return -1
			}
			if a.Timestamp > b.Timestamp {
				return 1
			}
			x, y := math.Float64bits(a.Value), math.Float64bits(b.Value)
			if x < y {
				return -1
			}
			if x > y {
				return 1
			}
			return 0
		})
		for _, p := range points {
			fmt.Fprintf(h, "%d:%016x\n", p.Timestamp, math.Float64bits(p.Value))
			n++
		}
	}
	return hex.EncodeToString(h.Sum(nil)), n
}

func evaluate(raw []byte, now time.Time) (result, error) {
	var out result
	sum := sha256.Sum256(raw)
	out.WireSHA = hex.EncodeToString(sum[:])
	records, err := store.ReadTransfer(bytes.NewReader(raw), 64<<20)
	if err != nil {
		return out, err
	}
	if len(records) != 1 || records[0].Version.ID == 0 || !records[0].Continuation {
		return out, errors.New("one existing continuation native record required")
	}
	r := records[0]
	out.Name = r.Config.Name
	out.Version = r.Version
	kv := memory{}
	for _, pair := range r.Keys {
		if _, ok := kv[string(pair.Key)]; ok {
			return out, errors.New("duplicate native key")
		}
		kv[string(pair.Key)] = bytes.Clone(pair.Value)
	}
	ctx := context.Background()
	c := &whisperio.ContinuationCodec{}
	plan, err := c.PlanArchiveAnchors(ctx, kv)
	if err != nil {
		return out, err
	}
	out.Archives = plan.Archives
	out.Writes = len(plan.Writes)
	out.Deletes = len(plan.Deletes)
	for _, a := range plan.Archives {
		if a.Blocked != "" {
			if out.Writes+out.Deletes != 0 {
				return out, errors.New("blocked plan has writes")
			}
			out.Status = "BLOCKED"
			return out, nil
		}
	}
	if out.Writes+out.Deletes == 0 {
		out.Status = "NOOP"
		return out, nil
	}
	m := store.Metadata{MetricConfig: r.Config, ID: r.Version.ID, Generation: r.Version.Generation, Revision: r.Version.Revision, Continuation: r.Continuation}
	clock := func() time.Time { return now }
	before, err := c.Snapshot(ctx, kv, m, clock)
	if err != nil {
		return out, err
	}
	out.BeforeSHA, out.Points = multiset(before)
	for _, key := range plan.Deletes {
		if err := kv.Delete(key); err != nil {
			return out, err
		}
	}
	for _, pair := range plan.Writes {
		if err := kv.Set(pair.Key, pair.Value); err != nil {
			return out, err
		}
	}
	after, err := c.Snapshot(ctx, kv, m, clock)
	if err != nil {
		return out, err
	}
	out.AfterSHA, _ = multiset(after)
	if err := store.EqualSnapshots(before, after); err != nil {
		return out, fmt.Errorf("physical multiset: %w", err)
	}
	if out.BeforeSHA != out.AfterSHA {
		return out, errors.New("physical multiset SHA differs")
	}
	for ai, a := range before.Archives {
		seen := map[int64]bool{}
		for _, p := range a.Points {
			if seen[p.Timestamp] {
				continue
			}
			seen[p.Timestamp] = true
			if p.Timestamp <= now.Unix()-int64(a.Retention.Step*a.Retention.Count) || p.Timestamp > now.Unix() {
				out.ExpiredOrFuture++
				continue
			}
			from := int(p.Timestamp) - a.Retention.Step
			age := now.Unix() - int64(from)
			selected := len(m.Retentions) - 1
			for i, ret := range m.Retentions {
				if int64(ret.Step*ret.Count) >= age {
					selected = i
					break
				}
			}
			if selected != ai {
				out.Nonselected++
				continue
			}
			got, err := c.Fetch(ctx, kv, m, from, int(p.Timestamp), clock)
			if err != nil {
				return out, err
			}
			if len(got.Values) != 1 || math.Float64bits(got.Values[0]) != math.Float64bits(p.Value) {
				return out, fmt.Errorf("selected native Fetch differs at archive %d timestamp %d", ai, p.Timestamp)
			}
			out.FetchPoints++
		}
	}
	again, err := c.PlanArchiveAnchors(ctx, kv)
	if err != nil {
		return out, err
	}
	if len(again.Writes)+len(again.Deletes) != 0 {
		return out, errors.New("anchor model is not idempotent")
	}
	for _, a := range again.Archives {
		if a.Changed || a.Blocked != "" || a.Misplaced != 0 {
			return out, errors.New("anchor model remains inconsistent")
		}
	}
	out.Status = "MODEL_VERIFIED"
	return out, nil
}

func main() {
	at := flag.Int64("at", 0, "fixed physical-capture clock, Unix seconds")
	flag.Parse()
	if *at <= 0 {
		fmt.Fprintln(os.Stderr, "positive fixed capture clock required")
		os.Exit(2)
	}
	raw, err := io.ReadAll(io.LimitReader(os.Stdin, 16<<20+1))
	if err == nil && len(raw) > 16<<20 {
		err = errors.New("native wire exceeds 16 MiB")
	}
	var out result
	if err == nil {
		out, err = evaluate(raw, time.Unix(*at, 0))
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	if err := json.NewEncoder(os.Stdout).Encode(out); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
}
