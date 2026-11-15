// parts-ops-check previews operational decisions on an exported catalog without
// opening the serving database or starting listeners.
package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/go-graphite/go-carbon/carbonserver"
	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/partsops"
	"github.com/go-graphite/go-carbon/persister"
	"github.com/go-graphite/go-carbon/points"
)

type metricRow struct {
	Scope                partsops.Scope
	Name                 string
	Retentions           []store.Retention
	Generation, Revision uint64
	LastUpdate           time.Time
	Pending              bool
}
type input struct {
	Metrics    []metricRow
	Partitions []partsops.Partition
	Queries    []string
	Events     []struct {
		Name    string
		Points  int
		InCache bool
	}
}
type match struct {
	Name string
	Leaf bool
}
type output struct {
	Expiration     partsops.ExpirationPlan
	DropPartitions []partsops.Partition
	Find           map[string][]match
	Throttled      []bool
}

func run(args []string, w io.Writer) error {
	fs := flag.NewFlagSet("parts-ops-check", flag.ContinueOnError)
	in := fs.String("input", "", "exported catalog JSON (required)")
	exp := fs.String("expiration", "", "existing storage-expiration.conf")
	quotas := fs.String("quotas", "", "existing storage-quotas.conf")
	schemasFile := fs.String("schemas", "", "storage-schemas.conf (required with quotas)")
	fallback := fs.Duration("expiration-default", 0, "configured inactivity expiration")
	nowText := fs.String("now", "", "RFC3339 evaluation time (required)")
	trie := fs.Bool("trie", true, "use current trie find backend")
	dry := fs.Bool("dry-run", false, "match configured expiration dry-run")
	ignorePhysical := fs.Bool("ignore-physical-quotas", false, "match shared-storage quota configuration")
	freq := fs.Duration("report-frequency", time.Minute, "configured quota report interval")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *in == "" || *nowText == "" {
		return errors.New("-input and -now are required")
	}
	now, err := time.Parse(time.RFC3339, *nowText)
	if err != nil {
		return err
	}
	data, err := os.ReadFile(*in)
	if err != nil {
		return err
	}
	var source input
	if err := json.Unmarshal(data, &source); err != nil {
		return err
	}
	var s partsops.Snapshot
	var metadata []store.Metadata
	pending := make(map[string]bool)
	for _, r := range source.Metrics {
		m := store.Metadata{MetricConfig: store.MetricConfig{Name: r.Name, Retentions: r.Retentions}, Generation: r.Generation, Revision: r.Revision, LastUpdate: r.LastUpdate}
		s.Metrics = append(s.Metrics, partsops.Metric{Scope: r.Scope, Metadata: m})
		metadata = append(metadata, m)
		pending[r.Name] = r.Pending
	}
	policy := partsops.ExpirationPolicy{Fallback: *fallback, DryRun: *dry}
	if *exp != "" {
		policy.Rules, err = persister.ReadWhisperExpiration(*exp)
		if err != nil {
			return err
		}
	}
	out := output{Expiration: partsops.PlanExpiration(s, policy, now, func(n string) bool { return pending[n] }), Find: make(map[string][]match)}
	out.DropPartitions, err = partsops.ExpiredPartitions(source.Partitions, now)
	if err != nil {
		return err
	}
	opts := carbonserver.PartsIndexOptions{Trie: *trie, ReportFrequency: *freq}
	if *quotas != "" {
		if *schemasFile == "" {
			return errors.New("-schemas required to estimate new metric quotas")
		}
		rules, err := persister.ReadWhisperQuotas(*quotas)
		if err != nil {
			return err
		}
		opts.Quotas, err = carbonserver.PartsQuotaRules(rules, *freq, *ignorePhysical)
		if err != nil {
			return err
		}
		schemas, err := persister.ReadWhisperSchemas(*schemasFile)
		if err != nil {
			return err
		}
		for _, e := range source.Events {
			if _, ok := schemas.Match(e.Name); !ok {
				return fmt.Errorf("no schema for %q", e.Name)
			}
		}
		opts.EstimateSize = func(name string) (int64, int64, int64) {
			schema, ok := schemas.Match(name)
			if !ok {
				return 0, 0, 0
			}
			logical := int64(16 + 12*len(schema.Retentions))
			var dp int64
			for _, r := range schema.Retentions {
				dp += int64(r.NumberOfPoints())
			}
			logical += 12 * dp
			return logical, 0, dp
		}
	}
	index, err := carbonserver.NewPartsCatalogIndex(metadata, opts)
	if err != nil {
		return err
	}
	for _, q := range source.Queries {
		names, leafs, err := index.Find(q)
		if err != nil {
			return err
		}
		for i, n := range names {
			out.Find[q] = append(out.Find[q], match{Name: n, Leaf: leafs[i]})
		}
	}
	for _, e := range source.Events {
		if e.Points < 0 || e.Points > 1000000 {
			return errors.New("event point count out of range")
		}
		out.Throttled = append(out.Throttled, index.Throttle(&points.Points{Metric: e.Name, Data: make([]points.Point, e.Points)}, e.InCache))
	}
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(out)
}
func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
