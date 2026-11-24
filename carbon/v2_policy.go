package carbon

import (
	"fmt"
	"slices"
	"strings"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2core"
	"github.com/go-graphite/go-carbon/internal/v2daemon"
	"github.com/go-graphite/go-carbon/internal/v2part"
	"github.com/go-graphite/go-carbon/internal/v2read"
)

// classicNames is the sorted, immutable inventory returned by openV2Classic.
// Reuse that inventory rather than caching names discovered during admission.
func (app *App) v2PolicyResolver(mode string, classicNames []string) v2daemon.PolicyResolver {
	c := app.Config
	return func(name string) ([]byte, error) {
		schema, ok := c.Whisper.Schemas.Match(name)
		if !ok {
			return nil, fmt.Errorf("no v2 schema for %q", name)
		}
		agg := c.Whisper.Aggregation.Match(name)
		cfg := sealrollup.Config{Method: sealrollup.Method(agg.AggregationMethod()), XFilesFactor: float32(agg.XFilesFactor())}
		for _, a := range schema.Retentions {
			cfg.Archives = append(cfg.Archives, sealrollup.Archive{Step: int64(a.SecondsPerPoint()), Count: int64(a.NumberOfPoints())})
		}
		_, classic := slices.BinarySearch(classicNames, name)
		// Only unambiguous classic names can use the inventory's absence proof.
		// Slash/empty components may alias an inventoried file; NUL must retain
		// the old path refusal. Live dual write can create names after startup.
		ambiguous := strings.ContainsAny(name, "/\x00") || strings.HasPrefix(name, ".") || strings.HasSuffix(name, ".") || strings.Contains(name, "..")
		if mode != v2Shadow && (mode == v2ServingDual || classic || ambiguous) {
			policy := v2read.Metric{AggregationMethod: agg.AggregationMethod().String(), XFilesFactor: cfg.XFilesFactor}
			for _, archive := range cfg.Archives {
				policy.Archives = append(policy.Archives, v2read.Archive{Step: archive.Step, Points: archive.Count})
			}
			if err := app.Carbonserver.ValidateV2UnionPolicy(name, policy); err != nil {
				return nil, err
			}
		}
		return v2core.EncodeRollupPolicy(v2core.RollupPolicy{Name: v2part.ID(name), Group: "local", Config: cfg})
	}
}
