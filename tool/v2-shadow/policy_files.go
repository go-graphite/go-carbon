package main

import (
	"errors"
	"flag"
	"fmt"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2core"
	"github.com/go-graphite/go-carbon/internal/v2part"
	"github.com/go-graphite/go-carbon/persister"
	whisper "github.com/go-graphite/go-whisper"
)

var classicSchemasPath = flag.String("classic-schemas", "", "read-only copy of classic storage-schemas.conf; requires classic-aggregation and full-l3")
var classicAggregationPath = flag.String("classic-aggregation", "", "read-only copy of classic storage-aggregation.conf")

type classicPolicyFiles struct {
	schemas     persister.WhisperSchemas
	aggregation *persister.WhisperAggregation
}

func readClassicPolicyFiles(schemas, aggregation string) (*classicPolicyFiles, error) {
	if schemas == "" && aggregation == "" {
		return nil, nil
	}
	if schemas == "" || aggregation == "" {
		return nil, errors.New("both classic policy files are required")
	}
	s, err := persister.ReadWhisperSchemas(schemas)
	if err != nil {
		return nil, err
	}
	a, err := persister.ReadWhisperAggregation(aggregation)
	if err != nil {
		return nil, err
	}
	return &classicPolicyFiles{s, a}, nil
}

// Match is the same first-match schema and aggregation selection used by classic
// when creating a name. This describes creation policy, not historical file policy.
func (p *classicPolicyFiles) policy(name string) ([]byte, error) {
	s, ok := p.schemas.Match(name)
	if !ok {
		return nil, fmt.Errorf("no classic schema for %q", name)
	}
	a := p.aggregation.Match(name)
	methods := map[whisper.AggregationMethod]sealrollup.Method{
		whisper.Average: sealrollup.Average, whisper.Sum: sealrollup.Sum,
		whisper.Last: sealrollup.Last, whisper.Max: sealrollup.Max, whisper.Min: sealrollup.Min,
	}
	m, ok := methods[a.AggregationMethod()]
	if !ok {
		return nil, fmt.Errorf("unsupported classic aggregation for %q", name)
	}
	cfg := sealrollup.Config{Method: m, XFilesFactor: float32(a.XFilesFactor())}
	for _, r := range s.Retentions {
		cfg.Archives = append(cfg.Archives, sealrollup.Archive{Step: int64(r.SecondsPerPoint()), Count: int64(r.NumberOfPoints())})
	}
	return v2core.EncodeRollupPolicy(v2core.RollupPolicy{Name: v2part.ID(name), Group: "shadow", Config: cfg})
}
