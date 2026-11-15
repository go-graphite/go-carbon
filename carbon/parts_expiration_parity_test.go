package carbon

import (
	"context"
	"encoding/json"
	"math"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/carbonserver"
	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/partsformat"
	"github.com/go-graphite/go-carbon/internal/partsops"
	"github.com/go-graphite/go-carbon/persister"
)

type partsExpirationOracle struct{ initialized, deleted bool }

func (*partsExpirationOracle) ListPage(context.Context, string, string, int) ([]store.Metadata, error) {
	return nil, nil
}

func TestPartsQuotaTranslationDifferential(t *testing.T) {
	for _, frequency := range []time.Duration{time.Minute, 5 * time.Minute, 30 * time.Second} {
		for _, ignore := range []bool{false, true} {
			for _, physical := range []int64{-1, 0, math.MaxInt64} {
				cfg := NewConfig()
				cfg.Whisper.StorageBackend = "pebble-chunk"
				cfg.Whisper.PebbleChunkIgnorePhysicalQuotas = ignore
				cfg.Whisper.StoreMirrorFiles = false
				cfg.Whisper.Quotas = persister.WhisperQuotas{{Pattern: "service.*", Namespaces: 1, Metrics: 2, LogicalSize: 3, PhysicalSize: physical, DataPoints: 4, Throughput: math.MaxInt64, DroppingPolicy: "new", StatMetricPrefix: ".exact"}, {Pattern: "/", Throughput: 100, DroppingPolicy: "none"}}
				want := cfg.getCarbonserverQuotas(frequency)
				got, err := carbonserver.PartsQuotaRules(cfg.Whisper.Quotas, frequency, ignore)
				if err != nil || !reflect.DeepEqual(got, want) {
					t.Fatal("quota configuration differs", frequency, ignore, physical, got, want, err)
				}
			}
		}
	}
}
func (s *partsExpirationOracle) InitializeActivity(context.Context, string, store.Metadata) error {
	s.initialized = true
	return nil
}
func (s *partsExpirationOracle) DeleteIfUnchanged(context.Context, string, store.Metadata) error {
	s.deleted = true
	return nil
}
func (*partsExpirationOracle) AsyncFlush() (<-chan struct{}, error) { return nil, nil }

func comparePartsExpiration(t *testing.T, rules persister.WhisperExpirationRules, names []string, fallback time.Duration) {
	t.Helper()
	until := time.Date(2027, 3, 31, 0, 0, 0, 0, time.UTC)
	for _, now := range []time.Time{until.Add(-time.Second), until, until.Add(time.Second)} {
		for _, name := range names {
			ttl := fallback
			if i := rules.MatchIndex(name, now); i >= 0 {
				ttl = rules[i].TTL()
			}
			for _, age := range []time.Duration{-time.Second, 0, 59 * time.Minute, time.Hour, 2 * time.Hour, 24 * time.Hour, 48 * time.Hour, 96 * time.Hour, 120 * time.Hour, 192 * time.Hour, 31 * 24 * time.Hour, 365 * 24 * time.Hour, ttl - time.Nanosecond, ttl, ttl + time.Nanosecond} {
				for _, unknown := range []bool{false, true} {
					for _, pending := range []bool{false, true} {
						for _, dry := range []bool{false, true} {
							oracle := &partsExpirationOracle{}
							r := &metricExpirer{db: oracle, rules: rules, expiration: fallback, now: func() time.Time { return now }, pending: func(string) bool { return pending }, stats: &expirationStats{}, dryRun: dry}
							m := store.Metadata{MetricConfig: store.MetricConfig{Name: name, Retentions: []store.Retention{{Step: 1, Count: 60}}}, Generation: 1, Revision: 1, LastUpdate: now.Add(-age)}
							if unknown {
								m.LastUpdate = time.Time{}
							}
							_, err := r.expire(context.Background(), m)
							if err != nil {
								t.Fatal(err)
							}
							scope := partsops.Scope{PlacementGroup: "sample", VShard: partsformat.Route(partsformat.ID(name))}
							plan := partsops.PlanExpiration(partsops.Snapshot{Scope: scope, Metrics: []partsops.Metric{{Scope: scope, Metadata: m}}}, partsops.ExpirationPolicy{Rules: rules, Fallback: fallback, DryRun: dry}, now, func(string) bool { return pending })
							if (len(plan.Delete) > 0) != oracle.deleted || (len(plan.Initialize) > 0) != oracle.initialized {
								t.Fatalf("name=%s age=%s unknown=%v pending=%v dry=%v now=%s: plan=%+v oracle=%+v", name, age, unknown, pending, dry, now, plan, oracle)
							}
						}
					}
				}
			}
		}
	}
}

func TestPartsExpirationDifferential(t *testing.T) {
	rules := expirationTestRules(t, `[pod]
pattern = ^aggregations\.secondly\..+\.by_pod\.
expiration = 24h
grace = 24h
[temporary]
pattern = ^aggregations\..*\.search\.
exclude = \.by_pod\.
expiration = 0s
until = 2027-03-31T00:00:00Z
[folded_one]
pattern = ^(?:origin_one\.|carbon\.probe\.origin_one\.)
expiration = 192h
[folded_two]
pattern = ^origin_two\.
expiration = 48h
[topk]
pattern = ^general\.topk\.
expiration = 59m
grace = 1m
[disabled]
pattern = ^keep\.
expiration = 0s
grace = 24h
`)
	for _, fallback := range []time.Duration{0, 192 * time.Hour} {
		comparePartsExpiration(t, rules, []string{"aggregations.secondly.service.by_pod.search.cpu", "aggregations.minutely.service.search.latency", "origin_one.host.cpu", "origin_two.host.cpu", "carbon.probe.origin_one.latency", "general.topk.user", "keep.forever", "unmatched.metric"}, fallback)
	}
}

// Optional private fixture lets an operator compare locally rendered policy
// files without putting deployment names or configuration into the repository.
func TestPartsExpirationPolicyFixture(t *testing.T) {
	path := os.Getenv("PARTSOPS_EXPIRATION_FIXTURE")
	if path == "" {
		t.Skip("optional locally rendered policy fixture")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var fixtures []struct {
		File         string
		Names        []string
		DefaultHours int64
	}
	if err := json.Unmarshal(data, &fixtures); err != nil {
		t.Fatal(err)
	}
	for _, f := range fixtures {
		rules, err := persister.ReadWhisperExpiration(f.File)
		if err != nil {
			t.Fatal(err)
		}
		comparePartsExpiration(t, rules, f.Names, time.Duration(f.DefaultHours)*time.Hour)
	}
}
