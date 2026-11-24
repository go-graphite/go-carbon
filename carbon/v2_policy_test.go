package carbon

import (
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/go-graphite/go-carbon/carbonserver"
	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2core"
	"github.com/go-graphite/go-carbon/internal/v2part"
	"github.com/go-graphite/go-carbon/internal/v2read"
	"github.com/go-graphite/go-carbon/persister"
	"github.com/go-graphite/go-whisper"
)

// Keep the old resolver independent: raw regexp matching in original rule order,
// unconditional classic validation outside shadow, then canonical JSON encoding.
func originalV2Policy(app *App, mode, name string) ([]byte, error) {
	c := app.Config
	var schema persister.Schema
	found := false
	for _, rule := range c.Whisper.Schemas {
		if rule.Pattern.MatchString(name) {
			schema, found = rule, true
			break
		}
	}
	if !found {
		return nil, fmt.Errorf("no v2 schema for %q", name)
	}
	agg := c.Whisper.Aggregation.Default
	for _, rule := range c.Whisper.Aggregation.Data {
		if rule.Pattern().MatchString(name) {
			agg = rule
			break
		}
	}
	cfg := sealrollup.Config{Method: sealrollup.Method(agg.AggregationMethod()), XFilesFactor: float32(agg.XFilesFactor())}
	policy := v2read.Metric{AggregationMethod: agg.AggregationMethod().String(), XFilesFactor: cfg.XFilesFactor}
	for _, a := range schema.Retentions {
		cfg.Archives = append(cfg.Archives, sealrollup.Archive{Step: int64(a.SecondsPerPoint()), Count: int64(a.NumberOfPoints())})
		policy.Archives = append(policy.Archives, v2read.Archive{Step: int64(a.SecondsPerPoint()), Points: int64(a.NumberOfPoints())})
	}
	if mode != v2Shadow {
		if err := app.Carbonserver.ValidateV2UnionPolicy(name, policy); err != nil {
			return nil, err
		}
	}
	return v2core.EncodeRollupPolicy(v2core.RollupPolicy{Name: v2part.ID(name), Group: "local", Config: cfg})
}

func TestV2AdmissionPolicyDifferential(t *testing.T) {
	app := v2TestApp(t, t.TempDir())
	var err error
	app.Config.Whisper.Schemas, err = persister.ReadWhisperSchemas("testdata/admission/storage-schemas.conf")
	if err != nil {
		t.Fatal(err)
	}
	app.Config.Whisper.Aggregation, err = persister.ReadWhisperAggregation("testdata/admission/storage-aggregation.conf")
	if err != nil {
		t.Fatal(err)
	}
	app.Carbonserver = carbonserver.NewCarbonserverListener(nil)
	app.Carbonserver.SetWhisperData(app.Config.Whisper.DataDir)
	// Matching single archive, mismatched policy, and unseeded multiarchive.
	legacy := []string{"legacy.hourly.min", "legacy.hourly.max", "legacy.secondly.min"}
	for i, name := range legacy {
		schema, _ := app.Config.Whisper.Schemas.Match(name)
		agg := app.Config.Whisper.Aggregation.Match(name)
		method := agg.AggregationMethod()
		if i == 1 {
			method = whisper.Average
		}
		path := filepath.Join(app.Config.Whisper.DataDir, strings.ReplaceAll(name, ".", "/")+".wsp")
		if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
			t.Fatal(err)
		}
		w, err := whisper.Create(path, schema.Retentions, method, float32(agg.XFilesFactor()))
		if err != nil {
			t.Fatal(err)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
	}
	classic, err := app.openV2Classic()
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(classic, []string{"legacy.hourly.max", "legacy.hourly.min", "legacy.secondly.min"}) {
		t.Fatal(classic)
	}
	names := slices.Clone(legacy)
	names = append(names, "aggregations.secondly.bplatform.foo.by_persona_cluster_pod.x.min", "aggregations.secondly.foo.by_persona_cluster_pod.x.sum", "aggregations.hourly.capacityservice.bplatform.secondly.x.pods.per_persona.mean", "plain.default", "quoted.\"name\\.sum")
	rng := rand.New(rand.NewSource(1739))
	for range 10000 {
		prefix := []string{"general", "aggregations.secondly", "aggregations.minutely", "general.ten_secondly", "other.hourly", "aggregations.secondly.bplatform"}[rng.Intn(6)]
		suffix := []string{"min", "max", "avg", "average", "mean", "last", "tick", "stddev", "count", "sum", "unknown"}[rng.Intn(11)]
		names = append(names, fmt.Sprintf("%s.host%d.component%d.%s", prefix, rng.Intn(1000), rng.Intn(1000), suffix))
	}
	schemas := app.Config.Whisper.Schemas
	aggregation := app.Config.Whisper.Aggregation
	for _, scenario := range []string{"fixtures", "schema-refusal", "aggregation-default", "invalid-policy"} {
		t.Run(scenario, func(t *testing.T) {
			app.Config.Whisper.Schemas = schemas
			app.Config.Whisper.Aggregation = aggregation
			switch scenario {
			case "schema-refusal":
				app.Config.Whisper.Schemas = schemas[:1]
			case "aggregation-default":
				app.Config.Whisper.Aggregation = persister.NewWhisperAggregation()
			case "invalid-policy":
				bad := slices.Clone(schemas)
				for i := range bad {
					bad[i].Retentions = nil
				}
				app.Config.Whisper.Schemas = bad
			}
			for _, mode := range []string{v2Only, v2ServingDual, v2Shadow} {
				resolve := app.v2PolicyResolver(mode, classic)
				refusals := 0
				for _, name := range names {
					got, gotErr := resolve(name)
					want, wantErr := originalV2Policy(app, mode, name)
					if !bytes.Equal(got, want) || fmt.Sprint(gotErr) != fmt.Sprint(wantErr) {
						t.Fatalf("%s %q: got %s / %v; want %s / %v", mode, name, got, gotErr, want, wantErr)
					}
					if gotErr != nil {
						refusals++
					}
				}
				if scenario == "fixtures" && mode != v2Shadow && refusals != 2 {
					t.Fatalf("%s refusals=%d, want mismatch + missing baseline", mode, refusals)
				}
			}
		})
	}
}

// An entry created by a live classic writer after the startup inventory must
// still participate in dual-write admission. Direct v2 history is immutable.
func TestV2AdmissionLiveClassicValidation(t *testing.T) {
	app := v2TestApp(t, t.TempDir())
	app.Carbonserver = carbonserver.NewCarbonserverListener(nil)
	app.Carbonserver.SetWhisperData(app.Config.Whisper.DataDir)
	classic, err := app.openV2Classic()
	if err != nil {
		t.Fatal(err)
	}
	resolveDual := app.v2PolicyResolver(v2ServingDual, classic)
	resolveOnly := app.v2PolicyResolver(v2Only, classic)
	path := filepath.Join(app.Config.Whisper.DataDir, "late.wsp")
	if err := os.WriteFile(path, []byte("invalid whisper"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := resolveDual("late"); err == nil {
		t.Fatal("live classic error bypassed")
	}
	if _, err := resolveOnly("late"); err != nil {
		t.Fatalf("immutable inventory miss probed classic: %v", err)
	}
}
