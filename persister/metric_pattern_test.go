package persister

import (
	"fmt"
	"math/rand"
	"regexp"
	"strings"
	"testing"
)

func TestMetricPatternMatchesRegexp(t *testing.T) {
	patterns := []string{`.*`, ``, `^`, `$`, `^$`, `foo`, `^foo`, `foo$`, `^foo$`, `.*\.sum$`, `^general\..*\.count$`, `^(general\.)foo$`, `(?i)^general.*COUNT$`, `(?m)^foo$`, `^foo(bar)?$`, `^(foo|bar)$`, `foo\b`, `^a.*(b|c)$`, `(?s)^foo.*bar$`, `^日本.*値$`, `^�.*�$`}
	names := []string{"", "foo", "foo\n", "\nfoo", "general.foo", "general.foo.count", "GENERAL.foo.COUNT", "general.foo.sum", "日本.値", "�.�", "\xff.\xff"}
	rng := rand.New(rand.NewSource(11))
	alphabet := []byte("general.foo_countSUM\n\x00\xff")
	for range 1000 {
		name := make([]byte, rng.Intn(100))
		for i := range name {
			name[i] = alphabet[rng.Intn(len(alphabet))]
		}
		names = append(names, string(name))
	}
	for _, expression := range patterns {
		re := regexp.MustCompile(expression)
		matcher := compileMetricPattern(re)
		for _, name := range names {
			if got, want := matcher.match(re, name), re.MatchString(name); got != want {
				t.Fatalf("pattern %q metric %q: %v != %v", expression, name, got, want)
			}
		}
		replacement := regexp.MustCompile(`replaced`)
		if !matcher.match(replacement, "replaced") {
			t.Fatal("stale compiled pattern")
		}
	}
}

func policyFixture(tb testing.TB) (WhisperSchemas, *WhisperAggregation, []string) {
	tb.Helper()
	schemas, err := ReadWhisperSchemas("../deploy/storage-schemas.conf")
	if err != nil {
		tb.Fatal(err)
	}
	aggregation, err := ReadWhisperAggregation("../deploy/storage-aggregation.conf")
	if err != nil {
		tb.Fatal(err)
	}
	// Ordered production-shaped policies; keep the deploy defaults as fallbacks.
	for i, prefix := range []string{"secondly", "ten_secondly", "minutely", "five_minutely", "hourly", "kafka"} {
		re := regexp.MustCompile(`^aggregations\.` + prefix + `\.`)
		schemas = append(schemas[:len(schemas)-1], Schema{Name: prefix, Pattern: re, matcher: compileMetricPattern(re), Priority: int64(i)}, schemas[len(schemas)-1])
	}
	for _, suffix := range []string{"sum", "min", "max", "last", "count"} {
		re := regexp.MustCompile(`.*\.` + suffix + `$`)
		aggregation.Data = append(aggregation.Data[:len(aggregation.Data)-1], &WhisperAggregationItem{name: suffix, pattern: re, matcher: compileMetricPattern(re)}, aggregation.Data[len(aggregation.Data)-1])
	}
	names := make([]string, 0, 1024)
	for i := range 1024 {
		namespace := []string{"general.flink", "aggregations.secondly", "aggregations.minutely", "general.mysql", "aggregations.kafka"}[i%5]
		suffix := []string{"rate", "count", "sum", "min", "max", "last", "mean"}[i%7]
		names = append(names, fmt.Sprintf("%s.prod.host%04d.%s.query.%s", namespace, i, strings.Repeat("component.", i%12), suffix))
	}
	return schemas, aggregation, names
}

func originalPolicyMatch(schemas WhisperSchemas, aggregation *WhisperAggregation, metric string) (string, string) {
	var schema string
	for _, s := range schemas {
		if s.Pattern.MatchString(metric) {
			schema = s.Name
			break
		}
	}
	for _, a := range aggregation.Data {
		if a.pattern.MatchString(metric) {
			return schema, a.name
		}
	}
	return schema, aggregation.Default.name
}

func TestMetricPolicyFirstMatchParity(t *testing.T) {
	schemas, aggregation, names := policyFixture(t)
	for _, name := range names {
		wantSchema, wantAggregation := originalPolicyMatch(schemas, aggregation, name)
		gotSchema, ok := schemas.Match(name)
		if !ok || gotSchema.Name != wantSchema || aggregation.Match(name).name != wantAggregation {
			t.Fatalf("policy changed for %q", name)
		}
	}
}

func BenchmarkMetricPolicy(b *testing.B) {
	schemas, aggregation, names := policyFixture(b)
	for _, fast := range []bool{false, true} {
		b.Run(fmt.Sprint(fast), func(b *testing.B) {
			b.ReportAllocs()
			for i := range b.N {
				name := names[i%len(names)]
				if fast {
					schemas.Match(name)
					aggregation.Match(name)
				} else {
					originalPolicyMatch(schemas, aggregation, name)
				}
			}
		})
	}
}
