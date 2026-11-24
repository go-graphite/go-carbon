package persister

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"
)

// Keep the original implementation as the oracle for rule order, deadlines and
// exclusions. A zero expiration still selects the rule (it exempts the name).
func originalExpirationMatchIndex(r WhisperExpirationRules, metric string, now time.Time) int {
	for i, rule := range r {
		if !rule.Until.IsZero() && !now.Before(rule.Until) {
			continue
		}
		if rule.Pattern.MatchString(metric) && (rule.Exclude == nil || !rule.Exclude.MatchString(metric)) {
			return i
		}
	}
	return -1
}

func expirationMatchFixture(tb testing.TB, patterns []string) WhisperExpirationRules {
	tb.Helper()
	var config strings.Builder
	for i, pattern := range patterns {
		fmt.Fprintf(&config, "[rule%d]\npattern = %s\nexpiration = %dh\n", i, pattern, i)
		if i%2 == 0 {
			config.WriteString("exclude = (?:^skip\\.|\\.keep$)\n")
		}
		if i%3 == 0 {
			config.WriteString("until = 2030-01-01T00:00:00Z\n")
		}
	}
	path := filepath.Join(tb.TempDir(), "expiration.conf")
	if err := os.WriteFile(path, []byte(config.String()), 0600); err != nil {
		tb.Fatal(err)
	}
	rules, err := ReadWhisperExpiration(path)
	if err != nil {
		tb.Fatal(err)
	}
	return rules
}

var expirationPatternSets = [][]string{
	{`^telemetry\..*\.preview\.worker\..*$`, `^telemetry\.(?:.*\.)?sandbox\.jobs\.latency\..*$`, `^services\.batch\.`, `^(?:gauges\.|counters\.service\.)`},
	{`^services\.queue\.`, `^services\.cache\.`, `^services\.database\.`, `^`},
	{`jobs`, `^jobs\.`, `\.count$`, `^jobs\.important$`, `.*`},
	{`(?:)`, `^$`, `(?m)^jobs$`, `(?i)^JOBS\..*COUNT$`},
	{`^日本\.`, `^�.*�$`, `^a(bc)?d`, `^(one|two)\.`, `^a.*(b|c)$`, `\bword\b`},
	{`^(?:telemetry\.)?optional`, `^foo(?:bar|baz)\.`, `^foo(?:bar)?\.`, `^(foo\.){0,2}bar`, `(?s)^start.*end$`, `\A(?:a\.b\.)(x|y)`, `^a\b.*b$`},
	{`^(ab)+cd`, `^(ab){2,4}cd`, `^(ab){0,4}cd`, `(?i:jobs)\.count`, `(?i)^ſ.*k$`, `^日本.*値$`, `^�x$`},
}

func expirationMatchNames(n int) []string {
	rng := rand.New(rand.NewSource(731))
	roots := []string{"telemetry", "services.batch", "services.queue", "jobs", "gauges", "counters.service", "other", "日本"}
	names := make([]string, 0, n+100)
	for i := range n {
		size := 60 + rng.Intn(241)
		name := fmt.Sprintf("%s.region%02d.node%04d.", roots[i%len(roots)], rng.Intn(8), rng.Intn(10000))
		for len(name) < size {
			name += "component."
		}
		name = name[:size]
		switch i % 10 {
		case 0:
			name += ".preview.worker.count"
		case 1:
			name += ".sandbox.jobs.latency.p99"
		case 2:
			name += ".keep"
		}
		names = append(names, name)
	}
	names = append(names, "", "jobs", "jobs.", "jobs.important", "jobs.important\n", "jobs.important.keep", "skip.jobs", "skip.jobs.keep", "JOBS.test.COUNT", "\njobs\n", "a.b.x", "a.b.y", "a.b.", "foobar.", "foobaz.", "foo.", "optional", "telemetry.optional", "bar", "foo.bar", "foo.foo.bar", "ad", "abcd", "one.", "two.", "a b", "word", "sword", "telemetry.x.preview.worker.count", "telemetry.sandbox.jobs.latency.count", "services.queue.count", "services.cache.count", "services.database.count", "ababcd", "cd", "jobs.count", "ſ.k", "S.k", "日本.長い値", "�x", "\xffx", "日本.値", "�.�", "\xff.\xfe", "start\nend", "\x00", strings.Repeat("telemetry.component.", 2048))
	alphabet := []byte("abc.jobs_012\n\x00\xff")
	for range n / 10 {
		name := make([]byte, rng.Intn(400))
		for i := range name {
			name[i] = alphabet[rng.Intn(len(alphabet))]
		}
		names = append(names, string(name))
	}
	sort.Strings(names)
	return names
}

func TestExpirationMatchIndexDifferential(t *testing.T) {
	names := expirationMatchNames(20000)
	comparisons := 0
	deadline := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
	for set, patterns := range expirationPatternSets {
		t.Run(fmt.Sprint(set), func(t *testing.T) {
			rules := expirationMatchFixture(t, patterns)
			for start := range rules {
				// Exercise later rules even when an earlier rule matches everything.
				current := rules[start:]
				for _, now := range []time.Time{deadline.Add(-time.Nanosecond), deadline, deadline.Add(time.Nanosecond)} {
					for _, name := range names {
						comparisons++
						if got, want := current.MatchIndex(name, now), originalExpirationMatchIndex(current, name, now); got != want {
							t.Fatalf("name %q at %v: got %d, want %d", name, now, got, want)
						}
					}
				}
			}
			// Public rules may also be constructed or have their Pattern replaced.
			rules[0].Pattern = regexp.MustCompile(`replaced`)
			for _, name := range []string{"replaced", "unmatched", "replaced.keep"} {
				comparisons++
				if got, want := rules.MatchIndex(name, deadline.Add(-time.Second)), originalExpirationMatchIndex(rules, name, deadline.Add(-time.Second)); got != want {
					t.Fatalf("replacement: got %d, want %d", got, want)
				}
			}
			// UnmarshalText recompiles a regexp without changing its pointer.
			mutated := expirationMatchFixture(t, patterns)
			if err := mutated[0].Pattern.UnmarshalText([]byte(`^mutated\.`)); err != nil {
				t.Fatal(err)
			}
			for _, name := range []string{"mutated.value", "mutated.value.keep", "unmatched"} {
				comparisons++
				if got, want := mutated.MatchIndex(name, deadline.Add(-time.Second)), originalExpirationMatchIndex(mutated, name, deadline.Add(-time.Second)); got != want {
					t.Fatalf("recompiled pattern: got %d, want %d", got, want)
				}
			}
		})
	}
	direct := WhisperExpirationRules{{Pattern: regexp.MustCompile(`^jobs\.`), Exclude: regexp.MustCompile(`\.keep$`)}}
	for _, name := range names {
		comparisons++
		if got, want := direct.MatchIndex(name, deadline), originalExpirationMatchIndex(direct, name, deadline); got != want {
			t.Fatalf("direct construction: got %d, want %d", got, want)
		}
	}
	t.Logf("%d name/time comparisons across %d synthetic rule sets and their suffixes", comparisons, len(expirationPatternSets))
}

var expirationMatchResult int

func BenchmarkWhisperExpirationMatchIndex(b *testing.B) {
	// Sorted names model the catalog's ListPage traversal. Each operation is one
	// name, rather than an entire page, so ns/op is directly ns/name.
	corpus := expirationMatchNames(4096)
	names := corpus[:0]
	for _, name := range corpus {
		if len(name) >= 60 && len(name) <= 300 && !strings.ContainsAny(name, "\x00\n\xff") {
			names = append(names, name)
		}
	}
	now := time.Date(2029, 1, 1, 0, 0, 0, 0, time.UTC)
	for set, patterns := range expirationPatternSets[:3] {
		rules := expirationMatchFixture(b, patterns)
		b.Run(fmt.Sprint(set), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			result := 0
			for i := range b.N {
				result += rules.MatchIndex(names[i%len(names)], now)
			}
			expirationMatchResult = result
		})
	}
}

func TestExpirationRequiredLiteral(t *testing.T) {
	tests := []struct{ pattern, literal string }{
		{`^a\.b\.(x|y)`, "a.b."},
		{`^a\..*\.preview\.worker\..*$`, ".preview.worker."},
		{`^(ab)+cd`, "ab"},
		{`^(ab){2,4}cd`, "ab"},
		{`^(ab){0,4}cd`, "cd"},
		{`(?:longer)?short`, "short"},
		{`(?i:jobs)\.count`, ".count"},
		{`(?i)^jobs.*COUNT$`, ""},
		{`^日本.*値$`, "日本"},
		{`^�.*�$`, ""},
		{`^(one|two)`, ""},
		{`^$`, ""},
		{``, ""},
	}
	for _, tt := range tests {
		if got := expirationRequiredLiteral(regexp.MustCompile(tt.pattern)); got != tt.literal {
			t.Errorf("%q: got %q, want %q", tt.pattern, got, tt.literal)
		}
	}
}

func TestExpirationLiteralReload(t *testing.T) {
	path := writeExpirationConfig(t, "[rule]\npattern = ^old\\..*required\nexpiration = 1h\n")
	before, err := ReadWhisperExpiration(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("[rule]\npattern = ^new\\..*replacement\nexpiration = 1h\n"), 0600); err != nil {
		t.Fatal(err)
	}
	after, err := ReadWhisperExpiration(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"old.x.required", "new.x.replacement", "old.x.replacement", "new.x.required"} {
		for _, rules := range []WhisperExpirationRules{before, after} {
			if got, want := rules.MatchIndex(name, time.Time{}), originalExpirationMatchIndex(rules, name, time.Time{}); got != want {
				t.Fatalf("reload for %q: got %d, want %d", name, got, want)
			}
		}
	}
}
