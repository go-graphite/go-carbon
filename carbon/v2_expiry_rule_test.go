package carbon

import (
	"fmt"
	"math/rand"
	"regexp"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/persister"
)

func randomExpiryRules(rng *rand.Rand, now time.Time) persister.WhisperExpirationRules {
	parts := []string{"a", "b", "c", "d"}
	rules := make(persister.WhisperExpirationRules, rng.Intn(6))
	for i := range rules {
		rule := persister.WhisperExpirationRule{Pattern: regexp.MustCompile(fmt.Sprintf(`^(%s|%s)\.`, parts[rng.Intn(4)], parts[rng.Intn(4)])), Expiration: time.Duration(rng.Intn(3)) * time.Hour}
		if rng.Intn(3) == 0 {
			rule.Exclude = regexp.MustCompile(fmt.Sprintf(`\.%s$`, parts[rng.Intn(4)]))
		}
		switch rng.Intn(3) {
		case 0: // lapsed
			rule.Until = now.Add(-time.Duration(rng.Intn(10)+1) * time.Minute)
		case 1: // still active
			rule.Until = now.Add(time.Duration(rng.Intn(10)) * time.Minute)
		}
		rules[i] = rule
	}
	return rules
}

// TestV2ExpiryRuleIndexDifferential: the per-name rule index answers exactly
// as MatchIndex over random rules, excludes, lapsed and active Until times.
func TestV2ExpiryRuleIndexDifferential(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	parts := []string{"a", "b", "c", "d"}
	now := time.Unix(1794700800, 0)
	for trial := 0; trial < 2000; trial++ {
		rules := randomExpiryRules(rng, now)
		for n := 0; n < 20; n++ {
			name := parts[rng.Intn(4)] + "." + parts[rng.Intn(4)]
			rule := v2ExpiryRule(rules, name)
			for _, at := range []time.Time{now.Add(-time.Hour), now, now.Add(5 * time.Minute), now.Add(time.Hour)} {
				if got, want := v2ExpiryRuleAt(rules, rule, name, at), rules.MatchIndex(name, at); got != want {
					t.Fatalf("trial %d %q at %v: index %d, MatchIndex %d (rule %d, rules %+v)", trial, name, at, got, want, rule, rules)
				}
			}
		}
	}
}

// BenchmarkV2ExpiryHookRule is the per-idle-candidate rule lookup: MatchIndex
// on a copied name (before) against the resolved rule index.
func BenchmarkV2ExpiryHookRule(b *testing.B) {
	var rules persister.WhisperExpirationRules
	for i := 0; i < 8; i++ {
		rules = append(rules, persister.WhisperExpirationRule{Pattern: regexp.MustCompile(fmt.Sprintf(`^carbon\.agents\.[^.]+\.rule%d\.`, i)), Expiration: time.Hour})
	}
	rules = append(rules, persister.WhisperExpirationRule{Pattern: regexp.MustCompile(`^servers\.`), Expiration: 24 * time.Hour})
	name := "servers.host-0042.cpu.total.user" // partsformat.ID is a string: the hook copies no bytes
	now := time.Unix(1794700800, 0)
	rule := v2ExpiryRule(rules, name)
	b.Run("matchindex", func(b *testing.B) {
		for range b.N {
			if rules.MatchIndex(name, now) < 0 {
				b.Fatal("miss")
			}
		}
	})
	b.Run("indexed", func(b *testing.B) {
		for range b.N {
			if v2ExpiryRuleAt(rules, rule, name, now) < 0 {
				b.Fatal("miss")
			}
		}
	})
}
