package persister

import (
	"fmt"
	"regexp"
	"time"
)

// WhisperExpirationRule is one ordered metric expiration override.
type WhisperExpirationRule struct {
	Name       string
	Pattern    *regexp.Regexp
	Expiration time.Duration
}

// WhisperExpirationRules contains metric expiration overrides in file order.
// The first matching rule wins.
type WhisperExpirationRules []WhisperExpirationRule

// ReadWhisperExpiration reads ordered metric expiration overrides from an INI
// file. Each section requires pattern and expiration fields.
func ReadWhisperExpiration(filename string) (WhisperExpirationRules, error) {
	config, err := parseIniFile(filename)
	if err != nil {
		return nil, err
	}

	rules := make(WhisperExpirationRules, 0, len(config))
	for _, section := range config {
		for key := range section {
			switch key {
			case "name", "pattern", "expiration":
			default:
				return nil, fmt.Errorf("[persister] unknown expiration setting %q for [%s]", key, section["name"])
			}
		}

		patternText := section["pattern"]
		if patternText == "" {
			return nil, fmt.Errorf("[persister] missing pattern for [%s]", section["name"])
		}
		if section["expiration"] == "" {
			return nil, fmt.Errorf("[persister] missing expiration for [%s]", section["name"])
		}

		pattern, err := regexp.Compile(patternText)
		if err != nil {
			return nil, fmt.Errorf("[persister] compile expiration pattern for [%s]: %w", section["name"], err)
		}
		expiration, err := time.ParseDuration(section["expiration"])
		if err != nil {
			return nil, fmt.Errorf("[persister] parse expiration for [%s]: %w", section["name"], err)
		}
		if expiration < 0 {
			return nil, fmt.Errorf("[persister] expiration for [%s] must not be negative", section["name"])
		}

		rules = append(rules, WhisperExpirationRule{
			Name:       section["name"],
			Pattern:    pattern,
			Expiration: expiration,
		})
	}

	return rules, nil
}

// Match returns the expiration of the first matching rule, or fallback when
// none matches. An expiration of zero explicitly exempts a metric.
func (r WhisperExpirationRules) Match(metric string, fallback time.Duration) time.Duration {
	for _, rule := range r {
		if rule.Pattern.MatchString(metric) {
			return rule.Expiration
		}
	}
	return fallback
}

// Enabled reports whether at least one override enables expiration.
func (r WhisperExpirationRules) Enabled() bool {
	for _, rule := range r {
		if rule.Expiration > 0 {
			return true
		}
	}
	return false
}
