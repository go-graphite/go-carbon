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
	Grace      time.Duration
	Exclude    *regexp.Regexp
	Until      time.Time
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
			case "name", "pattern", "expiration", "grace", "exclude", "until":
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

		var exclude *regexp.Regexp
		if text := section["exclude"]; text != "" {
			exclude, err = regexp.Compile(text)
			if err != nil {
				return nil, fmt.Errorf("compile expiration exclusion: %w", err)
			}
		}
		var grace time.Duration
		if text := section["grace"]; text != "" {
			grace, err = time.ParseDuration(text)
			if err != nil || grace < 0 || expiration > time.Duration(1<<63-1)-grace {
				return nil, fmt.Errorf("invalid expiration grace for [%s]", section["name"])
			}
		}
		var until time.Time
		if text := section["until"]; text != "" {
			until, err = time.Parse(time.RFC3339, text)
			if err != nil {
				return nil, fmt.Errorf("parse expiration until: %w", err)
			}
		}
		rules = append(rules, WhisperExpirationRule{
			Name:  section["name"],
			Grace: grace, Exclude: exclude, Until: until,
			Pattern:    pattern,
			Expiration: expiration,
		})
	}

	return rules, nil
}

// Match returns the expiration of the first matching rule, or fallback when
// none matches. An expiration of zero explicitly exempts a metric.
func (r WhisperExpirationRules) Match(metric string, fallback time.Duration) time.Duration {
	i := r.MatchIndex(metric, time.Now())
	if i < 0 {
		return fallback
	}
	return r[i].TTL()
}

func (r WhisperExpirationRule) TTL() time.Duration {
	if r.Expiration == 0 {
		return 0
	}
	return r.Expiration + r.Grace
}

// MatchIndex returns the first active rule, or -1 for the default policy.
func (r WhisperExpirationRules) MatchIndex(metric string, now time.Time) int {
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

// Enabled reports whether at least one override enables expiration.
func (r WhisperExpirationRules) Enabled() bool {
	for _, rule := range r {
		if rule.Expiration > 0 {
			return true
		}
	}
	return false
}
