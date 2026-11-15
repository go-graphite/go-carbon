package carbon

import (
	"encoding/json"
	"time"

	"github.com/go-graphite/go-carbon/persister"
)

// Include ordered-rule outcome and all its conditions in the identity. A reload
// that changes an exclusion, grace or deadline invalidates old deletion proof.
func expirationPolicyIdentity(rules persister.WhisperExpirationRules, fallback time.Duration, dry bool, name string, now time.Time) (string, time.Duration) {
	if dry {
		return "", 0
	}
	idx := rules.MatchIndex(name, now)
	var value any
	ttl := fallback
	if idx < 0 {
		value = struct{ Default time.Duration }{fallback}
	} else {
		rule := rules[idx]
		ttl = rule.TTL()
		exclude := ""
		if rule.Exclude != nil {
			exclude = rule.Exclude.String()
		}
		value = struct {
			Rule, Pattern, Exclude string
			Expiration, Grace      time.Duration
			Until                  time.Time
		}{rule.Name, rule.Pattern.String(), exclude, rule.Expiration, rule.Grace, rule.Until}
	}
	if ttl <= 0 {
		return "", 0
	}
	data, _ := json.Marshal(value)
	return string(data), ttl
}

// Capture immutable reload configuration rather than taking App's lifecycle
// lock from a request: shutdown may hold that lock while joining handlers.
func (app *App) configureExpirationEvidence() {
	if app.Buckyd == nil {
		return
	}
	cfg := app.Config.Whisper
	core := app.Cache
	app.Buckyd.SetExpirationPolicy(func(name string) (string, time.Duration, bool) {
		policy, ttl := expirationPolicyIdentity(cfg.ExpirationRules, cfg.StoreExpiration.Value(), cfg.StoreExpirationDryRun, name, time.Now())
		return policy, ttl, core.Has(name)
	})
}
