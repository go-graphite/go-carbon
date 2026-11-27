package carbon

import (
	"context"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

func TestExpirationRecordsExactPolicy(t *testing.T) {
	if isolatedTestProcess(t) {
		return
	}
	now := time.Unix(1790000000, 0)
	db := expirationTestDB(t, &now)
	m := createExpirationMetric(t, db, "jobs.old")
	rules := expirationTestRules(t, "[jobs]\npattern = ^jobs\\.\nexpiration = 72h\ngrace = 24h\n")
	r := &metricExpirer{db: db, rules: rules, now: func() time.Time { return now }}
	result, err := r.deleteBatch(context.Background(), []store.Metadata{m})
	if err != nil || result[0] != nil {
		t.Fatal(result, err)
	}
	receipt, err := db.ExpirationReceiptFor(context.Background(), m.Name)
	if err != nil {
		t.Fatal(err)
	}
	policy, ttl := expirationPolicyIdentity(rules, 0, false, m.Name, now)
	if ttl != 96*time.Hour || receipt.Policy != policy {
		t.Fatal(receipt, policy, ttl)
	}
	rules[0].Grace = time.Hour
	changed, _ := expirationPolicyIdentity(rules, 0, false, m.Name, now)
	if changed == policy {
		t.Fatal("grace did not invalidate policy")
	}
	disabled, ttl := expirationPolicyIdentity(rules, 0, true, m.Name, now)
	if disabled != "" || ttl != 0 {
		t.Fatal("dry-run authorized absence")
	}
}
