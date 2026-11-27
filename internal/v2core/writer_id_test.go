package v2core

import (
	"encoding/json"
	"testing"
)

// writerID is the ID a test writer submits for policy's name: its packed
// dictionary ID when the store has a dictionary (core refuses NumericIDs
// there), else numeric.
func writerID(t testing.TB, c *PolicyCatalog, numeric uint64, policy []byte) uint64 {
	t.Helper()
	if c.partDict() == nil {
		return numeric
	}
	var p RollupPolicy
	if err := json.Unmarshal(policy, &p); err != nil {
		t.Fatal(err)
	}
	h, err := c.admit(string(p.Name), policy)
	if err != nil {
		t.Fatal(err)
	}
	return h.id
}
