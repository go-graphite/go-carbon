package sealrollup

import (
	"testing"
	"time"
)

func TestHeadPolicyBudgetAndAge(t *testing.T) {
	for _, owners := range []int{8, 16, 32, 48} {
		p, err := NewHeadPolicy(20_000_000, 64<<30, 64, owners)
		if err != nil {
			t.Fatal(err)
		}
		if p.MaxPoints*uint64(owners)*64 > 64<<30 || p.MaxAge > 16*time.Minute {
			t.Fatal("policy exceeds budget", p)
		}
		// Effective age is ~53.69s, independent of execution geometry.
		if p.MaxAge < 53*time.Second || p.MaxAge > 54*time.Second {
			t.Fatal("unexpected intake-derived age", p)
		}
		at := time.Unix(1000, 0)
		if p.Due(at, at.Add(time.Hour), 0) || p.Due(at, at.Add(p.MaxAge-1), 1) ||
			!p.Due(at, at.Add(p.MaxAge), 1) || !p.Due(at, at, p.MaxPoints) {
			t.Fatal("age/size/empty behavior", p)
		}
	}
	p, err := NewHeadPolicy(100, 1<<30, 32, 8)
	if err != nil || p.MaxAge != 16*time.Minute || p.MaxPoints != 12000 {
		t.Fatal("age ceiling", p, err)
	}
}

func TestHeadPolicyRejectsUnusableBudget(t *testing.T) {
	for _, args := range [][4]uint64{{0, 100, 32, 1}, {1, 100, 0, 1}, {1, 100, 32, 0}, {1, 31, 32, 1}, {1, 32, 32, 2}, {^uint64(0), 32, 32, 1}} {
		if _, err := NewHeadPolicy(args[0], args[1], args[2], int(args[3])); err == nil {
			t.Fatal("accepted unusable policy", args)
		}
	}
}
