package v2core

import (
	"fmt"
	"slices"
	"testing"

	partsformat "github.com/go-graphite/go-carbon/internal/v2part"
)

func TestRollupStageSortMatchesMetricOrder(t *testing.T) {
	var stages []rollupStage
	for i := range 1000 {
		group := []string{"z", "ab", "aa", "a"}[i%4]
		name := []string{"a", "z", "é", "prefix.identical.first", "prefix.identical.last", "prefix.identical.firsu"}[i%6] + fmt.Sprintf(".%d", i)
		m := &rollupMetric{PolicyIdentity: &PolicyIdentity{policy: policyName{Name: partsformat.ID(name), policyDefinition: &policyDefinition{}}}, vshard: uint16(i % 7)}
		m.policy.Group = partsformat.Group(group)
		stages = append(stages, rollupStage{metric: m})
	}
	prepareRollupStageOrder(stages)
	got, want := make([]*rollupStage, len(stages)), make([]*rollupStage, len(stages))
	for i := range stages {
		got[i], want[i] = &stages[i], &stages[i]
	}
	slices.SortFunc(got, compareRollupStages)
	slices.SortFunc(want, func(a, b *rollupStage) int { return compareRollupMetrics(a.metric, b.metric) })
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("canonical order differs at %d", i)
		}
	}
}
