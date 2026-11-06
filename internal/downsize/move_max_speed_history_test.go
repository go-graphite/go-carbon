package downsize

import (
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestMaxSpeedDecisionHistorySurvivesCompactionAndResume(t *testing.T) {
	path := filepath.Join(t.TempDir(), "same-journal")
	j, err := openJournal(path, "same-approved-plan")
	if err != nil {
		t.Fatal(err)
	}
	original, amended := strings.Repeat("1", 64), strings.Repeat("2", 64)
	operation := j.operation
	for _, sum := range []string{original, amended, amended} {
		if err := j.appendRow(journalRow{Operation: operation, Plan: j.plan, MaxSpeedDecisionSHA256: sum}); err != nil {
			t.Fatal(err)
		}
	}
	if err := j.compact(); err != nil {
		t.Fatal(err)
	}
	j.close()
	j, err = openJournal(path, "same-approved-plan")
	if err != nil {
		t.Fatal(err)
	}
	defer j.close()
	if j.operation != operation || j.maxSpeedDecisionSHA != amended || !reflect.DeepEqual(j.maxSpeedDecisionHistory, []string{original, amended}) {
		t.Fatal("lost original authority or reset journal")
	}
}
