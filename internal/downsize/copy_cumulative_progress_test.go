package downsize

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestCopyResumeProgressIsCumulative(t *testing.T) {
	t.Parallel()
	names := copyNames("metric.", 100)
	file := filepath.Join(t.TempDir(), "names")
	if err := os.WriteFile(file, []byte(strings.Join(names, "\n")+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	o := copyTestOptions(t, "http://source", "http://target")
	o.names = file
	id, err := copyIdentity(o)
	if err != nil {
		t.Fatal(err)
	}
	j, err := openCopyJournal(o.state, id)
	if err != nil {
		t.Fatal(err)
	}
	if err := j.done("", names[0], names[74], 75, 0, time.Second); err != nil {
		t.Fatal(err)
	}
	j.close()
	// Reopen the exact durable journal, as a real --resume does.
	j, err = openCopyJournal(o.state, id)
	if err != nil {
		t.Fatal(err)
	}
	defer j.close()
	progress := newCopyProgress()
	if err := atomicStateJSON(o.state+".progress.json", progressSnapshot{Done: 75, Total: 100}); err != nil {
		t.Fatal(err)
	}
	progress.resume(o.state, j.completedNames)
	c := &planFreeCopier{o: o, journal: j, progress: progress}
	p := c.progressSnapshot()
	if p.Done != 75 || p.Total != 100 || p.FreshDone != 0 {
		t.Fatalf("resume lost durable progress: %+v", p)
	}
	// Workers may finish before the catalog counter. Those names must not be
	// counted twice in the denominator or presented as replay throughput.
	if err := j.done("", names[75], names[79], 5, 0, time.Second); err != nil {
		t.Fatal(err)
	}
	c.stats.copied.Store(5)
	c.countTotal(context.Background(), nil)
	p = c.progressSnapshot()
	if p.Done != 80 || p.Total != 100 || p.FreshDone != 5 {
		t.Fatalf("new work or total counted twice: %+v", p)
	}
	c.stats.copied.Store(25)
	if err := j.done("", names[80], names[99], 20, 0, time.Second); err != nil {
		t.Fatal(err)
	}
	p = c.progressSnapshot()
	if p.Done != 100 || p.Total != 100 || p.FreshDone != 25 {
		t.Fatalf("completion: %+v", p)
	}
}
