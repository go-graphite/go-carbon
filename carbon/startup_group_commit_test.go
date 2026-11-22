package carbon

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/go-graphite/go-carbon/points"
)

func TestStartupJournalStagedBeforeCacheBitExact(t *testing.T) {
	app := journalStartupFixture(t, t.TempDir())
	defer closeJournalFixture(app)
	p := releaseCrashPoints()
	for i, point := range p.Data {
		name := p.Metric + ";z=last;a=first"
		if i%2 == 0 {
			name = p.Metric + ";a=first;z=last"
		}
		one := &points.Points{Metric: name, Data: []points.Point{point}}
		raw := one.AppendBinary(nil)
		release, err := app.startupInput.AcquireCapture(len(raw), 1)
		if err != nil {
			t.Fatal(err)
		}
		err = app.startupInput.StageCapture(raw, 1)
		release()
		clear(raw)
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := app.inputLog.Load().Sync(); err != nil {
		t.Fatal(err)
	}
	if app.Cache != nil || app.inputLog.Load().Metrics().PointsWritten != int64(len(p.Data)) {
		t.Fatal("not durable before cache")
	}
	paths, _ := filepath.Glob(filepath.Join(app.Config.Dump.Path, "*.capture-*"))
	if len(paths) != 0 {
		t.Fatal("capture spool created", paths)
	}
	attachJournalFixture(t, app)
	name := p.Metric + ";a=first;z=last"
	awaitCapture(t, func() bool { return len(app.Cache.Get(name)) == len(p.Data) })
	assertReleaseReplayBits(t, app.Cache.Get(name), p.Data)
	before := app.inputLog.Load().Metrics().PointsWritten
	if err := app.inputLog.Load().Sync(); err != nil {
		t.Fatal(err)
	}
	if app.inputLog.Load().Metrics().PointsWritten != before {
		t.Fatal("startup input relogged")
	}
	awaitCapture(t, func() bool { return app.startupInput.journal.fence() == nil })
	if err := app.startupInput.receiver.SwitchCapture(app.Cache.Add, app.startupInput.journal.fence); err != nil {
		t.Fatal(err)
	}
	app.startupInput.direct.Store(true)
	awaitCapture(t, func() bool { return app.startupInput.journal.joined.Load() })
	// Source ownership must outlive application and the reader join.
	if app.startupInput.journal.parts[(hashJournalName(name))].source.Pending() == 0 {
		t.Fatal("source released before confirmation")
	}
	app.startupInput.journal.releasePersisted()
	if app.startupInput.journal.released {
		t.Fatal("retired unpersisted startup source")
	}
}

func TestStartupGroupCommitArchivesMatchSynchronous(t *testing.T) {
	if isolatedTestProcess(t) {
		return
	}
	var reference [][]points.Point
	for _, stage := range []bool{false, true} {
		root := t.TempDir()
		app := journalStartupFixture(t, root)
		p := releaseCrashPoints()
		for i := 0; i < len(p.Data); i += 4 {
			batch := points.Points{Metric: p.Metric, Data: p.Data[i:min(i+4, len(p.Data))]}
			raw := batch.AppendBinary(nil)
			var err error
			if stage {
				err = app.startupInput.journal.writeAdmitted(raw, len(batch.Data), true)
			} else {
				err = app.startupInput.Write(raw, len(batch.Data))
			}
			if err != nil {
				t.Fatal(err)
			}
			clear(raw)
		}
		if err := app.inputLog.Load().Sync(); err != nil {
			t.Fatal(err)
		}
		attachJournalFixture(t, app)
		awaitCapture(t, func() bool { return len(app.Cache.Get(p.Metric)) == len(p.Data) })
		db := releaseCrashStore(t, filepath.Join(root, "store"))
		batch, ok := app.Cache.PopNotConfirmed(p.Metric)
		if !ok {
			t.Fatal("missing captured points")
		}
		assertReleaseReplayBits(t, batch.Data, p.Data)
		if err := db.UpdateMany(context.Background(), p.Metric, batch.Data); err != nil {
			t.Fatal(err)
		}
		if err := db.Sync(); err != nil {
			t.Fatal(err)
		}
		snapshot, err := db.Snapshot(context.Background(), p.Metric)
		if err != nil {
			t.Fatal(err)
		}
		if !stage {
			for _, a := range snapshot.Archives {
				reference = append(reference, a.Points)
			}
		} else {
			if len(snapshot.Archives) != len(reference) {
				t.Fatal("archive count")
			}
			for i, a := range snapshot.Archives {
				assertReleaseReplayBits(t, a.Points, reference[i])
			}
		}
		db.Close()
		closeJournalFixture(app)
	}
}
