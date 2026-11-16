package main

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/partlog"
)

func TestReplayPacingBudgetAndCancellation(t *testing.T) {
	now := time.Unix(123, 0)
	p := replayPacer{rate: 30000000, streams: 48}
	if got := p.deadline(now, 32768).Sub(now); got != 52428800*time.Nanosecond {
		t.Fatal(got)
	}
	// A stalled stream cannot issue a catch-up burst after it resumes.
	later := now.Add(time.Second)
	if got := p.deadline(later, 32768).Sub(later); got != 0 {
		t.Fatal(got)
	}
	if got := p.deadline(later, 32768).Sub(later); got != 52428800*time.Nanosecond {
		t.Fatal(got)
	}
	stop := make(chan struct{})
	close(stop)
	p = replayPacer{rate: 1, streams: 48}
	if err := p.wait(stop, 32768); err != context.Canceled {
		t.Fatal(err)
	}
	for _, raw := range []string{"0", "-1", "oops"} {
		t.Setenv("PARTLOG_BENCH_HISTORY_RATE", raw)
		if _, err := replayRateEnv("PARTLOG_BENCH_HISTORY_RATE", 0); err == nil {
			t.Fatal(raw)
		}
	}
}

func TestPacedHistoricalReplayExact(t *testing.T) {
	root := t.TempDir()
	cfg := loggedConfig()
	cfg.Producers = 2
	j, err := partlog.Open(filepath.Join(root, "log"), partlog.Options{Streams: 2})
	if err != nil {
		t.Fatal(err)
	}
	for p := range cfg.Producers {
		for n := 1; n <= 3; n++ {
			if _, err = j.Append(p, int64(n), encodeLogRows(coreRows(p, n))); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err = j.Close(); err != nil {
		t.Fatal(err)
	}
	l, err := openLoggedCore(filepath.Join(root, "log"), filepath.Join(root, "paced"), cfg, partlog.Options{})
	if err != nil {
		t.Fatal(err)
	}
	l.historyRate = 1000000
	if err = <-l.startReplay(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err = l.close(); err != nil {
		t.Fatal(err)
	}
	cfg.JournalIdentity = l.journal.Identity()
	want, err := coreDigest(filepath.Join(root, "paced"), cfg)
	if err != nil {
		t.Fatal(err)
	}
	oracle, err := openLoggedCore(filepath.Join(root, "log"), filepath.Join(root, "oracle"), cfg, partlog.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if err = oracle.replay(); err != nil {
		t.Fatal(err)
	}
	if err = oracle.close(); err != nil {
		t.Fatal(err)
	}
	got, err := coreDigest(filepath.Join(root, "oracle"), cfg)
	if err != nil || got != want {
		t.Fatalf("paced digest%s oracle%s err%v", want, got, err)
	}
}
