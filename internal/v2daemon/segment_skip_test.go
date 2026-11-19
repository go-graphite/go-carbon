package v2daemon

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/partsformat"
	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2core"
	"github.com/go-graphite/go-carbon/points"
)

func segmentSkipConfig() Config {
	c := parallelReplayConfig()
	// Rotate after every group fsync, so every Drain starts a new segment.
	c.JournalSegmentBytes = 1
	return c
}

// segmentSkipWrite appends rounds with seals every third round. Owners also
// seal on their 32-row bound, so their frontiers differ per producer.
func segmentSkipWrite(t *testing.T, s *Store, rounds int, after func(round int)) {
	t.Helper()
	for round := 0; round < rounds; round++ {
		for i, p := range s.Producers {
			writeRound := int(p.applyOrder / 2)
			apps := restartApps(t, writeRound*2, writeRound*2+2)
			bits := []uint64{0, 1 << 63, 1, 0x7ff8000000000042, 0x7ff0000000000000, 0xfff0000000000000}
			var err error
			for j := range apps {
				a := &apps[j]
				a.Order = p.applyOrder + uint64(j) + 1
				a.Name = fmt.Sprintf("parallel.%d.%d", i, j)
				a.Policy, err = v2core.EncodeRollupPolicy(v2core.RollupPolicy{Name: partsformat.ID(a.Name), Group: "test", Config: sealrollup.Config{Method: sealrollup.Last, Archives: []sealrollup.Archive{{Step: 1, Count: 120}, {Step: 10, Count: 120}}}})
				if err != nil {
					t.Fatal(err)
				}
				a.Clock = 10040 - int64(writeRound%3)
				a.Points = []points.Point{{Timestamp: 10000 + int64(writeRound%7), Value: float64(writeRound+i) / 4}, {Timestamp: 10000 + int64(writeRound%11), Value: math.Float64frombits(bits[writeRound%len(bits)])}}
			}
			if err = p.AppendApplications(apps); err != nil {
				t.Fatal(err)
			}
			if round%3 == 2 {
				if err = p.Seal(); err != nil {
					t.Fatal(err)
				}
			}
			if err = p.Drain(); err != nil {
				t.Fatal(err)
			}
		}
		if after != nil {
			after(round)
		}
	}
}

func copyTree(t *testing.T, from, to string) {
	t.Helper()
	err := filepath.Walk(from, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(from, path)
		if err != nil {
			return err
		}
		if info.IsDir() {
			return os.MkdirAll(filepath.Join(to, rel), 0700)
		}
		b, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		return os.WriteFile(filepath.Join(to, rel), b, 0600)
	})
	if err != nil {
		t.Fatal(err)
	}
}

// segmentSkipOpen opens a copy of root and returns its digest and skip count.
func segmentSkipOpen(t *testing.T, root string, full, rebuild bool) ([32]byte, int, int) {
	t.Helper()
	dir := t.TempDir()
	copyTree(t, root, dir)
	if rebuild {
		if err := os.RemoveAll(filepath.Join(dir, "placement-group-00000001", "core")); err != nil {
			t.Fatal(err)
		}
	}
	c := segmentSkipConfig()
	c.fullReplay = full
	s, err := Open(dir, c)
	if err != nil {
		t.Fatal(err)
	}
	skipped, replayed := 0, 0
	for _, p := range s.Producers {
		skipped += p.RecoverySkippedSegments
		replayed += p.RecoveryReplayedSegments
	}
	got := parallelReplayDigest(t, s)
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	return got, skipped, replayed
}

// TestJournalSegmentSkipKill SIGKILLs a writer at several rounds, including
// inside a seal commit, then compares three recoveries of the same files:
// segment skip, the full journal replay, and a full rebuild from the journal
// alone. Skipping must restore the identical logical image.
func TestJournalSegmentSkipKill(t *testing.T) {
	if root := os.Getenv("V2_SEGMENT_SKIP_CHILD"); root != "" {
		cut, _ := strconv.Atoi(os.Getenv("V2_SEGMENT_SKIP_CUT"))
		commits, _ := strconv.Atoi(os.Getenv("V2_SEGMENT_SKIP_COMMITS"))
		var mu sync.Mutex
		seen := 0
		c := segmentSkipConfig()
		c.Hooks = v2core.Hooks{Stage: func(stage string) {
			if stage != "commit" || commits == 0 {
				return
			}
			mu.Lock()
			seen++
			n := seen
			mu.Unlock()
			if n == commits {
				fmt.Println("CUT")
				select {}
			}
		}}
		s, err := Open(root, c)
		if err != nil {
			t.Fatal(err)
		}
		segmentSkipWrite(t, s, 24, func(round int) {
			if round == cut {
				fmt.Println("CUT")
				select {}
			}
		})
		t.Fatal("missed cut")
	}
	// cut10 dies two rounds after the round-8 seal, so every owner's frontier
	// sits two data segments below the journal end: skipping one segment too
	// many drops round 9's points, which only the journal still holds.
	cases := []struct{ cut, commits int }{{10, 0}, {11, 0}, {23, 0}, {99, 23}}
	for _, k := range cases {
		t.Run(fmt.Sprintf("cut%d-commit%d", k.cut, k.commits), func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestJournalSegmentSkipKill$", "-test.timeout=55s")
			cmd.Env = append(os.Environ(), "V2_SEGMENT_SKIP_CHILD="+root, fmt.Sprintf("V2_SEGMENT_SKIP_CUT=%d", k.cut), fmt.Sprintf("V2_SEGMENT_SKIP_COMMITS=%d", k.commits))
			out, err := cmd.StdoutPipe()
			if err != nil {
				t.Fatal(err)
			}
			var stderr synchronizedBuffer
			cmd.Stderr = &stderr
			if err = cmd.Start(); err != nil {
				t.Fatal(err)
			}
			defer func() { _ = cmd.Process.Kill() }()
			scan := bufio.NewScanner(out)
			cut := false
			for scan.Scan() {
				if scan.Text() == "CUT" {
					cut = true
					break
				}
			}
			if !cut {
				_ = cmd.Wait()
				t.Fatalf("no crash barrier: %s", stderr.String())
			}
			if err = cmd.Process.Kill(); err != nil {
				t.Fatal(err)
			}
			_ = cmd.Wait()
			// Each owner's first seal writes its rollup checkpoint, and later
			// seals fall inside the checkpoint spacing: the skip open loads a
			// checkpoint written before the cut and folds the newer parts.
			if ckpts, _ := filepath.Glob(filepath.Join(root, "placement-group-*", "core", "owner-*", "rollup-state.ckpt")); len(ckpts) == 0 {
				t.Fatal("no rollup checkpoint written before the cut")
			}
			want, _, _ := segmentSkipOpen(t, root, true, true)
			full, fullSkipped, _ := segmentSkipOpen(t, root, true, false)
			got, skipped, replayed := segmentSkipOpen(t, root, false, false)
			if fullSkipped != 0 {
				t.Fatalf("oracle skipped %d segments", fullSkipped)
			}
			if full != want {
				t.Fatalf("full replay %x != journal rebuild %x", full, want)
			}
			if got != want {
				t.Fatalf("segment skip %x != journal rebuild %x (skipped %d)", got, want, skipped)
			}
			if k.cut >= 10 && skipped == 0 {
				t.Fatalf("no segment skipped (replayed %d)", replayed)
			}
			t.Logf("skipped=%d replayed=%d sha256=%x", skipped, replayed, got)
		})
	}
}

// TestJournalSegmentSkipQuiesced kills a store after every owner sealed the
// whole journal, with and without segment rotation: restart replays no segment. A tail written after that seal is only
// in the journal, so restart must replay it.
func TestJournalSegmentSkipQuiesced(t *testing.T) {
	if root := os.Getenv("V2_QUIESCED_CHILD"); root != "" {
		c := segmentSkipConfig()
		c.JournalSegmentBytes, _ = strconv.ParseInt(os.Getenv("V2_QUIESCED_SEGMENT"), 10, 64)
		s, err := Open(root, c)
		if err != nil {
			t.Fatal(err)
		}
		segmentSkipWrite(t, s, 7, nil)
		for i, p := range s.Producers {
			if err = s.Core.AppendBatches(i, nil, p.cursor, true); err != nil {
				t.Fatal(err)
			}
		}
		if os.Getenv("V2_QUIESCED_TAIL") == "true" {
			segmentSkipWrite(t, s, 1, nil)
		}
		fmt.Println("CUT")
		select {}
	}
	for _, k := range []struct {
		name         string
		segmentBytes int64
		tail         bool
	}{{"rotate", 1, false}, {"one-segment", 0, false}, {"rotate-tail", 1, true}, {"one-segment-tail", 0, true}} {
		t.Run(k.name, func(t *testing.T) {
			root := t.TempDir()
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestJournalSegmentSkipQuiesced$", "-test.timeout=55s")
			cmd.Env = append(os.Environ(), "V2_QUIESCED_CHILD="+root, fmt.Sprintf("V2_QUIESCED_SEGMENT=%d", k.segmentBytes), fmt.Sprintf("V2_QUIESCED_TAIL=%v", k.tail))
			out, err := cmd.StdoutPipe()
			if err != nil {
				t.Fatal(err)
			}
			var stderr synchronizedBuffer
			cmd.Stderr = &stderr
			if err = cmd.Start(); err != nil {
				t.Fatal(err)
			}
			defer cmd.Process.Kill()
			scan := bufio.NewScanner(out)
			cut := false
			for scan.Scan() {
				if scan.Text() == "CUT" {
					cut = true
					break
				}
			}
			if !cut {
				_ = cmd.Wait()
				t.Fatalf("no crash barrier: %s", stderr.String())
			}
			if err = cmd.Process.Kill(); err != nil {
				t.Fatal(err)
			}
			_ = cmd.Wait()
			want, _, _ := segmentSkipOpen(t, root, true, true)
			got, skipped, replayed := segmentSkipOpen(t, root, false, false)
			if got != want {
				t.Fatalf("segment skip %x != journal rebuild %x (skipped %d, replayed %d)", got, want, skipped, replayed)
			}
			if k.tail != (replayed > 0) {
				t.Fatalf("tail=%v but replayed %d segments (skipped %d)", k.tail, replayed, skipped)
			}
			t.Logf("skipped=%d replayed=%d", skipped, replayed)
		})
	}
}

// A sealed producer opens no catalog at restart, but its first new write must
// recover the persisted IDs and reject changes to their immutable definitions.
func TestJournalSegmentSkipLazyCatalog(t *testing.T) {
	root := t.TempDir()
	c := segmentSkipConfig()
	s, err := Open(root, c)
	if err != nil {
		t.Fatal(err)
	}
	segmentSkipWrite(t, s, 3, nil)
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(root, c)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for i, p := range s.Producers {
		if p.catalog != nil {
			t.Fatal("sealed producer opened its catalog")
		}
		apps := restartApps(t, 8, 10)
		for j := range apps {
			a := &apps[j]
			a.Name = fmt.Sprintf("parallel.%d.%d", i, j)
			a.Policy, err = v2core.EncodeRollupPolicy(v2core.RollupPolicy{Name: partsformat.ID(a.Name), Group: "test", Config: sealrollup.Config{Method: sealrollup.Last, Archives: []sealrollup.Archive{{Step: 1, Count: 120}, {Step: 10, Count: 120}}}})
			if err != nil {
				t.Fatal(err)
			}
			a.Order = p.applyOrder + uint64(j) + 1
		}
		if err = p.AppendApplications(apps); err != nil {
			t.Fatal(err)
		}
		if err = p.Drain(); err != nil {
			t.Fatal(err)
		}
		if p.catalog == nil || p.catalog.Count() != 2 {
			t.Fatal("first write changed catalog identities")
		}
		if _, err = p.RegisterApplication(apps[0].Name, []byte("changed")); err == nil {
			t.Fatal("changed definition accepted after lazy open")
		}
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	image := t.TempDir()
	copyTree(t, root, image)
	want, _, _ := segmentSkipOpen(t, image, true, true)
	got, _, _ := segmentSkipOpen(t, image, false, false)
	if got != want {
		t.Fatalf("resumed write digest %x != journal rebuild %x", got, want)
	}
}

func TestJournalSegmentEndTornTail(t *testing.T) {
	// Header-only records deliberately omit a PDC4 payload: this path must not
	// decode points that the core already owns.
	record := append([]byte{}, applicationMagic...)
	for _, n := range []uint64{3, 1, 1, 7, 3, 1, 2, 3, 0} {
		record = binary.AppendUvarint(record, n)
	}
	record = binary.LittleEndian.AppendUint32(record, crc32.Checksum(record, applicationCRC))
	framed := binary.LittleEndian.AppendUint32(nil, uint32(len(record)))
	framed = append(framed, record...)
	for _, tail := range [][]byte{nil, {1}, {10, 0, 0, 0, 1, 2}} {
		for _, newest := range []bool{false, true} {
			path := filepath.Join(t.TempDir(), "segment.pdc4")
			if err := os.WriteFile(path, append(append([]byte{}, framed...), tail...), 0600); err != nil {
				t.Fatal(err)
			}
			end, order, ok, err := segmentEnd(path, newest)
			if len(tail) > 0 && !newest {
				if err == nil {
					t.Fatal("accepted torn older segment")
				}
				continue
			}
			if err != nil || !ok || end != 3 || order != 7 {
				t.Fatalf("end=%d order=%d ok=%v err=%v", end, order, ok, err)
			}
			st, err := os.Stat(path)
			if err != nil || st.Size() != int64(len(framed)) {
				t.Fatal("newest torn tail not truncated")
			}
		}
	}
	path := filepath.Join(t.TempDir(), "bad.pdc4")
	framed[len(framed)-1] ^= 1
	if err := os.WriteFile(path, framed, 0600); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := segmentEnd(path, true); err == nil {
		t.Fatal("accepted corrupt final record")
	}
}
