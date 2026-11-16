package downsize

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func writeTypedLegacyCatalog(t *testing.T, s *stateStore, finalNewline bool) ([]byte, []syncName) {
	t.Helper()
	var data bytes.Buffer
	names := make([]syncName, s.unit.CatalogNames)
	for i := range names {
		names[i] = syncName{Name: fmt.Sprintf("service.metric.%09d", i), Sources: uint64(i%7) | 1<<62, OnTarget: i%2 == 0}
		fmt.Fprintf(&data, "%s\t%x\t%d\n", names[i].Name, names[i].Sources, i%2^1)
	}
	b := data.Bytes()
	if !finalNewline {
		b = b[:len(b)-1]
	}
	if err := os.WriteFile(filepath.Join(filepath.Dir(s.path), "union.tsv"), b, 0600); err != nil {
		t.Fatal(err)
	}
	return bytes.Clone(b), names
}

func TestTypedStateCatalogAdoptionResumeAndIndependentParity(t *testing.T) {
	for _, finalNewline := range []bool{true, false} {
		t.Run(fmt.Sprint(finalNewline), func(t *testing.T) {
			s := newTypedState(t, statePageNames*2+7)
			original, names := writeTypedLegacyCatalog(t, s, finalNewline)
			f, err := s.adoptSource("union.tsv")
			if err != nil {
				t.Fatal(err)
			}
			// Commit the first actual source page, then stop before its successor.
			reader := bufio.NewReader(f.file)
			next := f.cursor
			for range statePageNames {
				line, err := reader.ReadBytes('\n')
				if err != nil {
					t.Fatal(err)
				}
				f.prefix.Write(line)
				next.Offset += int64(len(line))
				next.Records++
			}
			copy(next.PrefixSHA256[:], f.prefix.Sum(nil))
			if err := f.commit(next, func(tx *stateTx) error { return tx.putCatalog(1, names[:statePageNames]) }); err != nil {
				t.Fatal(err)
			}
			// A stale reader cannot commit another page over the saved position.
			stale := *f
			stale.cursor.Offset = 0
			stale.cursor.Records = 0
			stale.cursor.PrefixSHA256 = sha256.Sum256(nil)
			if err := stale.commit(next, func(tx *stateTx) error { return nil }); err == nil {
				t.Fatal("stale import writer accepted")
			}
			if err := f.file.Close(); err != nil {
				t.Fatal(err)
			}
			if err := s.close(); err != nil {
				t.Fatal(err)
			}
			s, err = openStateStore(s.path, s.unit, false)
			if err != nil {
				t.Fatal(err)
			}
			defer s.close()
			stats, err := s.adoptCatalog("union.tsv")
			if err != nil || stats.ResumedNames != statePageNames || stats.AdoptedNames != statePageNames+7 {
				t.Fatalf("resume: %+v %v", stats, err)
			}
			stats, err = s.adoptCatalog("union.tsv")
			if err != nil || stats.AdoptedNames != 0 || stats.ResumedNames != int64(len(names)) {
				t.Fatalf("completed import replay: %+v %v", stats, err)
			}
			// Construct the expected catalog independently from the original
			// semantic names, without calling the importer or sharing its cursor.
			oracle := newTypedState(t, int64(len(names)))
			if err := oracle.update(func(tx *stateTx) error {
				for i := 0; i < len(names); i += statePageNames {
					if err := tx.putCatalog(int64(i+1), names[i:min(i+statePageNames, len(names))]); err != nil {
						return err
					}
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			if err := s.verifyRelations(typedDigests(t, oracle)); err != nil {
				t.Fatal(err)
			}
			for _, id := range []int64{1, statePageNames, statePageNames + 1, int64(len(names))} {
				got, err := s.catalogName(id)
				if err != nil || got != names[id-1] {
					t.Fatalf("name %d: %+v %v", id, got, err)
				}
			}
			data, err := os.ReadFile(filepath.Join(filepath.Dir(s.path), "union.tsv"))
			if err != nil || !bytes.Equal(original, data) {
				t.Fatal("adoption modified legacy input")
			}
		})
	}
}

func TestTypedStateAdoptionRejectsChangedSourceAndParity(t *testing.T) {
	s := newTypedState(t, 5)
	original, _ := writeTypedLegacyCatalog(t, s, true)
	if _, err := s.adoptCatalog("union.tsv"); err != nil {
		t.Fatal(err)
	}
	before := typedDigests(t, s)
	path := filepath.Join(filepath.Dir(s.path), "union.tsv")
	st, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	changed := bytes.Clone(original)
	changed[0] ^= 1
	if err := os.WriteFile(path, changed, 0600); err != nil {
		t.Fatal(err)
	}
	// Preserve size, inode and mtime: the complete SHA must catch this edit.
	if err := os.Chtimes(path, st.ModTime(), st.ModTime()); err != nil {
		t.Fatal(err)
	}
	if _, err := s.adoptCatalog("union.tsv"); err == nil {
		t.Fatal("same-stamp changed input accepted")
	}
	if !reflect.DeepEqual(before, typedDigests(t, s)) {
		t.Fatal("failed authentication changed semantic relations")
	}
	wrong := typedDigests(t, s)
	d := wrong["phase"]
	d.SHA256 = strings.Repeat("0", 64)
	wrong["phase"] = d
	if err := s.verifyRelations(wrong); err == nil || !strings.Contains(err.Error(), "phase") {
		t.Fatalf("did not name mismatched relation: %v", err)
	}
}

func TestTypedStateAdoptionRollbackAndSourcePath(t *testing.T) {
	s := newTypedState(t, 5)
	writeTypedLegacyCatalog(t, s, true)
	f, err := s.adoptSource("union.tsv")
	if err != nil {
		t.Fatal(err)
	}
	defer f.file.Close()
	next := f.cursor
	next.Offset, next.Records, next.Complete = next.Source.Size, 5, true
	next.PrefixSHA256 = next.Source.SHA256
	before := typedDigests(t, s)
	err = f.commit(next, func(tx *stateTx) error {
		if err := tx.putIssue(stateIssue{Scope: "root", Kind: "quarantine", NameID: 1, Reason: "test reason", Phase: "verify", FirstSeen: 1, LastSeen: 1}); err != nil {
			return err
		}
		return fmt.Errorf("independent reducer mismatch")
	})
	if err == nil || !reflect.DeepEqual(before, typedDigests(t, s)) {
		t.Fatal("mismatch committed a partial relation")
	}
	cursor, err := readStateAdoptPosition(s.db, f.cursor.Source)
	if err != nil || cursor.Offset != 0 || cursor.Records != 0 {
		t.Fatalf("mismatch advanced import cursor: %+v %v", cursor, err)
	}
	for _, path := range []string{"../union.tsv", "/etc/passwd", "", "./union.tsv"} {
		if f, err := s.adoptSource(path); err == nil {
			f.file.Close()
			t.Fatalf("accepted path %q", path)
		}
	}
	if err := os.Symlink("union.tsv", filepath.Join(filepath.Dir(s.path), "linked.tsv")); err != nil {
		t.Fatal(err)
	}
	if f, err := s.adoptSource("linked.tsv"); err == nil {
		f.file.Close()
		t.Fatal("accepted symlink source")
	}
}

func TestTypedStateInspectCommand(t *testing.T) {
	s := newTypedState(t, 5)
	_, names := writeTypedLegacyCatalog(t, s, true)
	if _, err := s.adoptCatalog("union.tsv"); err != nil {
		t.Fatal(err)
	}
	for _, query := range [][]string{{"--id", "3"}, {"--name", names[2].Name}} {
		var output bytes.Buffer
		args := append([]string{"inspect", "--database", s.path, "--hashes"}, query...)
		if err := stateMain(context.Background(), args, &output); err != nil {
			t.Fatal(err)
		}
		var got stateInspection
		if err := json.Unmarshal(output.Bytes(), &got); err != nil || got.NameID != 3 || got.Name == nil || *got.Name != names[2] || got.Phases[0].Phase != "window retry" || len(got.Digests) != 12 {
			t.Fatalf("inspection: %s %v", output.String(), err)
		}
	}
}
