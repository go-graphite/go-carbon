package v2daemon

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2core"
	"github.com/go-graphite/go-carbon/internal/v2dict"
	"github.com/go-graphite/go-carbon/internal/v2part"
	"github.com/go-graphite/go-carbon/points"
)

func admitConfig() Config {
	return Config{Layer: 3, Producers: 1, Shards: 1, GroupInterval: time.Hour, Hooks: v2core.Hooks{IDKeys: true, Identities: &v2core.PolicyCatalog{}}}
}

func admitPolicy(t *testing.T, name string) []byte {
	t.Helper()
	raw, err := v2core.EncodeRollupPolicy(v2core.RollupPolicy{Name: v2part.ID(name), Group: "test", Config: sealrollup.Config{Method: sealrollup.Sum, XFilesFactor: .5, Archives: []sealrollup.Archive{{Step: 1, Count: 120}, {Step: 10, Count: 120}}}})
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func admitApp(t *testing.T, name string, order uint64) Application {
	return Application{Name: name, Policy: admitPolicy(t, name), Order: order, Clock: 10020, Points: []points.Point{{Timestamp: 10000 + int64(order), Value: float64(order)}}, Sequence: []uint64{order}}
}

func dictDir(root string) string {
	return filepath.Join(root, "placement-group-00000001", "core", "dict")
}

// admitted returns the identity and dictionary ID the producer's catalog
// gave name, and checks the definition the dictionary alone serves against
// the catalog's for that name.
func admitted(t *testing.T, s *Store, name string) (*v2core.ApplicationPolicy, uint32) {
	t.Helper()
	p := s.Producers[0]
	local := p.catalog.Find(name)
	if local == 0 {
		t.Fatalf("%s not in the catalog", name)
	}
	h := s.Core.ApplicationPolicy(NumericID(local, p.index, p.count))
	id, ok := h.DictID()
	if !ok {
		t.Fatalf("%s has no dictionary ID after its drain", name)
	}
	_, def, ok := p.catalog.Lookup(local)
	if !ok {
		t.Fatalf("%s: catalog lookup failed", name)
	}
	got, ok := h.DictionaryDefinition()
	if !ok || !bytes.Equal(got, def) || !bytes.Equal(got, h.Definition()) || !bytes.Equal(def, admitPolicy(t, name)) {
		t.Fatalf("%s: dictionary definition %q ok=%v, catalog %q", name, got, ok, def)
	}
	e, ok := s.Core.Dictionary().Lookup("test", v2part.Route(v2part.ID(name)), []byte(name))
	if !ok || e.Deleted() || e.ID != id || e.Policy == 0 {
		t.Fatalf("%s: dictionary entry %+v ok=%v, identity ID %d", name, e, ok, id)
	}
	return h, id
}

// TestAdmissionAssignsDictionaryIDs: a drained name has its dictionary ID
// and policy before any seal, the seal keeps it, and the definition served
// from the dictionary equals the catalog's for every name.
func TestAdmissionAssignsDictionaryIDs(t *testing.T) {
	v2part.IDKeys.Store(true)
	defer v2part.IDKeys.Store(false)
	root := t.TempDir()
	s, err := Open(root, admitConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if s != nil {
			_ = s.Close()
		}
	}()
	p := s.Producers[0]
	var apps []Application
	for i := range 40 {
		apps = append(apps, admitApp(t, fmt.Sprintf("admit.%d", i), uint64(i+1)))
	}
	apps = append(apps, admitApp(t, "admit.0", 41)) // a repeat in the same drain
	if err = p.AppendApplications(apps); err != nil {
		t.Fatal(err)
	}
	if len(p.admit) != len(apps) {
		t.Fatalf("queued %d names for admission, want %d", len(p.admit), len(apps))
	}
	if h := s.Core.ApplicationPolicy(NumericID(1, 0, 1)); !h.NeedsDictID() {
		t.Fatal("a new name has a dictionary ID before its drain")
	}
	gen := s.Core.Dictionary().Generation()
	if err = p.Drain(); err != nil {
		t.Fatal(err)
	}
	if len(p.admit) != 0 {
		t.Fatal("drain kept the admission queue")
	}
	if g := s.Core.Dictionary().Generation(); g-gen != 40 {
		t.Fatalf("drain made %d dictionary records durable, want 40 (one per new name)", g-gen)
	}
	ids := map[string]uint32{}
	for i := range 40 {
		name := fmt.Sprintf("admit.%d", i)
		_, ids[name] = admitted(t, s, name)
	}
	// Known names queue nothing; the seal finds every ID present.
	if err = p.AppendApplications([]Application{admitApp(t, "admit.1", 42)}); err != nil {
		t.Fatal(err)
	}
	if len(p.admit) != 0 {
		t.Fatal("a name with a dictionary ID was queued again")
	}
	if err = p.Drain(); err != nil {
		t.Fatal(err)
	}
	if err = s.Core.AppendBatches(0, nil, p.cursor, true); err != nil {
		t.Fatal(err)
	}
	if g := s.Core.Dictionary().Generation(); g-gen != 40 {
		t.Fatalf("seal appended %d more dictionary records", g-gen-40)
	}
	for name, id := range ids {
		if _, got := admitted(t, s, name); got != id {
			t.Fatalf("%s changed ID %d -> %d at seal", name, id, got)
		}
	}
}

// TestAdmissionDictionaryKill SIGKILLs a store right after a drain admitted
// a never-sealed name, tears the dictionary log's tail, and requires the
// name durable in the dictionary on restart, with the same ID and the same
// definition when it is admitted again.
func TestAdmissionDictionaryKill(t *testing.T) {
	v2part.IDKeys.Store(true)
	defer v2part.IDKeys.Store(false)
	if root := os.Getenv("V2_ADMIT_CHILD_ROOT"); root != "" {
		s, err := Open(root, admitConfig())
		if err != nil {
			t.Fatal(err)
		}
		p := s.Producers[0]
		if err = p.AppendApplications([]Application{admitApp(t, "sealed.0", 1), admitApp(t, "sealed.1", 2)}); err != nil {
			t.Fatal(err)
		}
		if err = p.Drain(); err != nil {
			t.Fatal(err)
		}
		if err = s.Core.AppendBatches(0, nil, p.cursor, true); err != nil {
			t.Fatal(err)
		}
		if err = p.AppendApplications([]Application{admitApp(t, "fresh.0", 3)}); err != nil {
			t.Fatal(err)
		}
		if err = p.Drain(); err != nil {
			t.Fatal(err)
		}
		for _, name := range []string{"sealed.0", "sealed.1", "fresh.0"} {
			_, id := admitted(t, s, name)
			fmt.Printf("ID %s %d\n", name, id)
		}
		fmt.Println("READY")
		for {
			time.Sleep(time.Second)
		}
	}
	root := t.TempDir()
	cmd := exec.Command(os.Args[0], "-test.run=^TestAdmissionDictionaryKill$", "-test.timeout=30s")
	cmd.Env = append(os.Environ(), "V2_ADMIT_CHILD_ROOT="+root)
	pipe, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	var stderr synchronizedBuffer
	cmd.Stderr = &stderr
	if err = cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cmd.Process.Kill() }()
	want := map[string]uint32{}
	ready := make(chan bool, 1)
	go func() {
		scan := bufio.NewScanner(pipe)
		for scan.Scan() {
			f := strings.Fields(scan.Text())
			if len(f) == 3 && f[0] == "ID" {
				id, _ := strconv.ParseUint(f[2], 10, 32)
				want[f[1]] = uint32(id)
			}
			if scan.Text() == "READY" {
				ready <- true
				return
			}
		}
		ready <- false
	}()
	select {
	case ok := <-ready:
		if !ok {
			_ = cmd.Wait()
			t.Fatal("child did not admit", stderr.String())
		}
	case <-time.After(20 * time.Second):
		t.Fatal("child timeout", stderr.String())
	}
	if err = cmd.Process.Signal(syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	err = cmd.Wait()
	exit := &exec.ExitError{}
	if !errors.As(err, &exit) || !exit.Sys().(syscall.WaitStatus).Signaled() {
		t.Fatalf("not SIGKILL: %v", err)
	}
	if len(want) != 3 {
		t.Fatalf("child reported %v", want)
	}
	// A record torn by the kill after the last synced one.
	logs, _ := filepath.Glob(filepath.Join(dictDir(root), "*.log"))
	if len(logs) == 0 {
		t.Fatal("no dictionary log")
	}
	slices.Sort(logs)
	f, err := os.OpenFile(logs[len(logs)-1], os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.Write([]byte{60, 0, 0, 0, 1, 2, 3, 4, 9, 0, 't', 'e'}); err != nil {
		t.Fatal(err)
	}
	_ = f.Close()

	s, err := Open(root, admitConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	// Durable at admission: present before any re-admission.
	for name, id := range want {
		e, ok := s.Core.Dictionary().Lookup("test", v2part.Route(v2part.ID(name)), []byte(name))
		if !ok || e.ID != id || e.Policy == 0 {
			t.Fatalf("%s after kill: entry %+v ok=%v, want ID %d with a policy", name, e, ok, id)
		}
		def, ok := v2core.DictionaryDefinition(s.Core.Dictionary(), "test", v2part.Route(v2part.ID(name)), id)
		if !ok || !bytes.Equal(def, admitPolicy(t, name)) {
			t.Fatalf("%s after kill: dictionary-only definition %q ok=%v", name, def, ok)
		}
	}
	p := s.Producers[0]
	if err = p.AppendApplications([]Application{admitApp(t, "fresh.0", 10), admitApp(t, "sealed.0", 11), admitApp(t, "fresh.1", 12)}); err != nil {
		t.Fatal(err)
	}
	if err = p.Drain(); err != nil {
		t.Fatal(err)
	}
	for name, id := range want {
		if name == "sealed.1" {
			continue
		}
		if _, got := admitted(t, s, name); got != id {
			t.Fatalf("%s changed dictionary ID across the kill: %d -> %d", name, id, got)
		}
	}
	admitted(t, s, "fresh.1")
}

// TestAdmissionRevivedNameGetsNewID: a name deleted from the dictionary and
// admitted again gets a new ID; the old one is never reused.
func TestAdmissionRevivedNameGetsNewID(t *testing.T) {
	v2part.IDKeys.Store(true)
	defer v2part.IDKeys.Store(false)
	root := t.TempDir()
	const name = "lazy.0"
	vshard := v2part.Route(v2part.ID(name))
	s, err := Open(root, admitConfig())
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Producers[0].AppendApplications([]Application{admitApp(t, name, 1)}); err != nil {
		t.Fatal(err)
	}
	if err = s.Producers[0].Drain(); err != nil {
		t.Fatal(err)
	}
	_, old := admitted(t, s, name)
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	d, err := v2dict.Open(dictDir(root), v2dict.Options{Shards: 1})
	if err != nil {
		t.Fatal(err)
	}
	if ok, err := d.Delete("test", vshard, []byte(name)); !ok || err != nil {
		t.Fatalf("delete: %v %v", ok, err)
	}
	if err = d.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(root, admitConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	if err = s.Producers[0].AppendApplications([]Application{admitApp(t, name, 2)}); err != nil {
		t.Fatal(err)
	}
	if err = s.Producers[0].Drain(); err != nil {
		t.Fatal(err)
	}
	_, revived := admitted(t, s, name)
	if revived <= old {
		t.Fatalf("revived name reused ID %d (old %d)", revived, old)
	}
	if _, _, ok := s.Core.Dictionary().LookupID("test", vshard, old, nil); ok {
		t.Fatal("the deleted ID still resolves")
	}
}

// TestAdmissionOldDictionary opens a dictionary written before the policy
// table (entries with policy 0, no table file): its names keep their IDs,
// the dictionary reports no definition for them, and the catalog serves it.
func TestAdmissionOldDictionary(t *testing.T) {
	v2part.IDKeys.Store(true)
	defer v2part.IDKeys.Store(false)
	root := t.TempDir()
	const name = "old.0"
	vshard := v2part.Route(v2part.ID(name))
	d, err := v2dict.Open(dictDir(root), v2dict.Options{Shards: 1})
	if err != nil {
		t.Fatal(err)
	}
	for i := range 3 { // IDs 0..2; old.0 gets 2 if it is in the same vshard
		if _, _, err = d.Append("test", vshard, []byte(fmt.Sprintf("old.pad.%d", i)), 0); err != nil {
			t.Fatal(err)
		}
	}
	e, _, err := d.Append("test", vshard, []byte(name), 0)
	if err != nil {
		t.Fatal(err)
	}
	if err = d.Close(); err != nil {
		t.Fatal(err)
	}
	if err = os.Remove(filepath.Join(dictDir(root), "policies")); err != nil {
		t.Fatal(err)
	}
	s, err := Open(root, admitConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	p := s.Producers[0]
	if err = p.AppendApplications([]Application{admitApp(t, name, 1)}); err != nil {
		t.Fatal(err)
	}
	if err = p.Drain(); err != nil {
		t.Fatal(err)
	}
	h := s.Core.ApplicationPolicy(NumericID(p.catalog.Find(name), 0, 1))
	id, ok := h.DictID()
	if !ok || id != e.ID {
		t.Fatalf("old name got ID %d ok=%v, want %d", id, ok, e.ID)
	}
	if def, ok := h.DictionaryDefinition(); ok {
		t.Fatalf("policy-0 entry served a dictionary definition %q", def)
	}
	if !bytes.Equal(h.Definition(), admitPolicy(t, name)) {
		t.Fatal("catalog definition changed")
	}
}
