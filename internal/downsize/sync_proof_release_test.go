package downsize

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

type proofReleaseFixture struct {
	calls map[string]int
	fail  string
}

func (b *proofReleaseFixture) ReleaseProofs(_ context.Context, host string, names []string) error {
	if host == b.fail {
		b.fail = ""
		return errors.New("interrupted closeout")
	}
	for _, name := range names {
		b.calls[host+"/"+name]++
	}
	return nil
}
func seedProofCloseoutCatalog(t *testing.T, o syncOptions, names string) {
	t.Helper()
	if err := os.MkdirAll(o.state, 0700); err != nil {
		t.Fatal(err)
	}
	identity, err := syncIdentity(o)
	if err != nil {
		t.Fatal(err)
	}
	j, err := openSyncJournal(filepath.Join(o.state, "sync.jsonl"), identity)
	if err != nil {
		t.Fatal(err)
	}
	j.close()
	if err := os.WriteFile(filepath.Join(o.state, "union.tsv"), []byte(names), 0600); err != nil {
		t.Fatal(err)
	}
}
func TestProofCloseoutAllHoldersInterruptedResume(t *testing.T) {
	o := syncOptions{to: syncT, from: []string{syncD1, syncD2}, state: t.TempDir()}
	seedProofCloseoutCatalog(t, o, "a\t3\t1\nb\t3\t1\n")
	child := o
	child.state = filepath.Join(o.state, "ready-recovery-test")
	child.from = []string{"http://third.example:4242"}
	seedProofCloseoutCatalog(t, child, "c\t1\t1\n")
	b := &proofReleaseFixture{calls: map[string]int{}, fail: syncD2}
	if pending, err := resumeProofCloseout(t.Context(), o, b); pending || err != nil || len(b.calls) != 0 {
		t.Fatal("release before durable closeout", pending, err, b.calls)
	}
	if err := beginProofCloseout(o, false); err != nil {
		t.Fatal(err)
	}
	if pending, err := resumeProofCloseout(t.Context(), o, b); !pending || err == nil {
		t.Fatal("expected interruption", pending, err)
	}
	// Even during cleanup, the catalogue and original verification evidence survive.
	if _, err := os.Stat(filepath.Join(o.state, "union.tsv")); err != nil {
		t.Fatal(err)
	}
	if pending, err := resumeProofCloseout(t.Context(), o, b); !pending || err != nil {
		t.Fatal(pending, err)
	}
	expected := map[string]int{}
	for _, host := range append([]string{syncT}, o.from...) {
		for _, name := range []string{"a", "b"} {
			expected[host+"/"+name] = 1
		}
	}
	for _, host := range []string{syncT, "http://third.example:4242"} {
		expected[host+"/c"] = 1
	}
	if !reflect.DeepEqual(b.calls, expected) {
		t.Fatal(b.calls, expected)
	}
	if pending, err := resumeProofCloseout(t.Context(), o, b); pending || err != nil {
		t.Fatal(pending, err)
	}
	if !reflect.DeepEqual(b.calls, expected) {
		t.Fatal("completed closeout repeated releases", b.calls)
	}
	var receipt proofCloseout
	if err := readReadyObservationJSON(filepath.Join(o.state, "proof-closeout.json"), &receipt); err != nil || !receipt.Completed {
		t.Fatal(receipt, err)
	}
}
func TestProofCloseoutRetainsChangedCatalog(t *testing.T) {
	o := syncOptions{to: syncT, from: []string{syncD1}, state: t.TempDir()}
	seedProofCloseoutCatalog(t, o, "a\t1\t1\n")
	if err := beginProofCloseout(o, false); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(o.state, "union.tsv"), []byte("a\t1\t1\nb\t1\t1\n"), 0600); err != nil {
		t.Fatal(err)
	}
	b := &proofReleaseFixture{calls: map[string]int{}}
	if _, err := resumeProofCloseout(t.Context(), o, b); err == nil || len(b.calls) > 0 {
		t.Fatal("changed catalogue was released", err, b.calls)
	}
}
func TestProofResetDryRunAndApply(t *testing.T) {
	calls := 0
	secret := []byte("proof-reset-test-secret")
	host := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.Method != "POST" || r.URL.RequestURI() != proofMaintenancePath+"?action=reset" {
			t.Error(r.Method, r.URL)
		}
		token, err := jwt.Parse(r.Header.Get("X-Buckyd-Authorization"), func(*jwt.Token) (any, error) { return secret, nil })
		if err != nil || !token.Valid {
			t.Error("missing JWT", err)
		}
		fmt.Fprintf(w, `{"Finished":%q}`, time.Now().UTC().Format(time.RFC3339Nano))
	}))
	defer host.Close()
	if err := ProofResetMain(t.Context(), []string{"--host", host.URL, "--jwt-secret", "/missing/secret"}); err != nil {
		t.Fatal(err)
	}
	if calls != 0 {
		t.Fatal("dry run contacted host")
	}
	file := filepath.Join(t.TempDir(), "secret")
	if err := os.WriteFile(file, secret, 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PROOF_RESET_TEST_TOKEN", "")
	if err := ProofResetMain(t.Context(), []string{"--host", host.URL, "--jwt-secret", file, "--token-env", "PROOF_RESET_TEST_TOKEN", "--apply"}); err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Fatal(calls)
	}
}

func TestProofCloseoutDoesNotResealChangedFrontier(t *testing.T) {
	f, o, r := redoReadyFixture(t)
	ctx := t.Context()
	if err := runSyncReady(ctx, &o, r, f, func() error { return runSync(ctx, o, f) }); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(filepath.Join(o.state, "verification-frontier.bin"))
	if err != nil {
		t.Fatal(err)
	}
	if err := beginProofCloseout(o, false); err != nil {
		t.Fatal(err)
	}
	releaser := &proofReleaseFixture{calls: map[string]int{}}
	if _, err := resumeProofCloseout(ctx, o, releaser); err != nil {
		t.Fatal(err)
	}
	after, err := os.ReadFile(filepath.Join(o.state, "verification-frontier.bin"))
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatal("cleanup changed saved frontier", err)
	}
	// After cleanup a later pass must still compare/revalidate a changed token.
	calls := f.snapshot()
	delete(f.store(syncT).points["metric.001"][3600], syncTestNow-5*3600)
	if err := runSyncReady(ctx, &o, r, f, func() error { return runSync(ctx, o, f) }); err != nil {
		t.Fatal(err)
	}
	repaired := false
	for call, n := range f.snapshot() {
		if n > calls[call] && call.Name == "metric.001" {
			repaired = true
		}
	}
	if !repaired {
		t.Fatal("changed token was blindly resealed after closeout")
	}
}

func TestProofCloseoutAuthenticatesPriorHolder(t *testing.T) {
	secret := []byte("prior-holder-secret")
	host := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		token, err := jwt.Parse(r.Header.Get("X-Buckyd-Authorization"), func(*jwt.Token) (any, error) { return secret, nil })
		if err != nil || !token.Valid {
			t.Error("prior holder did not receive authenticated cleanup", err)
		}
		fmt.Fprint(w, `[{"Name":"proof","Status":"released","Released":1}]`)
	}))
	defer host.Close()
	b := newSyncLiveBackend(syncLiveOptions{auth: &hostTokenCache{readSecret: func() ([]byte, error) { return bytes.Clone(secret), nil }}, authOrigins: []string{"http://current.example:4242"}})
	defer b.m.client.CloseIdleConnections()
	defer b.client.CloseIdleConnections()
	if err := b.ReleaseProofs(t.Context(), host.URL, []string{"proof"}); err != nil {
		t.Fatal(err)
	}
	signed := b.m.client.Transport.(*syncJWTTransport)
	if signed.origins[host.URL] {
		t.Fatal("cleanup changed live origin permissions")
	}
}

func TestProofCloseoutLargeReceipt(t *testing.T) {
	o := syncOptions{to: syncT, from: []string{syncD1}, state: t.TempDir()}
	identity, err := syncIdentity(o)
	if err != nil {
		t.Fatal(err)
	}
	c := proofCloseout{Version: 1, Identity: identity, Completed: true}
	for i := range 1000 {
		c.Tasks = append(c.Tasks, proofCleanupTask{Catalog: fmt.Sprintf("ready-recovery-%d/union.tsv", i), Host: syncD1, Done: true})
	}
	if err := atomicStateJSON(filepath.Join(o.state, "proof-closeout.json"), c); err != nil {
		t.Fatal(err)
	}
	b := &proofReleaseFixture{calls: map[string]int{}}
	if pending, err := resumeProofCloseout(t.Context(), o, b); pending || err != nil || len(b.calls) != 0 {
		t.Fatal(pending, err, b.calls)
	}
}
