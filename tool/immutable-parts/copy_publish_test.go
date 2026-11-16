package main

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"fmt"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
)

func TestCopyManifestAuthenticationAndResume(t *testing.T) {
	p := copyFixture(t, t.TempDir(), 2, 3, 8, nil)
	snapshot := copySnapshot{Version: 1, Partitions: p}
	snapshot.Frontier[0] = 1
	blob, sum, err := copyPublish(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(blob, []byte(p[0].Files[0].Path)) {
		t.Fatal("sender path published")
	}
	snapshot.Frontier[0] = 2
	_, changed, _ := copyPublish(snapshot)
	if changed == sum {
		t.Fatal("frontier not bound")
	}
	// The TLS fixture certificate is trusted for both client and server roles.
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{SerialNumber: big.NewInt(1), NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	leaf, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	cert := tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
	roots := x509.NewCertPool()
	roots.AddCert(leaf)
	handler := copyManifestHandler(blob, map[[32]byte]bool{sha256.Sum256(leaf.Raw): true})
	var requests atomic.Int32
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { requests.Add(1); handler.ServeHTTP(w, r) }))
	server.TLS = &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{cert}, ClientCAs: roots, ClientAuth: tls.RequireAndVerifyClientCert}
	server.StartTLS()
	defer server.Close()
	client := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots, Certificates: []tls.Certificate{cert}}}}
	dir := t.TempDir()
	name := hex.EncodeToString(sum[:]) + ".json"
	// Emulate an interrupted, durably received prefix. Only the suffix travels.
	if err = os.WriteFile(filepath.Join(dir, name+".partial"), blob[:len(blob)/2], 0600); err != nil {
		t.Fatal(err)
	}
	got, err := copyFetchManifest(context.Background(), client, server.URL, dir, sum)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, blob) || requests.Load() != 1 {
		t.Fatal("resume differs")
	}
	got, err = copyFetchManifest(context.Background(), client, server.URL, dir, sum)
	if err != nil || !bytes.Equal(got, blob) || requests.Load() != 1 {
		t.Fatal("completed download repeated", err)
	}
	if _, err = copyFetchManifest(context.Background(), client, "http://example.invalid", t.TempDir(), sum); err == nil {
		t.Fatal("accepted cleartext")
	}
	r := httptest.NewRequest("GET", "https://example.invalid/v1/manifests/"+hex.EncodeToString(sum[:]), nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, r)
	if w.Code != http.StatusUnauthorized {
		t.Fatal("unauthenticated publication")
	}
	// A CA-valid but unlisted peer is denied as well.
	r.TLS = &tls.ConnectionState{VerifiedChains: [][]*x509.Certificate{{leaf}}, PeerCertificates: []*x509.Certificate{leaf}}
	w = httptest.NewRecorder()
	copyManifestHandler(blob, nil).ServeHTTP(w, r)
	if w.Code != http.StatusUnauthorized {
		t.Fatal("unlisted certificate")
	}
}

func TestCopyManifestInterruptedBody(t *testing.T) {
	blob := []byte(`{"immutable":"manifest"}`)
	sum := sha256.Sum256(blob)
	name := hex.EncodeToString(sum[:])
	var count int
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		count++
		w.Header().Set("ETag", `"`+name+`"`)
		if count == 1 {
			w.Header().Set("Content-Length", fmt.Sprint(len(blob)))
			_, _ = w.Write(blob[:8])
			return
		}
		if r.Header.Get("Range") != "bytes=8-" {
			t.Error("wrong resume offset", r.Header)
		}
		w.Header().Set("Content-Range", fmt.Sprintf("bytes 8-%d/%d", len(blob)-1, len(blob)))
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(blob[8:])
	}))
	defer server.Close()
	dir := t.TempDir()
	if _, err := copyFetchManifest(context.Background(), server.Client(), server.URL, dir, sum); err == nil {
		t.Fatal("accepted truncated body")
	}
	b, err := copyFetchManifest(context.Background(), server.Client(), server.URL, dir, sum)
	if err != nil || !bytes.Equal(b, blob) {
		t.Fatal(err)
	}
}

func TestCopyDoneLedgerCrashReconcile(t *testing.T) {
	p := copyFixture(t, t.TempDir(), 1, 2, 8, nil)[0]
	root := t.TempDir()
	gen := "store-a/frontier-1"
	reconciles := 0
	installed := false
	newLedger := func() *copyDoneLedger {
		return &copyDoneLedger{Dir: root, Snapshot: sha256.Sum256([]byte("snapshot")), Generation: func(int) (string, error) { return gen, nil }, Reconcile: func(int) (bool, error) { reconciles++; return installed, nil }}
	}
	ledger := newLedger()
	done, err := ledger.begin(0, p)
	if err != nil || done {
		t.Fatal(done, err)
	}
	// Crash after ingest, before finish: reopen, reconcile, persist done.
	installed = true
	gen = "store-a/frontier-2"
	ledger = newLedger()
	done, err = ledger.begin(0, p)
	if err != nil || !done || reconciles != 1 {
		t.Fatal(done, err, reconciles)
	}
	ledger = newLedger()
	done, err = ledger.begin(0, p)
	if err != nil || !done || reconciles != 1 {
		t.Fatal("done repeated", done, err, reconciles)
	}
	gen = "store-b/frontier-1"
	installed = false
	done, err = ledger.begin(0, p)
	if err != nil || done {
		t.Fatal("stale incarnation skipped", done, err)
	}
	ledger.Generation = nil
	if _, err = ledger.begin(0, p); err == nil {
		t.Fatal("missing durable fence accepted")
	}
}

func TestCopyResumableTransferDoesNotResend(t *testing.T) {
	p := copyFixture(t, t.TempDir(), 3, 2, 8, nil)
	address, stop := copyTestServer(t, p)
	defer stop()
	dir := t.TempDir()
	db := copyTestDB(t, filepath.Join(dir, "db"))
	defer db.Close()
	l := &copyDoneLedger{Dir: filepath.Join(dir, "ledger"), Generation: func(int) (string, error) { return "durable-incarnation", nil }, Reconcile: func(int) (bool, error) { return false, nil }}
	for round := 0; round < 2; round++ {
		st, err := copyTransferResumable(context.Background(), address, p, []int{0, 1, 2}, 2, []*pebble.DB{db}, dir, l)
		if err != nil {
			t.Fatal(err)
		}
		if round == 0 && st.Bytes == 0 || round == 1 && st.Bytes != 0 {
			t.Fatal("finished partition resent", st)
		}
	}
	if _, err := copyTransferResumable(context.Background(), address, p, []int{0, 0}, 2, []*pebble.DB{db}, dir, l); err == nil {
		t.Fatal("duplicate task accepted")
	}
}

func TestCopyReceiptLogicalIdentity(t *testing.T) {
	full := copyFixture(t, t.TempDir(), 1, 6, 1024, nil)[0]
	a := copySealedFixture(t, full)
	b := copySealedFixture(t, copySubsetFixture(t, full, t.TempDir(), []int{0, 1, 2, 3, 4, 5}, 33, -1, 4096))
	ak, err := copyReceiptKey(a)
	if err != nil {
		t.Fatal(err)
	}
	bk, err := copyReceiptKey(b)
	if err != nil || ak != bk {
		t.Fatal("seal history changed receipt identity", err)
	}
	b.Blocks.Digest[0] ^= 1
	if _, err = copyReceiptKey(b); err == nil {
		t.Fatal("corrupt logical receipt accepted")
	}
}
