package persister

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestEphemeralCheck(t *testing.T) {
	defer func() {
		if err := os.RemoveAll("testdata"); err != nil {
			t.Fatal(err)
		}
	}()

	newFile("testdata/ns1/ns0/e1/cpu.wsp", time.Minute)
	newFile("testdata/ns1/ns2/e1/cpu.wsp", time.Minute)
	newFile("testdata/ns1/ns2/e2/cpu.wsp", time.Hour*2)
	newFile("testdata/ns1/ns3/e3/cpu.wsp", time.Minute)
	newFile("testdata/ns3/ns3/e3/cpu.wsp", time.Minute)

	newFile("testdata/ephemeral.conf", 0)
	ioutil.WriteFile("testdata/ephemeral.conf", []byte(`
		[sys]
			pattern = ns1.*.$ephemeral.*
		[sys2]
			pattern = ns3.*.$ephemeral.*
	`), 0644)

	ef, err := newEphemeralFilter("testdata", "testdata/ephemeral.conf", time.Hour)
	if err != nil {
		t.Error(err)
	}

	{
		t.Log("replace older file")
		matched, path, err := ef.check("ns1.ns2.e3.cpu")
		if err != nil {
			t.Error(err)
		}
		if got, want := matched, true; got != want {
			t.Errorf("matched = %t; want %t", got, want)
		}
		if got, want := path, "testdata/ns1/ns2/e2/cpu.wsp"; got != want {
			t.Errorf("path = %s; want %s", got, want)
		}
	}
	{
		t.Log("ignore unmatched pattern")
		matched, path, err := ef.check("ns2.ns2.e3.cpu")
		if err != nil {
			t.Error(err)
		}
		if got, want := matched, false; got != want {
			t.Errorf("matched = %t; want %t", got, want)
		}
		if got, want := path, ""; got != want {
			t.Errorf("path = %s; want %s", got, want)
		}
	}

	{
		t.Log("do not return new file")
		matched, path, err := ef.check("ns3.ns0.e2.cpu")
		if err != nil {
			t.Error(err)
		}
		if got, want := matched, false; got != want {
			t.Errorf("matched = %t; want %t", got, want)
		}
		if got, want := path, ""; got != want {
			t.Errorf("path = %s; want %s", got, want)
		}
	}
}

func newFile(p string, mtime time.Duration) {
	if err := os.MkdirAll(filepath.Dir(p), 0777); err != nil {
		panic(err)
	}

	if _, err := os.Create(p); err != nil {
		panic(err)
	}
	if err := os.Chtimes(p, time.Now().Add(-1*mtime), time.Now().Add(-1*mtime)); err != nil {
		panic(err)
	}
}
