package ringplan

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestResizeMixedPlanAndReplacementDigest(t *testing.T) {
	root := t.TempDir()
	s := fixtureSpec(root, 3)
	s.New.Nodes = append([]member(nil), s.Old.Nodes[:2]...)
	s.New.Nodes[0] = member{ID: "replacement", Node: "new-host:2003=000", Endpoint: "http://new-host:4242"}
	s.Placements = map[string]Placement{"replacement": {Worker: "http://new-host:9080", Directory: "/data/clone", Preflight: "/data/preflight.json"}}
	old, err := newRing(&s.Old)
	if err != nil {
		t.Fatal(err)
	}
	var names [3][]string
	for i := range 300 {
		name := fmt.Sprintf("resize.metric.%04d", i)
		owner := old.compiled.Lookup(name, nil)[0]
		names[owner] = append(names[owner], name)
	}
	for i, in := range s.Inputs {
		if err := os.WriteFile(in.Path, []byte(strings.Join(names[i], "\n")+"\n"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	path := filepath.Join(root, "spec.json")
	blob, _ := json.Marshal(s)
	if err := os.WriteFile(path, blob, 0600); err != nil {
		t.Fatal(err)
	}
	out := filepath.Join(root, "plan")
	m, err := Resize(context.Background(), path, out, true, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(m.Whole) != 1 || m.Whole[0].Source != s.Old.Nodes[0].ID || m.Whole[0].Destination != "replacement" {
		t.Fatal("missing whole job", m.Whole)
	}
	incoming, outgoing := false, false
	for _, route := range m.Streams {
		if route.Source == m.Whole[0].Source && route.Destination == "replacement" {
			t.Fatal("whole clone redundantly copied by metric")
		}
		if route.Source == s.Old.Nodes[2].ID && route.Destination == "replacement" {
			incoming = true
		}
		if route.Source == s.Old.Nodes[2].ID && route.Destination == s.New.Nodes[1].ID {
			outgoing = true
		}
	}
	if !incoming || !outgoing {
		t.Fatal("mixed ownership routes missing", m.Streams)
	}
	data, err := os.ReadFile(filepath.Join(out, "manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := ValidateResize(data); err != nil {
		t.Fatal(err)
	}
	m.Whole[0].Source = s.Old.Nodes[2].ID
	forged, _ := json.Marshal(m)
	if err := ValidateResize(forged); err == nil {
		t.Fatal("altered replacement accepted")
	}
}

func TestResizeSameIdentitySkipsInventory(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("unchanged identities should not inventory")
		http.Error(w, "unexpected", 500)
	}))
	defer server.Close()
	root := t.TempDir()
	s := fixtureSpec(root, 1)
	s.Inputs = nil
	s.Old.Nodes[0].Endpoint = server.URL
	s.New.Nodes = []member{{ID: "new", Node: "new-host:2003=000", Endpoint: "http://new-host:4242"}}
	s.Placements = map[string]Placement{"new": {Worker: "http://new-host:9080", Directory: "/data/clone", Preflight: "/data/preflight.json"}}
	path := filepath.Join(root, "spec.json")
	blob, _ := json.Marshal(s)
	os.WriteFile(path, blob, 0600)
	m, err := Resize(context.Background(), path, filepath.Join(root, "out"), true, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(m.Streams) != 0 || len(m.Whole) != 1 || len(m.Spec.Inputs) != 0 {
		t.Fatal("wrong replacement plan", m)
	}
	s.New.Nodes[0].Endpoint = server.URL
	blob, _ = json.Marshal(s)
	os.WriteFile(path, blob, 0600)
	if _, err := Resize(context.Background(), path, "", false, 1); err == nil {
		t.Fatal("replacement aliases source")
	}
}

func TestTransferInventoryPages(t *testing.T) {
	var afters []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		after := r.URL.Query().Get("after")
		afters = append(afters, after)
		var metrics []string
		if after == "" {
			metrics = []string{"a", "b"}
		} else if after == "b" {
			metrics = []string{"c"}
		}
		if metrics == nil {
			metrics = []string{}
		}
		json.NewEncoder(w).Encode(metrics)
	}))
	defer server.Close()
	v, err := openInventory(context.Background(), input{Format: "transfer", URL: server.URL}, 0, "", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer v.close()
	var names []string
	for {
		name, _, err := v.next()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			t.Fatal(err)
		}
		names = append(names, name)
	}
	if !reflect.DeepEqual(names, []string{"a", "b", "c"}) || !reflect.DeepEqual(afters, []string{"", "b", "c"}) {
		t.Fatal(names, afters)
	}
}
