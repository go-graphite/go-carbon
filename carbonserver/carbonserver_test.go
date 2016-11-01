package carbonserver

import (
	"reflect"
	"testing"

	"github.com/dgryski/go-trigram"
)

func TestExtractTrigrams(t *testing.T) {

	tests := []struct {
		query string
		want  []string
	}{
		{"foo.bar.baz", []string{"foo", "oo.", "o.b", ".ba", "bar", "ar.", "r.b", "baz"}},
		{"foo.*.baz", []string{"foo", "oo.", ".ba", "baz"}},
		{"foo.bar[12]qux.*", []string{"foo", "oo.", "o.b", ".ba", "bar", "qux", "ux."}},
		{"foo.bar*.5*.qux.*", []string{"foo", "oo.", "o.b", ".ba", "bar", ".qu", "qux", "ux."}},
		{"foob[arzf", []string{"foo", "oob"}},
	}

	for _, tt := range tests {
		got := extractTrigrams(tt.query)
		var w []trigram.T
		for _, v := range tt.want {
			tri := trigram.T(v[0])<<16 | trigram.T(v[1])<<8 | trigram.T(v[2])
			w = append(w, tri)
		}
		if !reflect.DeepEqual(got, w) {
			t.Errorf("extractTrigrams(%q)=%q, want %#v\n", tt.query, got, tt.want)
		}
	}
}
