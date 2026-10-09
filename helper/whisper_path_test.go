package helper

import (
	"strings"
	"testing"
)

func TestWhisperPathFits(t *testing.T) {
	name := strings.Repeat("n", MaxWhisperFilenameLength)
	deep := "/" + strings.Repeat("d/", MaxWhisperPathLength)
	for _, tc := range []struct {
		path string
		want bool
	}{
		{"", true},
		{"/var/lib/carbon/whisper/a/b.wsp", true},
		{"/root/" + name + "/b.wsp", true},
		{"/root/" + name + "x/b.wsp", false},
		{"/root/" + name[:MaxWhisperFilenameLength-4] + ".wsp", true},
		{"/root/" + name[:MaxWhisperFilenameLength-3] + ".wsp", false},
		{deep[:MaxWhisperPathLength], true},
		{deep[:MaxWhisperPathLength+1], false},
	} {
		if got := WhisperPathFits(tc.path); got != tc.want {
			t.Errorf("WhisperPathFits(%d bytes ending %q) = %t, want %t", len(tc.path), tc.path[max(0, len(tc.path)-12):], got, tc.want)
		}
	}
}
