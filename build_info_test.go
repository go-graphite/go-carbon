package main

import (
	"runtime/debug"
	"testing"
)

func TestBuildRevision(t *testing.T) {
	const revision = "0123456789abcdef0123456789abcdef01234567"
	tests := []struct {
		name string
		info *debug.BuildInfo
		want string
	}{
		{name: "unavailable"},
		{name: "no VCS settings", info: &debug.BuildInfo{}},
		{name: "clean", info: &debug.BuildInfo{Settings: []debug.BuildSetting{
			{Key: "vcs.revision", Value: revision},
			{Key: "vcs.modified", Value: "false"},
		}}, want: "0123456789ab"},
		{name: "dirty", info: &debug.BuildInfo{Settings: []debug.BuildSetting{
			{Key: "vcs.modified", Value: "true"},
			{Key: "vcs.revision", Value: revision},
		}}, want: "0123456789ab-dirty"},
		{name: "modified without revision", info: &debug.BuildInfo{Settings: []debug.BuildSetting{
			{Key: "vcs.modified", Value: "true"},
		}}},
		{name: "short revision", info: &debug.BuildInfo{Settings: []debug.BuildSetting{
			{Key: "vcs.revision", Value: "0123456"},
		}}, want: "0123456"},
		{name: "unrelated settings", info: &debug.BuildInfo{Settings: []debug.BuildSetting{
			{Key: "GOOS", Value: "linux"},
			{Key: "vcs.revision", Value: revision},
		}}, want: "0123456789ab"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := buildRevision(tt.info); got != tt.want {
				t.Fatalf("buildRevision() = %q, want %q", got, tt.want)
			}
		})
	}
}
