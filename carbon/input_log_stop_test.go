package carbon

import (
	"testing"
)

func TestInputLogOnlyConfig(t *testing.T) {
	if NewConfig().Dump.InputLogOnly {
		t.Fatal("experimental stop enabled by default")
	}
	valid := func() *Config {
		c := NewConfig()
		c.Dump.InputLogOnly, c.Dump.Enabled = true, false
		c.Dump.Path = t.TempDir()
		c.InputLog.Enabled = true
		c.Dump.HandoffSupervisor, c.Dump.StoreInputHandoff = true, true
		c.Whisper.StorageBackend = "pebble-chunk"
		c.Tags.Enabled, c.Udp.Enabled, c.Pickle.Enabled = false, false, false
		c.Carbonserver.Enabled, c.Tcp.Enabled = true, true
		return c
	}
	if err := validateInputLogOnlyConfig(valid()); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*Config){
		"no log":            func(c *Config) { c.InputLog.Enabled = false },
		"no supervisor":     func(c *Config) { c.Dump.HandoffSupervisor = false },
		"no socket handoff": func(c *Config) { c.Dump.StoreInputHandoff = false },
		"mirrored store":    func(c *Config) { c.Whisper.StoreMirrorFiles = true },
		"tags":              func(c *Config) { c.Tags.Enabled = true },
		"mixed receivers":   func(c *Config) { c.Udp.Enabled = true },
	} {
		t.Run(name, func(t *testing.T) {
			c := valid()
			mutate(c)
			if err := validateInputLogOnlyConfig(c); err == nil {
				t.Fatal("unsafe journal-only configuration accepted")
			}
		})
	}
}

func TestInputLogOnlyRequiresRestart(t *testing.T) {
	old, next := NewConfig(), NewConfig()
	next.Dump.InputLogOnly = true
	if err := inputLogReloadError(old, next); err == nil {
		t.Fatal("changed recovery mode through reload")
	}
}
