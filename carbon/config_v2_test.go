package carbon

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/BurntSushi/toml"
)

func TestV2ConfigDecodeAndRestartFence(t *testing.T) {
	file := filepath.Join(t.TempDir(), "go-carbon.conf")
	body := `[v2]
enabled = false
data-dir = "/tmp/immutable-parts"
shards = 7
owners = 13
part-interval = "250ms"
journal-codec = "pdc3"
write-engine = "standalone"
`
	if err := os.WriteFile(file, []byte(body), 0600); err != nil {
		t.Fatal(err)
	}
	c, err := ReadConfig(file)
	if err != nil {
		t.Fatal(err)
	}
	if c.V2.WriteEngine != "standalone" || c.V2.JournalCodec != "pdc3" || len(c.unknownKeys) != 0 || c.V2.Shards != 7 || c.V2.Owners != 13 || c.V2.PartInterval.Value() != 250*time.Millisecond {
		t.Fatalf("decode: %+v, unknown %v", c.V2, c.unknownKeys)
	}
	for name, change := range map[string]func(*Config){
		"toggle":    func(n *Config) { n.V2.Enabled = true },
		"directory": func(n *Config) { n.V2.DataDir += "/new" },
		"shards":    func(n *Config) { n.V2.Shards++ },
		"owners":    func(n *Config) { n.V2.Owners++ },
		"interval":  func(n *Config) { n.V2.PartInterval.Duration++ },
		"codec":     func(n *Config) { n.V2.JournalCodec = "pdx1" },
		"engine":    func(n *Config) { n.V2.WriteEngine = "core" },
	} {
		t.Run(name, func(t *testing.T) {
			n := *c
			change(&n)
			if err := v2ReloadError(c, &n); err == nil {
				t.Fatal("restart-only setting accepted by reload")
			}
		})
	}
	if NewConfig().V2.Enabled || NewConfig().V2.Owners < 1 || NewConfig().V2.Shards < 1 {
		t.Fatal("unsafe defaults")
	}
}

func TestV2ReloadLeavesRunningBackendAndConfig(t *testing.T) {
	if isolatedTestProcess(t) {
		return
	}
	app := v2Fixture(t, &oracleBackend{})
	write := func(c *Config) {
		var body bytes.Buffer
		if err := toml.NewEncoder(&body).Encode(c); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(app.ConfigFilename, body.Bytes(), 0600); err != nil {
			t.Fatal(err)
		}
	}
	write(app.Config)
	if err := app.Start(); err != nil {
		t.Fatal(err)
	}
	before, backend := app.Config, app.v2
	next := *before
	next.V2.Enabled = false
	write(&next)
	if err := app.ReloadConfig(); err == nil || !strings.Contains(err.Error(), "restart") {
		t.Fatal("toggle accepted", err)
	}
	if app.Config != before || app.v2 != backend {
		t.Fatal("failed reload changed running state")
	}
	next = *before
	next.Common.MetricInterval = &Duration{2 * time.Minute}
	write(&next)
	if err := app.ReloadConfig(); err != nil {
		t.Fatal("safe reload failed", err)
	}
	if app.v2 != backend || app.Config.Common.MetricInterval.Value() != 2*time.Minute {
		t.Fatal("safe reload replaced storage or lost settings")
	}
}

func TestV2Validation(t *testing.T) {
	c := NewConfig()
	c.V2.Enabled = true
	c.Dump.Enabled = false
	if err := validateV2Config(c); err != nil {
		t.Fatal(err)
	}
	for name, change := range map[string]func(*Config){
		"relative":       func(n *Config) { n.V2.DataDir = "parts" },
		"root":           func(n *Config) { n.V2.DataDir = "/" },
		"legacy":         func(n *Config) { n.V2.DataDir = n.Whisper.DataDir },
		"zero shards":    func(n *Config) { n.V2.Shards = 0 },
		"zero owners":    func(n *Config) { n.V2.Owners = 0 },
		"zero interval":  func(n *Config) { n.V2.PartInterval = Duration{} },
		"unknown engine": func(n *Config) { n.V2.WriteEngine = "unknown" },
		"core geometry":  func(n *Config) { n.V2.WriteEngine = "core"; n.V2.Owners = n.V2.Shards + 1 },
		"unknown codec":  func(n *Config) { n.V2.JournalCodec = "unknown" },
		"legacy log":     func(n *Config) { n.InputLog.Enabled = true },
		"legacy dump":    func(n *Config) { n.Dump.Enabled = true },
		"handoff":        func(n *Config) { n.Dump.HandoffSupervisor = true },
		"tags":           func(n *Config) { n.Tags.Enabled = true },
		"copies":         func(n *Config) { n.Buckyd.Enabled = true },
	} {
		t.Run(name, func(t *testing.T) {
			n := *c
			change(&n)
			if err := validateV2Config(&n); err == nil {
				t.Fatal("unsafe configuration accepted")
			}
		})
	}
}
