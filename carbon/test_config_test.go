package carbon

import (
	"bytes"
	"os"
	"testing"

	"github.com/BurntSushi/toml"
)

func testConfig(t testing.TB, root string) string {
	t.Helper()
	path := TestConfig(root)
	cfg, err := ReadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	cfg.Udp.Listen = "127.0.0.1:0"
	cfg.Tcp.Listen = "127.0.0.1:0"
	cfg.Pickle.Listen = "127.0.0.1:0"
	cfg.Grpc.Listen = "127.0.0.1:0"
	cfg.Carbonlink.Listen = "127.0.0.1:0"
	cfg.Carbonserver.Listen = "127.0.0.1:0"
	cfg.Pprof.Listen = "127.0.0.1:0"
	cfg.Buckyd.Bind = "127.0.0.1:0"
	var buf bytes.Buffer
	if err := toml.NewEncoder(&buf).Encode(cfg); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, buf.Bytes(), 0600); err != nil {
		t.Fatal(err)
	}
	return path
}
