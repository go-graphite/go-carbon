package downsize

import (
	"os"
	"time"

	"github.com/BurntSushi/toml"
)

const (
	defaultGoCarbonConfig = "/etc/go-carbon/go-carbon.conf"
	defaultSyncStreams    = 64
	defaultSyncProgress   = 60 * time.Second
)

// defaultJWTSecretPath returns [buckyd] auth-jwt-secret-file from the local
// go-carbon config, or the built-in path when the config or key is missing.
func defaultJWTSecretPath(configPath string) string {
	var cfg struct {
		Buckyd struct {
			JWTSecretFile string `toml:"auth-jwt-secret-file"`
		} `toml:"buckyd"`
	}
	if data, err := os.ReadFile(configPath); err == nil && toml.Unmarshal(data, &cfg) == nil && cfg.Buckyd.JWTSecretFile != "" {
		return cfg.Buckyd.JWTSecretFile
	}
	return hostJWTSecretPath
}
