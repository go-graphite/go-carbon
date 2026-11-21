package downsize

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/BurntSushi/toml"
)

const (
	defaultGoCarbonConfig = "/etc/go-carbon/go-carbon.conf"
	defaultSyncStreams    = 64
	defaultSyncProgress   = 60 * time.Second
)

// defaultJWTSecretPath returns [buckyd] auth-jwt-secret-file and the config it
// came from. An explicit configPath is the only candidate; otherwise the
// -config of each running go-carbon, then the packaged default config. With
// no candidate naming a secret it returns the built-in path and "".
func defaultJWTSecretPath(configPath, procRoot string) (secret, config string) {
	candidates := []string{configPath}
	if configPath == "" {
		candidates = append(runningGoCarbonConfigs(procRoot), defaultGoCarbonConfig)
	}
	for _, path := range candidates {
		var cfg struct {
			Buckyd struct {
				JWTSecretFile string `toml:"auth-jwt-secret-file"`
			} `toml:"buckyd"`
		}
		if data, err := os.ReadFile(path); err == nil && toml.Unmarshal(data, &cfg) == nil && cfg.Buckyd.JWTSecretFile != "" {
			return cfg.Buckyd.JWTSecretFile, path
		}
	}
	return hostJWTSecretPath, ""
}

// runningGoCarbonConfigs returns the -config argument of every process under
// procRoot whose executable name starts with go-carbon, in PID order.
func runningGoCarbonConfigs(procRoot string) []string {
	entries, err := os.ReadDir(procRoot)
	if err != nil {
		return nil
	}
	var configs []string
	for _, e := range entries {
		if _, err := strconv.Atoi(e.Name()); err != nil {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(procRoot, e.Name(), "cmdline"))
		if err != nil || len(raw) == 0 {
			continue
		}
		args := strings.Split(strings.TrimRight(string(raw), "\x00"), "\x00")
		if !strings.HasPrefix(filepath.Base(args[0]), "go-carbon") {
			continue
		}
		for i := 1; i < len(args); i++ {
			name, value, inline := strings.Cut(strings.TrimPrefix(args[i], "-"), "=")
			if name != "config" && name != "-config" {
				continue
			}
			if !inline && i+1 < len(args) {
				value = args[i+1]
			}
			if value != "" {
				configs = append(configs, value)
			}
			break
		}
	}
	return configs
}
