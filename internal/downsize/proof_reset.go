package downsize

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/go-graphite/go-carbon/internal/chunkstore"
)

// ProofResetMain defaults to a local dry run: it does not contact the host.
func ProofResetMain(ctx context.Context, args []string) error {
	flags := flag.NewFlagSet("proof-reset", flag.ContinueOnError)
	host := flags.String("host", "", "one buckyd URL")
	apply := flags.Bool("apply", false, "reset all mutation-proof history; coordinate with active verifiers first")
	secret := flags.String("jwt-secret", "", "JWT signing secret file (default: from --config)")
	config := flags.String("config", "", "local go-carbon config")
	tokenEnv := flags.String("token-env", "BUCKYD_TOKEN", "JWT environment variable; takes precedence over --jwt-secret")
	timeout := flags.Duration("timeout", 10*time.Minute, "request deadline")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 || *host == "" || *timeout <= 0 {
		return errors.New("usage: storectl proof-reset --host URL [--jwt-secret FILE | --token-env ENV] [--apply]")
	}
	target, err := origin(*host)
	if err != nil {
		return err
	}
	if !*apply {
		return json.NewEncoder(os.Stdout).Encode(map[string]any{"dry_run": true, "method": "POST", "url": target + proofMaintenancePath + "?action=reset", "body": map[string]any{}})
	}
	ctx, cancel := context.WithTimeout(ctx, *timeout)
	defer cancel()
	m := &mover{options: options{engine: "metric", tokenEnv: *tokenEnv}, client: &http.Client{Timeout: *timeout, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}
	if os.Getenv(*tokenEnv) == "" {
		if *secret == "" {
			*secret, _ = defaultJWTSecretPath(*config, "/proc")
		}
		auth := &hostTokenCache{readSecret: func() ([]byte, error) { return os.ReadFile(*secret) }}
		withSyncJWT(m.client, auth, target)
	}
	defer m.client.CloseIdleConnections()
	var receipt chunkstore.ProofResetReceipt
	if _, err := m.requestOnce(ctx, target, proofMaintenancePath+"?action=reset", struct{}{}, &receipt); err != nil {
		event("proof_reset_failed", map[string]any{"level": "ERROR", "error": err.Error()})
		return err
	}
	if receipt.Finished.IsZero() {
		return fmt.Errorf("invalid reset receipt from %s", target)
	}
	return json.NewEncoder(os.Stdout).Encode(receipt)
}
