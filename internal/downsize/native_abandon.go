package downsize

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"io"
	"net/http"

	"github.com/go-graphite/go-carbon/internal/chunkstore"
)

// Explicit reconciliation command: no donor, import, replan or journal rewrite.
// The old identity is never recycled; a later copy needs a fresh reviewed plan.
func nativeAbandonMain(ctx context.Context, args []string, out io.Writer) error {
	flags := flag.NewFlagSet("storectl native-abandon", flag.ContinueOnError)
	target := flags.String("target", "", "target Buckyd origin")
	identity := flags.String("identity", "", "exact legacy native combine identity (64 hex characters)")
	token := flags.String("token-env", "BUCKYD_TOKEN", "root read/update JWT environment variable")
	apply := flags.Bool("apply", false, "permanently abandon this identity; default prints without contacting target")
	if err := flags.Parse(args); err != nil {
		return err
	}
	endpoint, err := origin(*target)
	if err != nil {
		return err
	}
	if len(*identity) != 64 {
		return errors.New("--identity must contain 64 hex characters")
	}
	if _, err := hex.DecodeString(*identity); err != nil {
		return err
	}
	if !*apply {
		return json.NewEncoder(out).Encode(map[string]any{"target": endpoint, "identity": *identity, "apply": false, "action": "abandon native combine without publishing"})
	}
	client := &http.Client{Transport: &http.Transport{}, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	defer client.CloseIdleConnections()
	m := &mover{options: options{tokenEnv: *token}, client: client}
	var receipt chunkstore.NativeAbandonReceipt
	if _, err := m.requestOnce(ctx, endpoint, "/v1/native/abandon", struct{ Identity string }{*identity}, &receipt); err != nil {
		return err
	}
	if receipt.Identity != *identity || receipt.State != "abandoned" && receipt.State != "completed" {
		return errors.New("invalid native abandon receipt")
	}
	return json.NewEncoder(out).Encode(receipt)
}
