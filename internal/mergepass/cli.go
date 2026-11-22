package mergepass

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"strings"
	"syscall"
)

const usage = `usage: storectl merge --to URL --from URL[,URL...] --state DIR [flags]

Merges every name the donors hold into the target; existing target points win
and donor-only points are added. Writes DIR/names (snapshot, once), DIR/done.log,
DIR/errors.log and DIR/report.json. A rerun with the same --state resumes.
DIR/streams, when present, sets the stream count and is read before each batch.
Exit 0 when the pass completed, even with skipped names.`

// Main parses flags and runs the pass. It returns an error only for
// configuration or state problems.
func Main(args []string) error {
	var o Options
	var from string
	flags := flag.NewFlagSet("merge", flag.ContinueOnError)
	flags.Usage = func() { os.Stderr.WriteString(usage + "\n\nflags:\n"); flags.PrintDefaults() }
	flags.StringVar(&o.To, "to", "", "target buckyd origin (http://host:4242)")
	flags.StringVar(&from, "from", "", "comma-separated donor origins, as the target's transfer-sources allowlist names them; on a conflict between donors the earlier one wins")
	flags.StringVar(&o.State, "state", "", "state directory")
	flags.IntVar(&o.Streams, "streams", 8, "concurrent batches when DIR/streams is absent")
	flags.IntVar(&o.Batch, "batch", 500, "names per request")
	flags.IntVar(&o.Limit, "limit", 0, "at most this many names (first in name order) in the snapshot; 0 for all")
	flags.StringVar(&o.After, "after", "", "snapshot only names after this one (name order), to split a donor into ranges")
	flags.Int64Var(&o.Until, "until", 0, "compare digests over [1, until) Unix seconds, e.g. the write cutover; 0 compares full history")
	flags.BoolVar(&o.NoDigest, "no-digest", false, "pull every name without the digest gate")
	flags.IntVar(&o.Sample, "sample", 1000, "names in the final parity sample")
	flags.Int64Var(&o.MaxBytes, "max-bytes", 0, "per-pull byte budget (0: target default); oversized batches are split")
	flags.StringVar(&o.JWTSecret, "jwt-secret", "/etc/go-carbon/buckyd_api_jwt_secret", "buckyd JWT secret file; empty sends no token")
	flags.IntVar(&o.Retries, "retries", 3, "attempts for a single failing name before it is logged and skipped")
	flags.DurationVar(&o.WaitOut, "wait-out", 0, "how long a restarting or overloaded daemon is waited out (default 30m)")
	if err := flags.Parse(args); err != nil {
		return err
	}
	for _, origin := range strings.Split(from, ",") {
		if origin = strings.TrimRight(strings.TrimSpace(origin), "/"); origin != "" {
			o.From = append(o.From, origin)
		}
	}
	o.To = strings.TrimRight(o.To, "/")
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	_, err := Run(ctx, o)
	if ctx.Err() != nil {
		return nil // interrupted: the done log already holds finished names
	}
	return err
}
