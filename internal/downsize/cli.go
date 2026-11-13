package downsize

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"os"
	"os/signal"
	"runtime"
	"syscall"

	"github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/downsize/ringplan"
)

// Main is the operator entrypoint. Legacy commands share these same planner,
// engine, journal, worker and transfer primitives.
func Main(args []string) error {
	if len(args) == 0 {
		return errors.New("usage: resize copy|sync|plan|run|status|worker|config|compare|snapshot")
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	switch args[0] {
	case "copy":
		return PlanFreeCopyMain(ctx, args[1:])
	case "sync":
		return SyncMain(ctx, args[1:])
	case "run":
		return MoveMain(args[1:])
	case "capture-native":
		return NativeCaptureMain(ctx, args[1:])
	case "native-abandon":
		return nativeAbandonMain(ctx, args[1:], os.Stdout)
	case "bulk-eval":
		return BulkEvalMain(args[1:])
	case "snapshot":
		return CopyMain(args[1:])
	case "config", "compare":
		return ringplan.Main(append([]string{"--mode", args[0]}, args[1:]...))
	case "plan":
		flags := flag.NewFlagSet("resize plan", flag.ContinueOnError)
		from := flags.String("from", "", "native copy window start (Unix seconds or RFC3339)")
		until := flags.String("until", "", "native copy window end (exclusive)")
		spec := flags.String("spec", "", "current/desired members, hashes and target placements")
		out := flags.String("out", "", "committed local plan directory")
		apply := flags.Bool("apply", false, "write the committed plan; default only prints")
		workers := flags.Int("workers", min(4, runtime.GOMAXPROCS(0)), "parallel inventory readers")
		segment := flags.Int("segment-metrics", chunkstore.NativeMaxRangeNames, "names per plan stream segment; native ranges never exceed the receiver limit of 4096")
		names := flags.String("names", "", "plan exactly this byte-sorted unique name list (LC_ALL=C sort -u) from --source to --target instead of --spec")
		source := flags.String("source", "", "with --names: source buckyd origin, e.g. http://donor:4242")
		target := flags.String("target", "", "with --names: target buckyd origin")
		estimate := flags.Int64("estimated-bytes", ringplan.DefaultNamesEstimate, "with --names: estimated bytes PER NAME (not a total) that the importer batches by; at most 64 MiB")
		if err := flags.Parse(args[1:]); err != nil {
			return err
		}
		if *segment < 1 {
			return errors.New("--segment-metrics must be positive")
		}
		if *names != "" || *source != "" || *target != "" {
			if *spec != "" || *from != "" || *until != "" {
				return errors.New("--names plans exclude --spec and time windows")
			}
			if *estimate < 1 {
				return errors.New("--estimated-bytes must be positive")
			}
			manifest, err := ringplan.PlanNames(ctx, ringplan.NamesPlan{Names: *names, Source: *source, Target: *target, Out: *out, Apply: *apply, SegmentMetrics: *segment, EstimatedBytes: *estimate})
			if err != nil {
				return err
			}
			return json.NewEncoder(os.Stdout).Encode(manifest)
		}
		window, err := parseNativeWindow(*from, *until)
		if err != nil {
			return err
		}
		manifest, err := ringplan.ResizeSegmented(ctx, *spec, *out, *apply, *workers, *segment, window)
		if err != nil {
			return err
		}
		return json.NewEncoder(os.Stdout).Encode(manifest)
	case "status":
		flags := flag.NewFlagSet("resize status", flag.ContinueOnError)
		plan := flags.String("plan", "", "plan directory")
		state := flags.String("state", "move-state.jsonl", "shared resume journal")
		if err := flags.Parse(args[1:]); err != nil {
			return err
		}
		return Status(*plan, *state, os.Stdout)
	case "worker":
		flags := flag.NewFlagSet("resize worker", flag.ContinueOnError)
		var o workerOptions
		flags.StringVar(&o.Bind, "bind", "127.0.0.1:9080", "target worker management listener")
		flags.StringVar(&o.Target, "target", "", "desired physical member ID")
		flags.StringVar(&o.Directory, "directory", "", "fixed canonical staging directory on this host")
		flags.StringVar(&o.Preflight, "preflight", "", "target capacity/configuration preflight report")
		flags.StringVar(&o.Helper, "helper", "", "local host.py telemetry/read-isolation helper URL")
		flags.StringVar(&o.PIDFile, "pid-file", "", "publish worker PID for host telemetry")
		flags.StringVar(&o.TokenEnv, "token-env", "BUCKYD_TOKEN", "source admin JWT environment variable")
		flags.StringVar(&o.ControlEnv, "control-token-env", "MOVE_CONTROL_TOKEN", "worker/helper control secret environment variable")
		flags.BoolVar(&o.LocalLab, "local-lab", false, "loopback-only synthetic telemetry for local fixtures")
		if err := flags.Parse(args[1:]); err != nil {
			return err
		}
		return serveWorker(ctx, o)
	default:
		return errors.New("unknown resize command")
	}
}
