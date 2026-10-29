package main

import (
	"context"
	"expvar"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	_ "net/http/pprof" // skipcq: GO-S2108
	"os"
	"os/signal"
	"os/user"
	"regexp"
	"runtime"
	"strconv"
	"syscall"
	"time"

	"github.com/lomik/zapwriter"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	daemon "github.com/sevlyar/go-daemon"

	"go.uber.org/zap"

	"github.com/go-graphite/go-carbon/carbon"
	"github.com/go-graphite/go-carbon/carbonserver"
	"github.com/go-graphite/go-carbon/internal/handoff"
	"github.com/go-graphite/go-carbon/points"
)

// Version of go-carbon
const Version = "0.19.1"

var BuildVersion = "(development version)"

func httpServe(addr string) (func(), error) {
	tcpAddr, err := net.ResolveTCPAddr("tcp", addr)
	if err != nil {
		return nil, err
	}

	listener, err := net.ListenTCP("tcp", tcpAddr)
	if err != nil {
		return nil, err
	}

	server := &http.Server{
		// TODO: set read timeout
	}

	go server.Serve(listener)
	return func() { listener.Close() }, nil
}

func main() {
	var err error

	/* CONFIG start */

	configFile := flag.String("config", "", "Filename of config")
	printDefaultConfig := flag.Bool("config-print-default", false, "Print default config")
	checkConfig := flag.Bool("check-config", false, "Check config and exit")

	// Scan metrics to check if there are mismatches against schema and aggregation configs.
	checkPolicies := flag.Int("check-policies", 0, "Scan all whisper files for inconsistencies of schema and aggregation policies. Triggered with a positive value. The positive value is used for specifying the check speed (unit: metrics per second). Example value: 120.")
	printInconsistentMetrics := flag.Bool("print-inconsistent-metrics", false, "Prints metric path with inconsistent configs, Used with -check-policies.")

	printVersion := flag.Bool("version", false, "Print version")

	isDaemon := flag.Bool("daemon", false, "Run in background")
	pidfile := flag.String("pidfile", "", "Pidfile path (only for daemon)")

	cat := flag.String("cat", "", "Print cache dump file")
	printFLC := flag.String("print-file-list-cache", "", "Print file list cache. (format: $path_to_cache. example: /var/lib/carbon/carbonserver-file-list-cache.bin)")
	handoffProtocol := flag.Bool("handoff-protocol", false, "Print the supported read handoff protocol and exit")
	prepareStore := flag.String("prepare-shared-store", "", "absolute verified-finalizer executable; requires a claimed predecessor handoff and mirrored exact storage")
	inputHandoffProtocol := flag.Bool("input-handoff-protocol", false, "Print the supported input handoff protocol and exit")
	sharedStoreHandoffProtocol := flag.Bool("shared-store-handoff-protocol", false, "Print the supported mirrored storage handoff protocol and exit")
	storeInputHandoffProtocol := flag.Bool("store-input-handoff-protocol", false, "Print the supported shared-store input handoff protocol and exit")

	flag.Parse()

	if *handoffProtocol {
		fmt.Println(handoff.Protocol)
		return
	}
	if *inputHandoffProtocol {
		fmt.Println(handoff.InputProtocol)
		return
	}
	if *storeInputHandoffProtocol {
		fmt.Println(handoff.StoreInputProtocol)
		return
	}
	if *sharedStoreHandoffProtocol {
		probe := carbon.New(*configFile)
		if err := probe.ParseConfig(); err == nil {
			cfg := probe.Config
			if cfg.Whisper.StoreMirrorFiles && cfg.Whisper.StoreLiveMigration && cfg.Dump.Enabled && !cfg.Tags.Enabled {
				fmt.Println(handoff.SharedStoreProtocol)
			}
		}
		return
	}

	release, err := supervisedRelease()
	if err != nil {
		log.Fatal(err)
	}

	if *printVersion {
		fmt.Println(Version)
		fmt.Println(BuildVersion)
		return
	}

	if *cat != "" {
		err = points.ReadFromFile(*cat, func(p *points.Points) {
			for _, d := range p.Data { // every metric point
				fmt.Printf("%s %v %v\n", p.Metric, d.Value, d.Timestamp)
			}
		})
		if err != nil {
			log.Fatal(err)
		}
		return
	}

	if *printFLC != "" {
		path := *printFLC
		if err := carbonserver.ReadFileListCache(path, carbonserver.FLCVersionUnspecified, os.Stdout); err != nil {
			fmt.Fprintf(os.Stderr, "failed to read flc cache: %s\n", err.Error())
		}

		return
	}

	if *printDefaultConfig {
		if err = carbon.PrintDefaultConfig(); err != nil {
			log.Fatal(err)
		}
		return
	}

	app := carbon.New(*configFile)

	if err = app.ParseConfig(); err != nil {
		log.Fatal(err)
	}

	cfg := app.Config

	var runAsUser *user.User
	if cfg.Common.User != "" {
		runAsUser, err = user.Lookup(cfg.Common.User)
		if err != nil {
			log.Fatal(err)
		}
	}

	// config parsed successfully. Exit in check-only mode
	if *checkConfig {
		return
	}

	if *checkPolicies > 0 {
		app.CheckPersisterPolicyConsistencies(*checkPolicies, *printInconsistentMetrics)
		return
	}

	if cfg.Dump.HandoffSupervisor && release == nil {
		if *isDaemon {
			log.Fatal("dump.handoff-supervisor is not supported with -daemon")
		}
		os.Exit(runSupervisor())
	}
	app.RegisterReadHandoff()

	for i := 0; i < len(cfg.Logging); i++ {
		if err := zapwriter.PrepareFileForUser(cfg.Logging[i].File, runAsUser); err != nil {
			log.Fatal(err)
		}
	}

	if err = zapwriter.ApplyConfig(cfg.Logging); err != nil {
		log.Fatal(err)
	}

	mainLogger := zapwriter.Logger("main")

	if *isDaemon {
		runtime.LockOSThread()

		context := new(daemon.Context)
		if *pidfile != "" {
			context.PidFileName = *pidfile
			context.PidFilePerm = 0644
		}

		if runAsUser != nil {
			uid, err := strconv.ParseUint(runAsUser.Uid, 10, 32)
			if err != nil {
				mainLogger.Fatal(err.Error())
			}

			gid, err := strconv.ParseUint(runAsUser.Gid, 10, 32)
			if err != nil {
				mainLogger.Fatal(err.Error())
			}

			context.Credential = &syscall.Credential{
				Uid: uint32(uid),
				Gid: uint32(gid),
			}
		}

		child, _ := context.Reborn()

		if child != nil {
			return
		}
		defer context.Release()

		runtime.UnlockOSThread()
	}
	/* CONFIG end */

	// pprof
	httpStop := func() {}
	if cfg.Pprof.Enabled || cfg.Prometheus.Enabled {
		httpStop, err = httpServe(cfg.Pprof.Listen)
		if err != nil {
			mainLogger.Fatal(err.Error())
		}
	}

	if cfg.Pprof.Enabled {
		expvar.NewString("GoVersion").Set(runtime.Version())
		expvar.NewString("BuildVersion").Set(BuildVersion)
		expvar.NewString("Version").Set(Version)
		expvar.Publish("Config", expvar.Func(func() interface{} { return cfg }))
		expvar.Publish("GoroutineCount", expvar.Func(func() interface{} { return runtime.NumGoroutine() }))
	}

	if cfg.Prometheus.Enabled {
		buildInfo := prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "go_carbon_build_info",
			Help: "Go-carbon release, binary build, and Go toolchain versions.",
			ConstLabels: prometheus.Labels{
				"version":       Version,
				"build_version": BuildVersion,
				"go_version":    runtime.Version(),
			},
		})
		buildInfo.Set(1)
		app.PromRegisterer.MustRegister(
			buildInfo,
			collectors.NewBuildInfoCollector(),
			collectors.NewGoCollector(
				collectors.WithGoCollectorRuntimeMetrics(collectors.GoRuntimeMetricsRule{Matcher: regexp.MustCompile("/.*")}),
			),
			collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
		http.Handle(cfg.Prometheus.Endpoint, promhttp.HandlerFor(app.PromRegistry, promhttp.HandlerOpts{}))
	}
	// Register before startup: restore/index warmup can take minutes. A HUP or
	// USR2 received then must wait for startup instead of taking the default
	// action and terminating the process in the middle of replay.
	stopSignals := make(chan os.Signal, 1)
	reloadSignals := make(chan os.Signal, 1)
	signal.Notify(stopSignals, syscall.SIGUSR2)
	signal.Notify(reloadSignals, syscall.SIGHUP)
	defer signal.Stop(stopSignals)
	defer signal.Stop(reloadSignals)
	if *prepareStore != "" {
		prepareContext, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
		err = app.PrepareSharedStoreForHandoff(prepareContext, *prepareStore)
		cancel()
		if err != nil {
			mainLogger.Fatal("shared storage preparation failed", zap.Error(err))
		}
	}
	if err = app.Start(); err != nil {
		mainLogger.Fatal(err.Error())
	} else {
		mainLogger.Info("started")
	}

	// A supervisor may let the next instance start once this one holds no
	// listener, lock, or unsynchronized file other than the sockets it hands
	// over: reads, and input with its locked tail log when the next instance
	// takes input too. DumpStop returns early with listeners open on failure.
	var released bool
	releaseToSupervisor := func() {
		httpStop()
		mainLogger.Info("releasing to supervisor")
		if err := release(); err != nil {
			mainLogger.Error("supervisor release failed", zap.Error(err))
		}
		released = true
	}
	if release != nil && cfg.Dump.Enabled {
		// DumpStop switches mirrored readers to the original files and closes
		// the shared handle before releasing its exclusive lock to a successor.
		app.SetReadHandoff(successorBinary(), releaseToSupervisor)
		app.SetSuccessorResolver(serviceManagerBinary)
	}
	go func() {
		for {
			<-stopSignals
			if err := app.DumpStop(); err == nil && release != nil && cfg.Dump.Enabled && !released {
				app.ReleaseForHandoff()
				releaseToSupervisor()
			}
			app.RecordStopExit()
			os.Exit(0)
		}
	}()

	go func() {
		for {
			<-reloadSignals
			mainLogger.Info("HUP received. Reload config")
			if err := app.ReloadConfig(); err != nil {
				mainLogger.Error("config reload failed", zap.Error(err))
			} else {
				mainLogger.Info("config successfully reloaded")
			}
		}
	}()

	app.Loop()

	mainLogger.Info("stopped")
}
