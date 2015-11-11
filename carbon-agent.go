package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"os/user"
	"runtime"
	"strconv"
	"syscall"

	"github.com/Sirupsen/logrus"
	"github.com/lomik/go-carbon/carbon"
	"github.com/lomik/go-carbon/logging"
	"github.com/lomik/go-daemon"
)

import _ "net/http/pprof"

// Version of go-carbon
const Version = "0.6"

func main() {
	var err error

	/* CONFIG start */

	configFile := flag.String("config", "", "Filename of config")
	printDefaultConfig := flag.Bool("config-print-default", false, "Print default config")
	checkConfig := flag.Bool("check-config", false, "Check config and exit")

	printVersion := flag.Bool("version", false, "Print version")

	isDaemon := flag.Bool("daemon", false, "Run in background")
	pidfile := flag.String("pidfile", "", "Pidfile path (only for daemon)")

	flag.Parse()

	if *printVersion {
		fmt.Print(Version)
		return
	}

	if *printDefaultConfig {
		if err = carbon.PrintConfig(carbon.NewConfig()); err != nil {
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

	if err := logging.SetLevel(cfg.Common.LogLevel); err != nil {
		log.Fatal(err)
	}

	// config parsed successfully. Exit in check-only mode
	if *checkConfig {
		return
	}

	if err := logging.PrepareFile(cfg.Common.Logfile, runAsUser); err != nil {
		logrus.Fatal(err)
	}

	if err := logging.SetFile(cfg.Common.Logfile); err != nil {
		logrus.Fatal(err)
	}

	if *isDaemon {
		runtime.LockOSThread()

		context := new(daemon.Context)
		if *pidfile != "" {
			context.PidFileName = *pidfile
			context.PidFilePerm = 0644
		}

		if runAsUser != nil {
			uid, err := strconv.ParseInt(runAsUser.Uid, 10, 0)
			if err != nil {
				log.Fatal(err)
			}

			gid, err := strconv.ParseInt(runAsUser.Gid, 10, 0)
			if err != nil {
				log.Fatal(err)
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

	runtime.GOMAXPROCS(cfg.Common.MaxCPU)

	/* CONFIG end */

	// pprof
	if cfg.Pprof.Enabled {
		go func() {

			// c := make(chan os.Signal, 1)
			// signal.Notify(c, syscall.SIGSTOP)
			// @todo: stop

			logrus.Fatal(http.ListenAndServe(cfg.Pprof.Listen, nil))
		}()
	}

	if err = app.Start(); err != nil {
		logrus.Fatal(err)
	} else {
		logrus.Info("started")
	}

	go func() {
		c := make(chan os.Signal, 1)
		signal.Notify(c, syscall.SIGUSR2)
		<-c
		app.GraceStop()
	}()

	app.Loop()

	logrus.Info("stopped")
}
