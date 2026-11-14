package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"runtime/debug"

	"github.com/go-graphite/go-carbon/internal/downsize"
)

// BuildVersion may be set by release builds; source builds use Go's VCS stamp.
var BuildVersion string

func version() string {
	if BuildVersion != "" {
		return BuildVersion
	}
	if info, ok := debug.ReadBuildInfo(); ok {
		revision := ""
		dirty := false
		for _, setting := range info.Settings {
			if setting.Key == "vcs.revision" {
				revision = setting.Value
			}
			if setting.Key == "vcs.modified" {
				dirty = setting.Value == "true"
			}
		}
		if revision != "" {
			if dirty {
				revision += "-dirty"
			}
			return revision
		}
	}
	return "devel"
}

func main() {
	if len(os.Args) == 2 && os.Args[1] == "--version" {
		fmt.Println("storectl", version())
		return
	}
	if err := run(os.Args[1:]); err != nil && !errors.Is(err, flag.ErrHelp) {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) > 0 && args[0] == "maintain" {
		return maintain(args[1:])
	}
	return downsize.MainWithMetrics(args)
}
