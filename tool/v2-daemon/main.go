// v2-daemon measures catalog and journal layers around the numeric core.
package main

import (
	"flag"
	"fmt"
	"net"
	"net/http"
	_ "net/http/pprof" // Optional benchmark diagnostics, bound to loopback only.
	"os"

	"github.com/go-graphite/go-carbon/internal/v2daemon"
)

func main() {
	layer := flag.Int("layer", 0, "0=core, 1=catalog, 2=journal, 3=canonical rollups")
	root := flag.String("root", "", "fresh output directory")
	profile := flag.String("cpuprofile", "", "optional timed intake/close CPU profile")
	block := flag.String("blockprofile", "", "optional timed intake/close blocking profile")
	diagnostics := flag.String("pprof", "", "optional loopback benchmark diagnostics address")
	flag.Parse()
	if *diagnostics != "" {
		host, _, err := net.SplitHostPort(*diagnostics)
		if err != nil || net.ParseIP(host) == nil || !net.ParseIP(host).IsLoopback() {
			fmt.Fprintln(os.Stderr, "-pprof requires a loopback IP address")
			os.Exit(2)
		}
		listener, err := net.Listen("tcp", *diagnostics)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		go func() { _ = http.Serve(listener, nil) }()
	}
	if *root == "" {
		fmt.Fprintln(os.Stderr, "-root is required")
		os.Exit(2)
	}
	fixture := v2daemon.DefaultFixture()
	fixture.CPUProfile = *profile
	fixture.BlockProfile = *block
	if _, err := v2daemon.RunFixed(*root, *layer, fixture, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
