// v2-daemon measures catalog and journal layers around the numeric core.
package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/go-graphite/go-carbon/internal/v2daemon"
)

func main() {
	layer := flag.Int("layer", 0, "0=core, 1=catalog, 2=journal, 3=canonical rollups")
	root := flag.String("root", "", "fresh output directory")
	profile := flag.String("cpuprofile", "", "optional timed intake/close CPU profile")
	block := flag.String("blockprofile", "", "optional timed intake/close blocking profile")
	flag.Parse()
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
