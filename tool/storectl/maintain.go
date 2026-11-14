package main

import (
	"flag"
	"fmt"
	"github.com/go-graphite/go-carbon/internal/storemaint/anchors"
	"github.com/go-graphite/go-carbon/internal/storemaint/coldpack"
	"github.com/go-graphite/go-carbon/internal/storemaint/convert"
	"github.com/go-graphite/go-carbon/internal/storemaint/foldooo"
	"github.com/go-graphite/go-carbon/internal/storemaint/migration"
	"github.com/go-graphite/go-carbon/internal/storemaint/prune"
	"github.com/go-graphite/go-carbon/internal/storemaint/reclaim"
	"os"
)

func maintain(args []string) error {
	const usage = "usage: storectl maintain migration|fold-ooo|anchors|cold-pack|convert|prune-empty-dirs|reclaim-orphans [flags]"
	if len(args) == 0 {
		return fmt.Errorf("%s", usage)
	}
	switch args[0] {
	case "--help", "-h":
		fmt.Fprintln(os.Stdout, usage)
		return flag.ErrHelp
	case "fold-ooo":
		return foldooo.Main(args[1:])
	case "migration":
		return migration.Main(args[1:])
	case "anchors":
		return anchors.Main(args[1:])
	case "cold-pack":
		return coldpack.Main(args[1:])
	case "convert":
		return convert.Main(args[1:])
	case "prune-empty-dirs":
		return prune.Main(args[1:])
	case "reclaim-orphans":
		return reclaim.Main(args[1:])
	default:
		return fmt.Errorf("unknown maintenance action %q; %s", args[0], usage)
	}
}
