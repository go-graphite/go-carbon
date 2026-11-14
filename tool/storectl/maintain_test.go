package main

import (
	"errors"
	"flag"
	"testing"
)

func TestMaintenanceDispatch(t *testing.T) {
	// Repeated parsing in one process must not share flags or run an operation.
	for pass := 0; pass < 2; pass++ {
		for _, action := range []string{"cold-pack", "convert", "prune-empty-dirs", "reclaim-orphans", "configs"} {
			if err := run([]string{"maintain", action, "--help"}); !errors.Is(err, flag.ErrHelp) {
				t.Fatalf("%s help: %v", action, err)
			}
			if err := run([]string{"maintain", action, "unexpected"}); err == nil || errors.Is(err, flag.ErrHelp) {
				t.Fatalf("%s accepted positional argument: %v", action, err)
			}
		}
	}
}

func TestMaintenanceRejectsAliases(t *testing.T) {
	for _, action := range []string{"store-convert", "store-prune-empty-dirs", "store-reclaim-orphans", "persister_configs_differ", "unknown"} {
		if err := run([]string{"maintain", action}); err == nil {
			t.Fatalf("accepted %s", action)
		}
	}
}
