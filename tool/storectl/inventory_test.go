package main

import (
	"errors"
	"flag"
	"testing"
)

func TestInventoryDispatch(t *testing.T) {
	for i := 0; i < 2; i++ {
		if err := run([]string{"inventory", "inputlog", "--help"}); !errors.Is(err, flag.ErrHelp) {
			t.Fatalf("help: %v", err)
		}
	}
	for _, args := range [][]string{{"inventory"}, {"inventory", "inputlog-inventory"}, {"inventory", "inputlog", "unexpected"}} {
		if err := run(args); err == nil {
			t.Fatalf("accepted %v", args)
		}
	}
}
