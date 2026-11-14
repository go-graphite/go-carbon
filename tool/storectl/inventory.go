package main

import (
	"errors"
	"fmt"
	"github.com/go-graphite/go-carbon/internal/inputlog/inventory"
)

func inventoryMain(args []string) error {
	if len(args) < 1 {
		return errors.New("usage: storectl inventory inputlog [flags]")
	}
	if args[0] != "inputlog" {
		return fmt.Errorf("unknown inventory action %q; use inputlog", args[0])
	}
	return inventory.Main(args[1:])
}
