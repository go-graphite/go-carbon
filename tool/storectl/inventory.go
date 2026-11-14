package main

import (
	"errors"
	"fmt"
	"github.com/go-graphite/go-carbon/internal/inputlog/inventory"
	"github.com/go-graphite/go-carbon/internal/sealedinventory"
)

func inventoryMain(args []string) error {
	if len(args) < 1 {
		return errors.New("usage: storectl inventory inputlog|sealed [flags]")
	}
	if args[0] == "sealed" {
		return sealedinventory.Main(args[1:])
	}
	if args[0] != "inputlog" {
		return fmt.Errorf("unknown inventory action %q; use inputlog or sealed", args[0])
	}
	return inventory.Main(args[1:])
}
