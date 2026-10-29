package main

import (
	"fmt"
	"os"

	"github.com/go-graphite/go-carbon/internal/downsize/ringplan"
)

func main() {
	if err := ringplan.Main(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
