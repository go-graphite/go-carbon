package main

import (
	"fmt"
	"os"

	"github.com/go-graphite/go-carbon/internal/downsize"
)

func main() {
	fmt.Fprintln(os.Stderr, "store-copy is deprecated; use storectl (same commands and journals)")
	if err := downsize.MainWithMetrics(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
