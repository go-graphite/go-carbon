package main

import (
	"fmt"
	"os"

	"github.com/go-graphite/go-carbon/internal/downsize"
)

func main() {
	if err := downsize.CopyMain(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
