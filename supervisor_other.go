//go:build !linux

package main

import "os/exec"

func bindWorker(*exec.Cmd) {}
