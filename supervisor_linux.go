package main

import (
	"os/exec"
	"runtime"
	"syscall"
)

// bindWorker kills the worker if the supervisor dies first, e.g. on a service
// manager's stop timeout, so an orphan can never keep dumping while the next
// instance restores. After a release the worker only has to exit, so the same
// signal is harmless then. Pdeathsig follows the forking thread: keep it alive.
func bindWorker(cmd *exec.Cmd) {
	runtime.LockOSThread()
	cmd.SysProcAttr = &syscall.SysProcAttr{Pdeathsig: syscall.SIGKILL}
}
