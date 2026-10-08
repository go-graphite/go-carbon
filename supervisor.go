package main

import (
	"errors"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"syscall"
)

// supervisedEnv marks the worker started by runSupervisor; the inherited
// release pipe is its first extra file descriptor.
const (
	supervisedEnv = "GO_CARBON_SUPERVISED_WORKER"
	releaseFD     = 3
)

// runSupervisor starts this binary as a worker and stays its parent until the
// worker reports that a dump stop has released every listener and lock. The
// parent then exits so a service manager tracking only the main process can
// start the next instance while the kernel still reclaims the worker's memory.
// Otherwise the parent forwards signals and exits with the worker's status.
func runSupervisor() int {
	exe, err := os.Executable()
	if err != nil {
		log.Print(err)
		return 1
	}
	r, w, err := os.Pipe()
	if err != nil {
		log.Print(err)
		return 1
	}
	cmd := exec.Command(exe, os.Args[1:]...)
	cmd.Stdin, cmd.Stdout, cmd.Stderr = os.Stdin, os.Stdout, os.Stderr
	cmd.Env = append(os.Environ(), supervisedEnv+"=1")
	cmd.ExtraFiles = []*os.File{w}
	bindWorker(cmd)
	signals := make(chan os.Signal, 8)
	signal.Notify(signals, syscall.SIGUSR2, syscall.SIGHUP, syscall.SIGTERM, syscall.SIGINT)
	if err = cmd.Start(); err != nil {
		log.Print(err)
		return 1
	}
	_ = w.Close()

	released := make(chan struct{})
	go func() {
		var b [1]byte
		if n, _ := r.Read(b[:]); n == 1 {
			close(released)
		}
	}()
	exited := make(chan struct{})
	go func() {
		_ = cmd.Wait()
		close(exited)
	}()
	for {
		select {
		case s := <-signals:
			_ = cmd.Process.Signal(s)
		case <-released:
			return 0
		case <-exited:
			return exitStatus(cmd.ProcessState)
		}
	}
}

func exitStatus(state *os.ProcessState) int {
	if ws, ok := state.Sys().(syscall.WaitStatus); ok && ws.Signaled() {
		return 128 + int(ws.Signal())
	}
	return state.ExitCode()
}

// supervisedRelease returns the worker's release notification, or nil when the
// process is not supervised. It must run before any file is opened.
func supervisedRelease() (func() error, error) {
	if os.Getenv(supervisedEnv) == "" {
		return nil, nil
	}
	if err := os.Unsetenv(supervisedEnv); err != nil {
		return nil, err
	}
	pipe := os.NewFile(releaseFD, "supervisor-release")
	if pipe == nil {
		return nil, errors.New("supervisor release pipe missing")
	}
	return func() error {
		_, err := pipe.Write([]byte{1})
		return errors.Join(err, pipe.Close())
	}, nil
}
