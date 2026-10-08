package main

import (
	"errors"
	"io"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
)

// supervisedEnv marks the worker started by runSupervisor. Its first extra file
// descriptor is the release pipe, the second the supervisor's lifeline.
const (
	supervisedEnv = "GO_CARBON_SUPERVISED_WORKER"
	releaseFD     = 3
	lifelineFD    = 4
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
	// The worker sees EOF on the lifeline only when this process is gone.
	lifeline, keep, err := os.Pipe()
	if err != nil {
		log.Print(err)
		return 1
	}
	defer keep.Close()
	cmd := exec.Command(exe, os.Args[1:]...)
	// Keep the invoked path: at stop the worker checks the binary there, which
	// a deployment may have replaced, for handoff support.
	cmd.Args[0] = os.Args[0]
	cmd.Stdin, cmd.Stdout, cmd.Stderr = os.Stdin, os.Stdout, os.Stderr
	cmd.Env = append(os.Environ(), supervisedEnv+"=1")
	cmd.ExtraFiles = []*os.File{w, lifeline}
	signals := make(chan os.Signal, 8)
	signal.Notify(signals, syscall.SIGUSR2, syscall.SIGHUP, syscall.SIGTERM, syscall.SIGINT)
	if err = cmd.Start(); err != nil {
		log.Print(err)
		return 1
	}
	_ = w.Close()
	_ = lifeline.Close()

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

// successorBinary is the path the service manager starts, as invoked.
func successorBinary() string {
	if path, err := exec.LookPath(os.Args[0]); err == nil {
		return path
	}
	return os.Args[0]
}

// supervisedRelease returns the worker's release notification, or nil when the
// process is not supervised. It must run before any file is opened.
//
// Until released, the worker kills itself as soon as the supervisor is gone,
// e.g. after a service manager's stop-timeout SIGKILL, so an orphan can never
// keep dumping while the next instance restores. After release the worker may
// outlive the supervisor: it then only serves frozen reads or exits.
func supervisedRelease() (func() error, error) {
	if os.Getenv(supervisedEnv) == "" {
		return nil, nil
	}
	if err := os.Unsetenv(supervisedEnv); err != nil {
		return nil, err
	}
	pipe, lifeline := os.NewFile(releaseFD, "supervisor-release"), os.NewFile(lifelineFD, "supervisor-lifeline")
	if pipe == nil || lifeline == nil {
		return nil, errors.New("supervisor pipes missing")
	}
	var released atomic.Bool
	go func() {
		_, _ = io.Copy(io.Discard, lifeline)
		if !released.Load() {
			_ = syscall.Kill(os.Getpid(), syscall.SIGKILL)
		}
	}()
	var once sync.Once
	var err error
	return func() error {
		once.Do(func() {
			released.Store(true)
			_, err = pipe.Write([]byte{1})
			err = errors.Join(err, pipe.Close())
		})
		return err
	}, nil
}
