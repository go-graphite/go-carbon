package main

import (
	"context"
	"errors"
	"io"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
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

// successorBinary is the path the service manager starts, as invoked. A
// deployment that replaces the binary there is checked at stop; one that
// points the service at another binary is not (see serviceManagerBinary).
func successorBinary() string {
	if path, err := exec.LookPath(os.Args[0]); err == nil {
		return path
	}
	return os.Args[0]
}

// serviceManagerBinary returns the executable systemd starts next for the
// service this process runs in, or "" when that cannot be told. A rollback or
// canary through a unit override (a drop-in with another ExecStart) starts a
// different binary than this process was invoked as, so checking the invoked
// path for handoff support at stop answers for the wrong binary. systemd
// reports the ExecStart it will run: the loaded one, which a daemon-reload
// before the restart has already replaced.
func serviceManagerBinary() string {
	if os.Getenv("INVOCATION_ID") == "" {
		return "" // not started by systemd
	}
	data, err := os.ReadFile("/proc/self/cgroup")
	if err != nil {
		return ""
	}
	unit := cgroupUnit(string(data))
	if unit == "" {
		return ""
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, "systemctl", "show", "--property=ExecStart", "--value", "--", unit).Output()
	if err != nil {
		return ""
	}
	return execStartPath(string(out))
}

// cgroupUnit returns the systemd service named in /proc/self/cgroup content
// (cgroup v2, or v1's name=systemd hierarchy).
func cgroupUnit(cgroup string) string {
	for _, line := range strings.Split(cgroup, "\n") {
		fields := strings.SplitN(strings.TrimSpace(line), ":", 3)
		if len(fields) != 3 || (fields[1] != "" && fields[1] != "name=systemd") {
			continue
		}
		elems := strings.Split(fields[2], "/")
		for i := len(elems) - 1; i >= 0; i-- {
			if strings.HasSuffix(elems[i], ".service") {
				return elems[i]
			}
		}
	}
	return ""
}

// execStartPath returns the executable of the first command in a systemd
// ExecStart property value such as
// "{ path=/usr/bin/go-carbon ; argv[]=/usr/bin/go-carbon -config ... ; ... }".
func execStartPath(value string) string {
	const key = "path="
	i := strings.Index(value, key)
	if i < 0 {
		return ""
	}
	path := value[i+len(key):]
	if j := strings.Index(path, " ;"); j >= 0 {
		path = path[:j]
	}
	path = strings.TrimSpace(path)
	if !filepath.IsAbs(path) {
		return ""
	}
	return path
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
