package carbon

import (
	"bytes"
	"runtime"
	"runtime/pprof"
	"strings"
	"testing"
	"time"
)

func TestRuntimeProfilingReload(t *testing.T) {
	previous := runtime.SetMutexProfileFraction(-1)
	defer runtime.SetMutexProfileFraction(previous)
	defer runtime.SetBlockProfileRate(0)
	path, cfg := sharedAppConfig(t)
	if cfg.Pprof.MutexProfileFraction != 0 || cfg.Pprof.BlockProfileRate != 0 {
		t.Fatal("profiling must default off")
	}
	cfg.Pprof.MutexProfileFraction = 5
	cfg.Pprof.BlockProfileRate = 0
	writeSharedConfig(t, path, cfg)
	app := New(path)
	if err := app.ParseConfig(); err != nil {
		t.Fatal(err)
	}
	if err := app.Start(); err != nil {
		t.Fatal(err)
	}
	defer app.Stop()
	if got := runtime.SetMutexProfileFraction(-1); got != 5 {
		t.Fatalf("startup fraction %d", got)
	}
	for _, fraction := range []int{1, 100, 0} {
		cfg.Pprof.MutexProfileFraction = fraction
		cfg.Pprof.BlockProfileRate = 1
		writeSharedConfig(t, path, cfg)
		if err := app.ReloadConfig(); err != nil {
			t.Fatal(err)
		}
		if got := runtime.SetMutexProfileFraction(-1); got != fraction {
			t.Fatalf("reloaded fraction %d want %d", got, fraction)
		}
	}
	profileBlock(t, enabledProfileBlock)
	if !strings.Contains(blockProfile(t), "enabledProfileBlock") {
		t.Fatal("reload did not enable block profiling")
	}
	cfg.Pprof.BlockProfileRate = 0
	writeSharedConfig(t, path, cfg)
	if err := app.ReloadConfig(); err != nil {
		t.Fatal(err)
	}
	profileBlock(t, disabledProfileBlock)
	if strings.Contains(blockProfile(t), "disabledProfileBlock") {
		t.Fatal("reload did not disable block profiling")
	}
	cfg.Pprof.MutexProfileFraction = -1
	writeSharedConfig(t, path, cfg)
	if err := app.ReloadConfig(); err == nil {
		t.Fatal("negative profile setting accepted")
	}
	if runtime.SetMutexProfileFraction(-1) != 0 {
		t.Fatal("invalid reload changed profiling")
	}
	cfg.Pprof.MutexProfileFraction = 0
	cfg.Pprof.BlockProfileRate = -1
	if validateRuntimeProfiling(cfg) == nil {
		t.Fatal("negative block rate accepted")
	}
}

func profileBlock(t *testing.T, wait func(<-chan struct{}, chan<- struct{})) {
	t.Helper()
	blocked, ready, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	go func() { wait(blocked, ready); close(done) }()
	<-ready
	timer := time.AfterFunc(2*time.Millisecond, func() { close(blocked) })
	defer timer.Stop()
	<-done
}

//go:noinline
func enabledProfileBlock(ch <-chan struct{}, ready chan<- struct{}) { close(ready); <-ch }

//go:noinline
func disabledProfileBlock(ch <-chan struct{}, ready chan<- struct{}) { close(ready); <-ch }

func blockProfile(t *testing.T) string {
	t.Helper()
	var b bytes.Buffer
	if err := pprof.Lookup("block").WriteTo(&b, 1); err != nil {
		t.Fatal(err)
	}
	return b.String()
}
