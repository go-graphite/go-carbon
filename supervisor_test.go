package main

import (
	"os"
	"testing"
	"time"
)

const testWorkerEnv = "GO_CARBON_TEST_SUPERVISED_MODE"

// TestMain lets the test binary act as the supervised worker.
func TestMain(m *testing.M) {
	if mode := os.Getenv(testWorkerEnv); mode != "" && os.Getenv(supervisedEnv) != "" {
		release, err := supervisedRelease()
		if err != nil || release == nil || os.Getenv(supervisedEnv) != "" {
			os.Exit(2)
		}
		if mode == "release" {
			if err = release(); err != nil {
				os.Exit(2)
			}
			// Still tearing down: the supervisor must not wait for this.
			time.Sleep(5 * time.Second)
		}
		os.Exit(3)
	}
	os.Exit(m.Run())
}

func TestSupervisorExitsOnRelease(t *testing.T) {
	t.Setenv(testWorkerEnv, "release")
	started := time.Now()
	if code := runSupervisor(); code != 0 {
		t.Fatal("release status", code)
	}
	if elapsed := time.Since(started); elapsed > 4*time.Second {
		t.Fatal("supervisor waited for worker exit", elapsed)
	}
}

func TestSupervisorPropagatesWorkerStatus(t *testing.T) {
	t.Setenv(testWorkerEnv, "exit")
	if code := runSupervisor(); code != 3 {
		t.Fatal("worker status", code)
	}
}

func TestUnsupervisedWorkerHasNoRelease(t *testing.T) {
	release, err := supervisedRelease()
	if err != nil || release != nil {
		t.Fatal("unexpected release", err)
	}
}
