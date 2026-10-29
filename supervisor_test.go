package main

import (
	"io"
	"net"
	"os"
	"testing"
	"time"
)

const testWorkerEnv = "GO_CARBON_TEST_SUPERVISED_MODE"
const testWorkerGateEnv = "GO_CARBON_TEST_SUPERVISED_GATE"

// TestMain lets the test binary act as the supervised worker.
func TestMain(m *testing.M) {
	if mode := os.Getenv(testWorkerEnv); mode != "" && os.Getenv(supervisedEnv) != "" {
		release, err := supervisedRelease()
		if err != nil || release == nil || os.Getenv(supervisedEnv) != "" {
			os.Exit(2)
		}
		if mode == "release" {
			gate, err := net.DialTimeout("tcp", os.Getenv(testWorkerGateEnv), time.Second)
			if err != nil {
				os.Exit(2)
			}
			if err = release(); err != nil {
				os.Exit(2)
			}
			// Stay alive until the parent observes the supervisor's return.
			_ = gate.SetReadDeadline(time.Now().Add(5 * time.Second))
			_, _ = io.Copy(io.Discard, gate)
			_ = gate.Close()
		}
		os.Exit(3)
	}
	os.Exit(m.Run())
}

func TestSupervisorExitsOnRelease(t *testing.T) {
	gate, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer gate.Close()
	t.Setenv(testWorkerGateEnv, gate.Addr().String())
	t.Setenv(testWorkerEnv, "release")
	started := time.Now()
	if code := runSupervisor(); code != 0 {
		t.Fatal("release status", code)
	}
	if elapsed := time.Since(started); elapsed > 4*time.Second {
		t.Fatal("supervisor waited for worker exit", elapsed)
	}
	_ = gate.(*net.TCPListener).SetDeadline(time.Now().Add(time.Second))
	worker, err := gate.Accept()
	if err != nil {
		t.Fatal(err)
	}
	worker.Close()
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

func TestServiceManagerBinaryParsing(t *testing.T) {
	for _, tc := range []struct{ cgroup, unit string }{
		{"0::/system.slice/go-carbon.service\n", "go-carbon.service"},
		{"12:memory:/system.slice/other.service\n1:name=systemd:/system.slice/go-carbon.service\n0::/\n", "go-carbon.service"},
		{"0::/system.slice/go-carbon.service/worker\n", "go-carbon.service"},
		{"0::/user.slice/user-1000.slice/session-2.scope\n", ""},
	} {
		if got := cgroupUnit(tc.cgroup); got != tc.unit {
			t.Errorf("cgroupUnit(%q) = %q, want %q", tc.cgroup, got, tc.unit)
		}
	}
	for _, tc := range []struct{ value, path string }{
		{"{ path=/var/lib/carbon/bin/go-carbon-candidate ; argv[]=/var/lib/carbon/bin/go-carbon-candidate -config /etc/go-carbon/go-carbon.conf ; ignore_errors=no ; start_time=[n/a] ; stop_time=[n/a] ; pid=0 ; code=(null) ; status=0/0 }\n", "/var/lib/carbon/bin/go-carbon-candidate"},
		{"", ""},
		{"{ path=go-carbon ; argv[]=go-carbon }", ""},
	} {
		if got := execStartPath(tc.value); got != tc.path {
			t.Errorf("execStartPath(%q) = %q, want %q", tc.value, got, tc.path)
		}
	}
}
