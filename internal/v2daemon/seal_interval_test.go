package v2daemon

import (
	"bufio"
	"fmt"
	"os"
	"os/exec"
	"testing"
	"time"
)

func TestSealIntervalSIGKILLUnsealedReplay(t *testing.T) {
	cfg := restartConfig(3)
	cfg.Shards = 1
	cfg.SealInterval = 10 * time.Second
	if root := os.Getenv("V2_SEAL10_CRASH_ROOT"); root != "" {
		s, err := Open(root, cfg)
		if err != nil {
			t.Fatal(err)
		}
		if err = s.Producers[0].AppendApplications(restartApps(t, 0, 20)); err != nil {
			t.Fatal(err)
		}
		// Drain fsyncs the real input log, without forcing a core seal.
		if err = s.Producers[0].Drain(); err != nil {
			t.Fatal(err)
		}
		if s.Core.Observe().Seals != 0 {
			t.Fatal("already sealed")
		}
		fmt.Println("UNSEALED_DURABLE")
		select {}
	}
	root := t.TempDir()
	cmd := exec.Command(os.Args[0], "-test.run=^TestSealIntervalSIGKILLUnsealedReplay$", "-test.timeout=20s")
	cmd.Env = append(os.Environ(), "V2_SEAL10_CRASH_ROOT="+root)
	out, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	cmd.Stderr = os.Stderr
	if err = cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cmd.Process.Kill(); _ = cmd.Wait() }()
	ready := make(chan bool, 1)
	go func() {
		scan := bufio.NewScanner(out)
		for scan.Scan() {
			if scan.Text() == "UNSEALED_DURABLE" {
				ready <- true
				return
			}
		}
		ready <- false
	}()
	select {
	case ok := <-ready:
		if !ok {
			t.Fatal("child failed")
		}
	case <-time.After(15 * time.Second):
		t.Fatal("child timeout")
	}
	if err = cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = cmd.Wait()
	begin := time.Now()
	s, err := Open(root, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	t.Logf("unsealed journal replay %s", time.Since(begin))
	checkRestartOracle(t, s, restartApps(t, 0, 20), 3)
}
