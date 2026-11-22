package v2daemon

import (
	"bufio"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
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
		// Drain hands the rows to core without a seal; the next publication
		// writes them to an L0 run file.
		if err = s.Producers[0].Drain(); err != nil {
			t.Fatal(err)
		}
		for deadline := time.Now().Add(10 * time.Second); ; time.Sleep(10 * time.Millisecond) {
			files, _ := filepath.Glob(filepath.Join(root, "placement-group-00000001", "core", "owner-000", "l0", "l0-*[0-9]"))
			if len(files) > 0 {
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("no L0 run file")
			}
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
	t.Logf("unsealed L0 load %s", time.Since(begin))
	checkRestartOracle(t, s, restartApps(t, 0, 20), 3)
}
