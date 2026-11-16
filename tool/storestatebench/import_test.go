package main

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"syscall"
	"testing"

	"github.com/go-graphite/go-carbon/internal/statestore"
)

func TestImportCrashResume(t *testing.T) {
	if input := os.Getenv("STORESTATE_IMPORT_TEST_INPUT"); input != "" {
		offset, err := strconv.ParseInt(os.Getenv("STORESTATE_IMPORT_TEST_OFFSET"), 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		before := os.Getenv("STORESTATE_IMPORT_TEST_BEFORE") == "true"
		err = importSpoolWithCut("sqlite", input, os.Getenv("STORESTATE_IMPORT_TEST_OUTPUT"), func(at int64, staged bool) {
			if at == offset && staged == before {
				_ = syscall.Kill(os.Getpid(), syscall.SIGKILL)
			}
		})
		t.Fatalf("child did not reach cut: %v", err)
	}
	root := t.TempDir()
	input := filepath.Join(root, "source.spool")
	f, err := os.Create(input)
	if err != nil {
		t.Fatal(err)
	}
	var boundaries []int64
	for i := 0; i < 4*4096; i++ {
		if err = writeRecord(f, statestore.Record{Bucket: "proofs", ID: uint64(i), Payload: bytes.Repeat([]byte{byte(i)}, 64)}); err != nil {
			t.Fatal(err)
		}
		if (i+1)%4096 == 0 {
			at, err := f.Seek(0, io.SeekCurrent)
			if err != nil {
				t.Fatal(err)
			}
			boundaries = append(boundaries, at)
		}
	}
	if err = f.Close(); err != nil {
		t.Fatal(err)
	}
	for _, batch := range []int{0, 1, 2} {
		for _, before := range []bool{true, false} {
			t.Run(fmt.Sprintf("batch-%d-before-%t", batch+1, before), func(t *testing.T) {
				output := filepath.Join(t.TempDir(), "state.db")
				cmd := exec.Command(os.Args[0], "-test.run=^TestImportCrashResume$")
				cmd.Env = append(os.Environ(), "STORESTATE_IMPORT_TEST_INPUT="+input, "STORESTATE_IMPORT_TEST_OUTPUT="+output, "STORESTATE_IMPORT_TEST_OFFSET="+strconv.FormatInt(boundaries[batch], 10), "STORESTATE_IMPORT_TEST_BEFORE="+strconv.FormatBool(before))
				b, err := cmd.CombinedOutput()
				exit, ok := err.(*exec.ExitError)
				if !ok {
					t.Fatalf("expected SIGKILL: %v %s", err, b)
				}
				status, ok := exit.Sys().(syscall.WaitStatus)
				if !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
					t.Fatalf("wrong cut: %v %s", exit, b)
				}
				if err = importSpool("sqlite", input, output); err != nil {
					t.Fatal(err)
				}
				// Completed imports are idempotent and retain their exact record hash.
				if err = importSpool("sqlite", input, output); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}
