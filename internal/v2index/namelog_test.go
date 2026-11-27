package v2index

import (
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"testing"

	"github.com/lomik/zapwriter"
)

// Kill after publishing small levels and expiry, including while a base merge
// is writing and after its rename but before installation. No clean Close.
func TestNameLogKillExact(t *testing.T) {
	if dir := os.Getenv("V2INDEX_NAMELOG_DIR"); dir != "" {
		var c Catalog
		if err := c.SetDir(dir); err != nil {
			t.Fatal(err)
		}
		names := baseNames(20000, 1)
		if err := c.Publish(1, names); err != nil {
			t.Fatal(err)
		}
		if err := c.Apply(2, []string{"fresh.small"}, names[:7]); err != nil {
			t.Fatal(err)
		}
		mode := os.Getenv("V2INDEX_NAMELOG_MODE")
		if mode != "small" {
			block := make(chan struct{})
			fileHook = func(int) error { <-block; return nil }
			if err := c.Apply(3, baseNames(3000, 2), nil); err != nil {
				t.Fatal(err)
			}
			if c.job == nil {
				t.Fatal("expected merge")
			}
			if err := c.Apply(4, []string{names[0], "fresh.during"}, []string{names[8], "fresh.small"}); err != nil {
				t.Fatal(err)
			}
			if mode == "keep" {
				if err := c.Apply(5, baseNames(1000, 3), nil); err != nil {
					t.Fatal(err)
				}
				if err := c.Apply(6, baseNames(50, 4), nil); err != nil {
					t.Fatal(err)
				}
				close(block)
				<-c.job.done
				c.settle()
				if c.job != nil {
					t.Fatal("unexpected merge before expiry")
				}
				block = make(chan struct{})
				if err := c.Apply(7, nil, names[100:3100]); err != nil {
					t.Fatal(err)
				}
				if c.job == nil || len(c.Snapshot().levels) <= len(c.job.inputs) {
					t.Fatal("expected merge to keep a small level")
				}
				close(block)
				<-c.job.done
				b, err := json.Marshal(slices.Collect(c.Snapshot().Names()))
				if err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(dir, "want.json"), b, 0644); err != nil {
					t.Fatal(err)
				}
			}
			if mode == "renamed" {
				close(block)
				<-c.job.done
			}
		}
		syscall.Kill(os.Getpid(), syscall.SIGKILL)
		select {}
	}
	for _, mode := range []string{"small", "writing", "renamed", "keep"} {
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			cmd := exec.Command(os.Args[0], "-test.run=^TestNameLogKillExact$")
			cmd.Env = append(os.Environ(), "V2INDEX_NAMELOG_DIR="+dir, "V2INDEX_NAMELOG_MODE="+mode)
			out, err := cmd.CombinedOutput()
			var exit *exec.ExitError
			if !errors.As(err, &exit) || exit.Sys().(syscall.WaitStatus).Signal() != syscall.SIGKILL {
				t.Fatalf("not killed: %v %s", err, out)
			}
			names := baseNames(20000, 1)
			want := append(slices.Clone(names[7:]), "fresh.small")
			if mode != "small" {
				want = append(slices.Clone(names[9:]), names[0], names[7], "fresh.during")
				want = append(want, baseNames(3000, 2)...)
			}
			if mode == "keep" {
				b, err := os.ReadFile(filepath.Join(dir, "want.json"))
				if err != nil {
					t.Fatal(err)
				}
				if err := json.Unmarshal(b, &want); err != nil {
					t.Fatal(err)
				}
			}
			slices.Sort(want)
			var c Catalog
			if err := c.SetDir(dir); err != nil {
				t.Fatal(err)
			}
			if got := slices.Collect(c.Snapshot().Names()); !slices.Equal(got, want) {
				t.Fatalf("restart got %d names, want %d (fresh=%v expired=%v)", len(got), len(want), slices.Contains(got, "fresh.small"), slices.Contains(got, names[1]))
			}
			// A subsequent process can append and restart from the same generation.
			if err := c.Apply(2, []string{"fresh.again"}, []string{names[9]}); err != nil {
				t.Fatal(err)
			}
			c.Close()
			var d Catalog
			if err := d.SetDir(dir); err != nil {
				t.Fatal(err)
			}
			defer d.Close()
			got := slices.Collect(d.Snapshot().Names())
			if !slices.Contains(got, "fresh.again") || slices.Contains(got, names[9]) {
				t.Fatal("second restart lost append or expiry")
			}
		})
	}
}

func TestNameLogTornTail(t *testing.T) {
	dir := t.TempDir()
	var c Catalog
	if err := c.SetDir(dir); err != nil {
		t.Fatal(err)
	}
	if err := c.Publish(1, baseNames(1000, 1)); err != nil {
		t.Fatal(err)
	}
	if err := c.Apply(2, []string{"fresh.name"}, nil); err != nil {
		t.Fatal(err)
	}
	c.Close()
	p := filepath.Join(dir, "log-000001")
	f, err := os.OpenFile(p, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	f.Write([]byte{255, 255, 255})
	f.Close()
	var d Catalog
	if err := d.SetDir(dir); err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	if !slices.Contains(slices.Collect(d.Snapshot().Names()), "fresh.name") {
		t.Fatal("whole record before torn tail lost")
	}
	if err := d.Apply(2, []string{"after.tail"}, nil); err != nil {
		t.Fatal(err)
	}
	d.Close()
	var e Catalog
	if err := e.SetDir(dir); err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	if !slices.Contains(slices.Collect(e.Snapshot().Names()), "after.tail") {
		t.Fatal("tail was not truncated before append")
	}
}

func TestNameLogCorruptRebuilds(t *testing.T) {
	dir := t.TempDir()
	var c Catalog
	if err := c.SetDir(dir); err != nil {
		t.Fatal(err)
	}
	if err := c.Publish(1, baseNames(1000, 1)); err != nil {
		t.Fatal(err)
	}
	if err := c.Apply(2, []string{"fresh.name"}, nil); err != nil {
		t.Fatal(err)
	}
	c.Close()
	p := filepath.Join(dir, "log-000001")
	b, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	b[len(b)-1] ^= 1
	if err := os.WriteFile(p, b, 0644); err != nil {
		t.Fatal(err)
	}
	var d Catalog
	if err := d.SetDir(dir); err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	if d.Snapshot().Revision() != 0 {
		t.Fatal("corrupt projection served")
	}
}

func TestNameLogMissingRebuilds(t *testing.T) {
	dir := t.TempDir()
	var c Catalog
	if err := c.SetDir(dir); err != nil {
		t.Fatal(err)
	}
	if err := c.Publish(1, baseNames(1000, 1)); err != nil {
		t.Fatal(err)
	}
	c.Close()
	if err := os.Remove(filepath.Join(dir, "log-000001")); err != nil {
		t.Fatal(err)
	}
	var d Catalog
	if err := d.SetDir(dir); err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	if d.Snapshot().Revision() != 0 {
		t.Fatal("base without its log served")
	}
}

func TestNameLogWriteFailureRebuilds(t *testing.T) {
	defer zapwriter.Test()()
	dir := t.TempDir()
	var c Catalog
	if err := c.SetDir(dir); err != nil {
		t.Fatal(err)
	}
	names := baseNames(1000, 1)
	if err := c.Publish(1, names); err != nil {
		t.Fatal(err)
	}
	if err := c.log.Close(); err != nil {
		t.Fatal(err)
	}
	if err := c.Apply(2, []string{"fresh.name"}, names[:1]); err != nil {
		t.Fatal(err)
	}
	got := slices.Collect(c.Snapshot().Names())
	if !strings.Contains(zapwriter.TestString(), "name log write failed") || !slices.Contains(got, "fresh.name") || slices.Contains(got, names[0]) {
		t.Fatal("write failure damaged live view or was not reported")
	}
	if failures, _ := c.BaseFileStats(); failures != 0 || c.Err() != nil {
		t.Fatal("name log failure changed base-file error stats")
	}
	c.Close()
	var d Catalog
	if err := d.SetDir(dir); err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	if d.Snapshot().Revision() != 0 {
		t.Fatal("failed log served at restart")
	}
}
