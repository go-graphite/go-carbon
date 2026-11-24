package v2core

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"os/exec"
	"sync"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2part"
)

func TestOwnerHydrationSIGKILL(t *testing.T) {
	v2part.IDKeys.Store(true)
	defer v2part.IDKeys.Store(false)
	cfg := coreTestConfig()
	cfg.Shards = 1
	cfg.Producers = 1
	cfg.Owners = 1
	cfg.Rollups = true
	cfg.SealInterval = time.Hour
	child := os.Getenv("V2_HYDRATE_KILL_ROOT")
	hooks := Hooks{
		IDKeys: true,
		Now:    func() time.Time { return time.Unix(1, 0) },
		Expire: func(RollupPolicy, RollupMetadata, time.Time) bool { return false },
		ExpiryTTL: func(RollupPolicy, time.Time) (time.Duration, time.Time) {
			if child != "" {
				// Lazy recovery installs each metric before filing its expiry.
				// Pause with one of this owner's two metrics installed in RAM.
				fmt.Println("HYDRATING")
				select {}
			}
			return time.Hour, time.Time{}
		},
	}
	if root := child; root != "" {
		e, err := Open(root, cfg, hooks)
		if err != nil {
			t.Fatal(err)
		}
		defer e.Close()
		// No query or intake: the existing maintenance loop hydrates in background.
		select {}
	}
	root := t.TempDir()
	e, err := Open(root, cfg, hooks)
	if err != nil {
		t.Fatal(err)
	}
	for i, name := range []string{"hydrate.crash.metric", "hydrate.crash.other"} {
		_, policy := rollupTestPolicy(t, name, "1s:1d,1m:30d", sealrollup.Average, .5)
		id, order := uint64(7+i), uint64(1+i)
		if err = e.AppendBatches(0, []Batch{{ID: id, Order: order, Clock: 1794700800, Policy: policy, Points: []Row{{ID: id, Time: 1794700790, Bits: math.Float64bits(42), Seq: 1}}}}, order, true); err != nil {
			t.Fatal(err)
		}
	}
	dump := func(e *Engine) []byte {
		t.Helper()
		var out bytes.Buffer
		snap := e.DBs()[0].NewSnapshot()
		defer snap.Close()
		err := scanRollups(snap, e.identities, func(p RollupPolicy, m RollupMetadata, r []sealrollup.Record) error {
			return json.NewEncoder(&out).Encode(struct {
				P RollupPolicy
				M RollupMetadata
				R []sealrollup.Record
			}{p, m, r})
		})
		if err != nil {
			t.Fatal(err)
		}
		return out.Bytes()
	}
	want := dump(e)
	if err = e.Close(); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestOwnerHydrationSIGKILL$", "-test.timeout=30s")
	cmd.Env = append(os.Environ(), "V2_HYDRATE_KILL_ROOT="+root)
	pipe, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err = cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cmd.Process.Kill() })
	cut := make(chan bool, 1)
	go func() {
		scan := bufio.NewScanner(pipe)
		for scan.Scan() {
			if scan.Text() == "HYDRATING" {
				cut <- true
				return
			}
		}
		cut <- false
	}()
	select {
	case ok := <-cut:
		if !ok {
			_ = cmd.Wait()
			t.Fatalf("no background hydration barrier: %s", stderr.String())
		}
	case <-time.After(15 * time.Second):
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		t.Fatal("background hydration timeout")
	}
	if err = cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	if err = cmd.Wait(); err == nil {
		t.Fatal("child was not killed")
	}
	for restart := range 2 {
		e, err = Open(root, cfg, hooks)
		if err != nil {
			t.Fatal(err)
		}
		o := e.owners[0]
		if o.hydrate == nil || len(o.rollups.ids) != 0 {
			t.Fatal("ID owner was eagerly inventoried")
		}
		var wg sync.WaitGroup
		errs := make(chan error, 8)
		for range 8 {
			wg.Go(func() { errs <- o.gc.inventory.AwaitHydration(context.Background()) })
		}
		wg.Wait()
		close(errs)
		for err := range errs {
			if err != nil {
				t.Fatal(err)
			}
		}
		if len(o.rollups.ids) != 2 {
			t.Fatalf("restart %d installed %d identities", restart, len(o.rollups.ids))
		}
		if got := dump(e); !bytes.Equal(got, want) {
			t.Fatalf("restart %d changed durable samples/metadata", restart)
		}
		if err = e.Close(); err != nil {
			t.Fatal(err)
		}
	}
}
