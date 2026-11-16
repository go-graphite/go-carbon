package v2core

import "testing"

func TestCoreCreditBudgetIsRuntimeOnly(t *testing.T) {
	cfg := coreConfig{Shards: 1, Producers: 1, MaxRows: 16, PartitionSeconds: 3600}
	root := t.TempDir()
	t.Setenv("CORE_STREAM_BYTES", "33554432")
	e, err := openCore(root, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if e.owners[0].queues[0].limit != 32<<20 {
		t.Fatal("edge quota did not follow aggregate credit")
	}
	if e.streamBytes != 32<<20 {
		t.Fatal("byte credit ignored")
	}
	if err = e.submit(0, []row{{ID: 1, Bits: 1 << 63}}, 1, true); err != nil {
		t.Fatal(err)
	}
	if err = e.close(); err != nil {
		t.Fatal(err)
	}
	t.Setenv("CORE_STREAM_BYTES", "8388608")
	e, err = openCore(root, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if e.globalFrontier()[0] != 1 {
		t.Fatal("runtime credit changed durable frontier")
	}
	if err = e.close(); err != nil {
		t.Fatal(err)
	}
	for _, value := range []string{"bad", "0", "1073741825"} {
		t.Setenv("CORE_STREAM_BYTES", value)
		if e, err = openCore(t.TempDir(), cfg); err == nil {
			_ = e.close()
			t.Fatalf("accepted invalid byte budget %q", value)
		}
	}
}
