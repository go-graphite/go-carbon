package carbon

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/BurntSushi/toml"
)

func TestReadableSize(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		input string
		bytes bool
		want  uint64
		bad   bool
	}{
		{"0", false, 0, false}, {"120000000000", false, 120_000_000_000, false},
		{`"120G"`, false, 120_000_000_000, false}, {`"12K"`, false, 12_000, false},
		{`"12M"`, false, 12_000_000, false}, {`"12T"`, false, 12_000_000_000_000, false},
		{`"123"`, false, 123, false}, {`"0G"`, false, 0, false},
		{`"12K"`, true, 12_000, false}, {`"12M"`, true, 12_000_000, false}, {`"12G"`, true, 12_000_000_000, false},
		{`"12KiB"`, true, 12 << 10, false}, {`"12MiB"`, true, 12 << 20, false}, {`"12GiB"`, true, 12 << 30, false},
		{`"1KiB"`, false, 0, true}, {`"1MiB"`, false, 0, true}, {`"1GiB"`, false, 0, true},
		{"-1", false, 0, true}, {`"-1"`, false, 0, true}, {`""`, false, 0, true},
		{`"12GB"`, true, 0, true}, {`"1.5G"`, true, 0, true}, {`"12m"`, false, 0, true},
		{`" 12M"`, false, 0, true}, {`"12 M"`, false, 0, true}, {`"+12M"`, false, 0, true},
		{`"1e6"`, false, 0, true}, {`"1_000"`, false, 0, true}, {`"M"`, false, 0, true},
		{`"9223372036854775808"`, false, 0, true}, {`"9223372037G"`, false, 0, true},
		{`"8589934592GiB"`, true, 0, true}, {"true", false, 0, true}, {"1.5", false, 0, true}, {"[]", false, 0, true},
		{"9223372036854775807", false, 9223372036854775807, false},
	} {
		t.Run(fmt.Sprintf("%s/bytes=%t", tc.input, tc.bytes), func(t *testing.T) {
			var got uint64
			view := struct{ Size readableSize }{readableSize{target: &got, bytes: tc.bytes}}
			_, err := toml.Decode("Size = "+tc.input, &view)
			if (err != nil) != tc.bad {
				t.Fatalf("decode error=%v; bad=%t", err, tc.bad)
			}
			if !tc.bad && got != tc.want {
				t.Fatalf("got %d, want %d", got, tc.want)
			}
		})
	}
	// Signed integer sentinels keep the behavior of the original integer field.
	var signed int64
	decoder := readableSize{target: &signed, bytes: true}
	if err := decoder.UnmarshalTOML(int64(-1)); err != nil || signed != -1 {
		t.Fatalf("signed sentinel: %d, %v", signed, err)
	}
	var count int
	decoder = readableSize{target: &count}
	if err := decoder.UnmarshalTOML("12M"); err != nil || count != 12_000_000 {
		t.Fatalf("int count: %d, %v", count, err)
	}
}

func TestReadableConfig(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct{ integer, readable string }{
		{"[cache]\nmax-size=120000000000\nmax-bytes=2147483648\nbloom-size=1000000\nwriteout-min-points=12000\n", "[cache]\nmax-size='120G'\nmax-bytes='2GiB'\nbloom-size='1M'\nwriteout-min-points='12K'\n"},
		{"[whisper]\nstore-cache-size=2147483648\nstore-memtable-size=67108864\nstore-compaction-rate=-1\n", "[whisper]\nstore-cache-size='2GiB'\nstore-memtable-size='64MiB'\nstore-compaction-rate=-1\n"},
		{"[carbonserver]\nmax-metrics-globbed=12000000\nmax-metrics-rendered=1000000\n", "[carbonserver]\nmax-metrics-globbed='12M'\nmax-metrics-rendered='1M'\n"},
		{"[dump]\nrestore-per-second=12000\n", "[dump]\nrestore-per-second='12K'\n"},
	} {
		path := filepath.Join(t.TempDir(), "go-carbon.conf")
		var configs []*Config
		for _, body := range []string{tc.integer, tc.readable} {
			if err := os.WriteFile(path, []byte(body), 0600); err != nil {
				t.Fatal(err)
			}
			cfg, err := ReadConfig(path)
			if err != nil {
				t.Fatal(err)
			}
			configs = append(configs, cfg)
		}
		if !reflect.DeepEqual(configs[0], configs[1]) {
			t.Fatalf("different numeric configs for %s", tc.readable)
		}
	}
	for _, body := range []string{"[cache]\nmax-size='120GiB'", "[cache]\nmax-size='120GB'", "[whisper]\nstore-cache-size='invalid'"} {
		path := filepath.Join(t.TempDir(), "bad.conf")
		if err := os.WriteFile(path, []byte(body), 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := ReadConfig(path); err == nil {
			t.Fatalf("accepted %s", body)
		}
	}
}

func TestReadableSizeReloadEquality(t *testing.T) {
	if isolatedTestProcess(t) {
		return
	}
	path := testConfig(t, t.TempDir())
	cfg, err := ReadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	cfg.Whisper.Enabled = false
	cfg.Tcp.Enabled, cfg.Udp.Enabled, cfg.Pickle.Enabled = false, false, false
	cfg.Grpc.Enabled, cfg.Carbonlink.Enabled = false, false
	cfg.Common.MetricInterval = &Duration{time.Hour}
	cfg.Cache.MaxSize = 120_000_000_000
	cfg.Cache.MaxBytes = 2 << 30
	writeBatchingConfig(t, path, cfg)
	app := New(path)
	if err := app.ParseConfig(); err != nil {
		t.Fatal(err)
	}
	if err := app.Start(); err != nil {
		t.Fatal(err)
	}
	defer app.Stop()
	before := app.Config.Cache
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	text := strings.Replace(string(data), "max-size = 120000000000", `max-size = "120G"`, 1)
	text = strings.Replace(text, "max-bytes = 2147483648", `max-bytes = "2GiB"`, 1)
	if text == string(data) {
		t.Fatal("integer config was not rewritten")
	}
	if err := os.WriteFile(path, []byte(text), 0600); err != nil {
		t.Fatal(err)
	}
	if err := app.ReloadConfig(); err != nil {
		t.Fatal(err)
	}
	if app.Config.Cache != before {
		t.Fatalf("cache changed on equivalent reload: %+v -> %+v", before, app.Config.Cache)
	}
	if got := app.Cache.GetInfo()["byte_limit"]; got != int64(2<<30) {
		t.Fatalf("byte limit=%v", got)
	}
}
