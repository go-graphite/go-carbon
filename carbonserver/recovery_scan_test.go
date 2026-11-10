package carbonserver

import (
	"sync/atomic"
	"testing"
	"time"
)

func TestRecoveryDefersScanWithoutBlockingRealtimeDiscovery(t *testing.T) {
	t.Parallel()
	for _, release := range []bool{false, true} {
		t.Run(map[bool]string{false: "cancel", true: "recover"}[release], func(t *testing.T) {
			dir := t.TempDir()
			l := NewCarbonserverListener(nil)
			l.SetWhisperData(dir)
			l.SetTrieIndex(true)
			l.SetConcurrentIndex(true)
			names := l.SetRealtimeIndex(10)
			l.updateFileList(dir, nil, nil)
			var ready atomic.Bool
			checked := make(chan struct{}, 1)
			l.SetScanRecoveryReady(func() bool {
				select {
				case checked <- struct{}{}:
				default:
				}
				return ready.Load()
			})
			if err := addFileToSys("/namespace/on/disk.wsp", dir); err != nil {
				t.Fatal(err)
			}
			force, exit, done := make(chan struct{}, 1), make(chan struct{}), make(chan struct{})
			go func() {
				defer close(done)
				l.fileListUpdater(dir, nil, force, exit)
			}()
			t.Cleanup(func() {
				close(exit)
				select {
				case <-done:
				case <-time.After(time.Second):
					t.Error("deferred scan did not stop promptly")
				}
			})
			force <- struct{}{}
			select {
			case <-checked:
			case <-time.After(time.Second):
				t.Fatal("forced scan bypassed the recovery boundary")
			}
			names <- "namespace.realtime"
			deadline := time.Now().Add(time.Second)
			for !l.MetricExists("namespace.realtime") && time.Now().Before(deadline) {
				time.Sleep(time.Millisecond)
			}
			if !l.MetricExists("namespace.realtime") || l.MetricExists("namespace.on.disk") {
				t.Fatal("recovery must defer disk scanning while retaining realtime discovery")
			}
			if release {
				ready.Store(true)
				deadline = time.Now().Add(3 * time.Second)
				for !l.MetricExists("namespace.on.disk") && time.Now().Before(deadline) {
					time.Sleep(time.Millisecond)
				}
				if !l.MetricExists("namespace.on.disk") {
					t.Fatal("deferred force request was lost after recovery")
				}
			}
		})
	}
}
