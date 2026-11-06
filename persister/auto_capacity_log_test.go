package persister

import (
	"bytes"
	"encoding/json"
	"testing"
	"testing/synctest"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func TestCapacityRateLogsAreBoundedWhileActuatorKeepsMoving(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var logs bytes.Buffer
		p := &Whisper{logger: zap.New(zapcore.NewCore(zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig()), zapcore.AddSync(&logs), zap.InfoLevel))}
		defer p.stopAutoUpdateRate()
		a := &p.autoUpdate
		a.floor = 1
		a.controller.reason = "steady"
		rate := 10000
		for range 36 {
			a.ordinary = rate
			p.applyAutoUpdateRateLocked()
			if a.applied != rate {
				t.Fatal("INFO filter changed actual actuator", a.applied, rate)
			}
			rate += rate / 50 // 2% every five seconds, below the 25% log filter
			time.Sleep(5 * time.Second)
		}
		lines := bytes.Split(bytes.TrimSpace(logs.Bytes()), []byte("\n"))
		entries := make([]struct {
			Time  float64 `json:"ts"`
			Value int     `json:"updates_per_second"`
		}, len(lines))
		for i, line := range lines {
			if err := json.Unmarshal(line, &entries[i]); err != nil {
				t.Fatal(err)
			}
		}
		if len(entries) != 3 {
			t.Fatal("expected initial and two material minute observations", len(entries))
		}
		for i := 1; i < len(entries); i++ {
			if entries[i].Time-entries[i-1].Time < time.Minute.Seconds() {
				t.Fatal("stable rate reason logged more than once/minute")
			}
			before, after := entries[i-1].Value, entries[i].Value
			if after-before < before/4 {
				t.Fatal("sub-25% change logged", before, after)
			}
		}
		// A changed reason bypasses the cooldown even when the rate change is tiny.
		a.controller.reason, a.ordinary = "restart", rate
		p.applyAutoUpdateRateLocked()
		if bytes.Count(logs.Bytes(), []byte("\n")) != len(entries)+1 {
			t.Fatal("reason transition hidden by INFO filter")
		}
	})
}

func TestCapacityAdjustmentLogCooldownAndReasons(t *testing.T) {
	var log autoAdjustmentLog
	now := time.Now()
	count := 0
	for i := range 36 {
		reason := "observing"
		if i%2 != 0 {
			reason = "token wait with idle hardware"
		}
		if log.allow(now.Add(time.Duration(i)*5*time.Second), capacityLogReason(reason), 48000+i*1000, 0) {
			count++
		}
	}
	if count != 3 {
		t.Fatal("capacity INFO exceeded once/minute in the healthy regime", count)
	}
	if !log.allow(now.Add(176*time.Second), "hard pressure", 10000, 0) {
		t.Fatal("hard pressure transition hidden by cooldown")
	}
	if log.allow(now.Add(177*time.Second), "hard pressure", 7000, 0) {
		t.Fatal("repeated hard pressure bypassed cooldown")
	}
}
