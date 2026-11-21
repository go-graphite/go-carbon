package v2core

import "testing"

func TestCoreWakeHintsCannotBlockOrLoseCredit(t *testing.T) {
	wake := make(chan struct{}, 1)
	for range 100 {
		coreWake(wake)
	}
	if len(wake) != 1 {
		t.Fatal("notification did not coalesce")
	}
	router := &coreRouter{}
	router.bytes.Store(100)
	lease := &coreDispatch{router: router, bytes: 100}
	lease.remaining.Store(2)
	lease.release()
	if router.bytes.Load() != 100 {
		t.Fatal("early credit return")
	}
	lease.release()
	if router.bytes.Load() != 0 {
		t.Fatal("last owner did not return credit")
	}
}
