package downsize

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestNativeRouteDefaultHasNoSourceBarrier(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	started := make(chan string, 2)
	release := make(chan struct{})
	done := make(chan error, 1)
	// A zero-value option must retain the original cross-source scheduling:
	// the second donor starts while the first donor is still running.
	go func() {
		done <- runNativeRoutes(ctx, []route{{sourceURL: "a"}, {sourceURL: "b"}}, 2, options{}.nativeSourceBarrier, func(ctx context.Context, r route) error {
			started <- r.sourceURL
			select {
			case <-release:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		})
	}()
	seen := map[string]bool{}
	for range 2 {
		select {
		case source := <-started:
			seen[source] = true
		case <-ctx.Done():
			t.Fatal("default controller added a source barrier")
		}
	}
	close(release)
	if err := <-done; err != nil || !seen["a"] || !seen["b"] {
		t.Fatal("default controller changed routes", seen, err)
	}
}

func TestNativeRouteSourceBarrier(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	routes := []route{{Path: "a0", sourceURL: "a"}, {Path: "a1", sourceURL: "a"}, {Path: "b0", sourceURL: "b"}, {Path: "a2", sourceURL: "a"}}
	started := make(chan string, len(routes))
	release := map[string]chan struct{}{}
	for _, r := range routes {
		release[r.Path] = make(chan struct{})
	}
	done := make(chan error, 1)
	go func() {
		done <- runNativeRoutes(ctx, routes, 2, true, func(ctx context.Context, r route) error {
			started <- r.Path
			select {
			case <-release[r.Path]:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		})
	}()
	next := func() string {
		t.Helper()
		select {
		case s := <-started:
			return s
		case <-ctx.Done():
			t.Fatal("route did not start")
			return ""
		}
	}
	a, b := next(), next()
	if !((a == "a0" && b == "a1") || (a == "a1" && b == "a0")) {
		t.Fatal(a, b)
	}
	close(release["a0"])
	select {
	case s := <-started:
		t.Fatal("crossed source barrier", s)
	case <-time.After(25 * time.Millisecond):
	}
	close(release["a1"])
	if s := next(); s != "b0" {
		t.Fatal(s)
	}
	select {
	case s := <-started:
		t.Fatal("reordered donor blocks", s)
	case <-time.After(25 * time.Millisecond):
	}
	close(release["b0"])
	if s := next(); s != "a2" {
		t.Fatal(s)
	}
	close(release["a2"])
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestNativeRouteFailureDrainsSourceBarrier(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	want := errors.New("injected range failure")
	started := make(chan struct{}, 2)
	fail := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- runNativeRoutes(ctx, []route{{Path: "a0", sourceURL: "a"}, {Path: "a1", sourceURL: "a"}, {Path: "b", sourceURL: "b"}}, 2, true, func(ctx context.Context, r route) error {
			if r.sourceURL == "b" {
				return errors.New("source barrier was crossed after failure")
			}
			started <- struct{}{}
			if r.Path == "a1" {
				<-fail
				return want
			}
			<-ctx.Done()
			return ctx.Err()
		})
	}()
	for range 2 {
		select {
		case <-started:
		case <-ctx.Done():
			t.Fatal("workers serialized")
		}
	}
	close(fail)
	select {
	case err := <-done:
		if !errors.Is(err, want) {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("source barrier deadlocked")
	}
}
