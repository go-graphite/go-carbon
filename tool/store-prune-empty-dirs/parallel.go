package main

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/go-graphite/go-carbon/internal/whisperpath"
	"golang.org/x/sys/unix"
)

type walkProgress struct {
	mu     sync.Mutex
	counts counts
}

func (p *walkProgress) store(c counts) {
	c.Suffixes = nil
	p.mu.Lock()
	p.counts = c
	p.mu.Unlock()
}
func (p *walkProgress) load() counts {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.counts
}

func (c *counts) add(other counts) {
	c.Passes = max(c.Passes, other.Passes)
	c.Dirs += other.Dirs
	c.EmptyDirs += other.EmptyDirs
	c.RemovableDirs += other.RemovableDirs
	c.Removed += other.Removed
	c.Occupied += other.Occupied
	c.Files += other.Files
	c.Symlinks += other.Symlinks
	c.Other += other.Other
	c.Excluded += other.Excluded
	c.LocksEligible += other.LocksEligible
	c.LocksRemoved += other.LocksRemoved
	if c.Suffixes != nil {
		for suffix, n := range other.Suffixes {
			c.Suffixes[suffix] += n
		}
	}
}

type parallelWalk struct {
	walkers  []*walker
	progress []*walkProgress
}

func newParallelWalk(workers int, dry bool, exclude map[string]bool) *parallelWalk {
	p := &parallelWalk{}
	for i := 0; i < workers+2; i++ {
		p.walkers = append(p.walkers, &walker{dry: dry, exclude: exclude, counts: counts{Suffixes: make(map[string]uint64)}})
		p.progress = append(p.progress, &walkProgress{})
	}
	return p
}

func (p *parallelWalk) snapshot() counts {
	var total counts
	for _, slot := range p.progress {
		total.add(slot.load())
	}
	return total
}
func (p *parallelWalk) final() counts {
	total := counts{Suffixes: make(map[string]uint64)}
	for _, w := range p.walkers {
		total.add(w.counts)
	}
	return total
}

type pruneSubtree struct {
	parent    int
	name, rel string
}

func (p *parallelWalk) walk(ctx context.Context, root string, before func() error) error {
	fd, err := whisperpath.OpenDir(root)
	if err != nil {
		return err
	}
	defer unix.Close(fd)
	var st unix.Stat_t
	if err := unix.Fstat(fd, &st); err != nil {
		return err
	}
	for i, w := range p.walkers {
		slot := p.progress[i]
		w.rootDev = uint64(st.Dev)
		w.cutoff = time.Now().Add(-time.Hour)
		w.before = before
		w.check = func() error { slot.store(w.counts); return ctx.Err() }
	}
	top, cleanup := p.walkers[0], p.walkers[1]
	cleanup.maxDepth = 3
	for {
		top.counts.Passes++
		changed := p.snapshot().Removed + p.snapshot().LocksRemoved
		jobs := make(chan pruneSubtree, 2*(len(p.walkers)-2))
		workCtx, cancel := context.WithCancelCause(ctx)
		var wg sync.WaitGroup
		for i := 2; i < len(p.walkers); i++ {
			w, slot := p.walkers[i], p.progress[i]
			wg.Go(func() {
				for job := range jobs {
					if workCtx.Err() == nil {
						if err := w.subtree(workCtx, job); err != nil {
							cancel(err)
						}
					}
					unix.Close(job.parent)
					slot.store(w.counts)
				}
			})
		}
		top.dispatch = func(parent int, name, rel string) error {
			dup, err := unix.FcntlInt(uintptr(parent), unix.F_DUPFD_CLOEXEC, 0)
			if err != nil {
				return err
			}
			job := pruneSubtree{parent: dup, name: strings.Clone(name), rel: rel}
			select {
			case jobs <- job:
				return nil
			case <-workCtx.Done():
				unix.Close(dup)
				return context.Cause(workCtx)
			}
		}
		_, err = top.visit(workCtx, fd, -1, "", "", 0)
		close(jobs)
		wg.Wait()
		if err == nil {
			err = context.Cause(workCtx)
		}
		cancel(nil)
		p.progress[0].store(top.counts)
		if err != nil {
			return err
		}
		if top.dry {
			return nil
		}
		if _, err := unix.Seek(fd, 0, 0); err != nil {
			return err
		}
		_, err = cleanup.visit(ctx, fd, -1, "", "", 0)
		p.progress[1].store(cleanup.counts)
		if err != nil {
			return err
		}
		after := p.snapshot()
		if after.Removed+after.LocksRemoved == changed {
			return nil
		}
		if _, err := unix.Seek(fd, 0, 0); err != nil {
			return err
		}
	}
}

func (w *walker) subtree(ctx context.Context, job pruneSubtree) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	w.cutoff = time.Now().Add(-time.Hour)
	if !w.dry {
		if w.before != nil {
			if err := w.before(); err != nil {
				return err
			}
		}
		removed, err := whisperpath.RemoveDir(job.parent, job.name)
		if removed {
			w.counts.Dirs++
			w.counts.EmptyDirs++
			w.counts.RemovableDirs++
			w.counts.Removed++
			return nil
		}
		if errors.Is(err, unix.EBUSY) || errors.Is(err, unix.EXDEV) {
			w.counts.Excluded++
			return nil
		}
		if errors.Is(err, unix.ENOTDIR) || errors.Is(err, unix.ELOOP) {
			return nil
		}
		if err != nil {
			return err
		}
	}
	fd, err := openChildDir(job.parent, job.name, w.rootDev)
	if errors.Is(err, unix.ENOENT) || errors.Is(err, unix.ENOTDIR) || errors.Is(err, unix.ELOOP) {
		return nil
	}
	if errors.Is(err, unix.EXDEV) {
		w.counts.Excluded++
		return nil
	}
	if err != nil {
		return err
	}
	_, err = w.visit(ctx, fd, job.parent, job.name, job.rel, 0)
	return err
}
