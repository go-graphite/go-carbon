package stop

import "sync"

type Interface interface {
	Start()
	Stop()
}

// Struct is abstract class with Start/Stop methods
type Struct struct {
	sync.RWMutex
	exit     chan struct{}
	wg       sync.WaitGroup
	Go       func(callable func(exit chan struct{}))
	WithExit func(callable func(exit chan struct{}))
}

// Start ...
func (s *Struct) Start() {
	s.StartFunc(func() error { return nil })
}

// Stop ...
func (s *Struct) Stop() {
	s.StopFunc(func() {})
}

// StartFunc ...
func (s *Struct) StartFunc(startProcedure func() error) error {
	s.Lock()
	defer s.Unlock()

	// already started
	if s.exit != nil {
		return nil
	}

	exit := make(chan struct{})
	s.exit = exit
	s.Go = func(callable func(exit chan struct{})) {
		s.wg.Add(1)
		go func() {
			callable(exit)
			s.wg.Done()
		}()
	}
	s.WithExit = func(callable func(exit chan struct{})) {
		callable(exit)
	}

	err := startProcedure()

	// stop all if start failed
	if err != nil {
		s.doStop(func() {})
	}

	return err
}

func (s *Struct) doStop(callable func()) {
	// already stopped
	if s.exit == nil {
		return
	}

	close(s.exit)
	callable()
	s.wg.Wait()
	s.exit = nil
	s.Go = func(callable func(exit chan struct{})) {}
	s.WithExit = func(callable func(exit chan struct{})) {
		callable(nil)
	}
}

// StopFunc ...
func (s *Struct) StopFunc(callable func()) {
	s.Lock()
	defer s.Unlock()

	// already stopped
	if s.exit == nil {
		return
	}

	s.doStop(callable)
}
