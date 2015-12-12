package helper

import "sync"

type StoppableInterface interface {
}

// Stoppable is abstract class with Start/Stop methods
type Stoppable struct {
	sync.RWMutex
	exit     chan bool
	wg       sync.WaitGroup
	Go       func(callable func(exit chan bool))
	WithExit func(callable func(exit chan bool))
}

// Start ...
func (s *Stoppable) Start() {
	s.StartFunc(func() error { return nil })
}

// Stop ...
func (s *Stoppable) Stop() {
	s.StopFunc(func() {})
}

// StartFunc ...
func (s *Stoppable) StartFunc(startProcedure func() error) error {
	s.Lock()
	defer s.Unlock()

	// already started
	if s.exit != nil {
		return nil
	}

	exit := make(chan bool)
	s.exit = exit
	s.Go = func(callable func(exit chan bool)) {
		s.wg.Add(1)
		go func() {
			callable(exit)
			s.wg.Done()
		}()
	}
	s.WithExit = func(callable func(exit chan bool)) {
		callable(exit)
	}

	err := startProcedure()

	// stop all if start failed
	if err != nil {
		s.doStop(func() {})
	}

	return err
}

func (s *Stoppable) doStop(callable func()) {
	// already stopped
	if s.exit == nil {
		return
	}

	close(s.exit)
	callable()
	s.wg.Wait()
	s.exit = nil
	s.Go = func(callable func(exit chan bool)) {}
	s.WithExit = func(callable func(exit chan bool)) {
		callable(nil)
	}
}

// StopFunc ...
func (s *Stoppable) StopFunc(callable func()) {
	s.Lock()
	defer s.Unlock()

	// already stopped
	if s.exit == nil {
		return
	}

	s.doStop(callable)
}
