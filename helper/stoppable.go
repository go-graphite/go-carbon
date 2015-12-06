package helper

import "sync"

// Stoppable is abstract class with Start/Stop methods
type Stoppable struct {
	sync.RWMutex
	exit chan bool
	wg   sync.WaitGroup
	Go   func(callable func(exit chan bool))
}

// Start ...
func (s *Stoppable) Start() {
	s.StartFunc(func() {})
}

// Stop ...
func (s *Stoppable) Stop() {
	s.StopFunc(func() {})
}

// // Exit returns exit channel
// func (s *Stoppable) Exit() chan bool {
// 	s.RLock()
// 	defer s.RUnlock()
// 	return s.exit
// }

// StartFunc ...
func (s *Stoppable) StartFunc(callable func()) {
	s.Lock()
	defer s.Unlock()

	// already started
	if s.exit != nil {
		return
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
	callable()
}

// StopFunc ...
func (s *Stoppable) StopFunc(callable func()) {
	s.Lock()
	defer s.Unlock()

	// already stopped
	if s.exit == nil {
		return
	}

	close(s.exit)
	callable()
	s.wg.Wait()
	s.exit = nil
	s.Go = func(callable func(exit chan bool)) {}
}
