package recovery

import "io"

// ReadCaptureFramesPipeline overlaps decode/preparation with ordered application.
// Exactly two decoded buffers circulate; neither can be reused until visit has
// returned. prepare must honor stop while waiting, and reader must be bounded or
// independently interruptible. Only visit advances the returned point count.
func ReadCaptureFramesPipeline(reader io.Reader, buffer int, prepare func([]byte, int, <-chan struct{}) (int64, error), visit func([]byte, int, int64, int64) error) (int, error) {
	type frame struct {
		raw  []byte
		n    int
		end  int64
		cost int64
	}
	ready := make(chan frame)
	free := make(chan []byte, 2)
	free <- nil
	stop := make(chan struct{})
	finished := make(chan error, 1)
	go func() {
		_, err := readCaptureFramesBuffer(reader, buffer, func(raw []byte, n int, end int64) ([]byte, error) {
			cost, err := prepare(raw, n, stop)
			if err != nil {
				return raw, err
			}
			select {
			case ready <- frame{raw, n, end, cost}:
			case <-stop:
				return raw, io.ErrClosedPipe
			}
			select {
			case raw = <-free:
				return raw, nil
			case <-stop:
				return nil, io.ErrClosedPipe
			}
		})
		close(ready)
		finished <- err
	}()
	count := 0
	for frame := range ready {
		if err := visit(frame.raw, frame.n, frame.end, frame.cost); err != nil {
			close(stop)
			<-finished
			return count, err
		}
		count += frame.n
		free <- frame.raw
	}
	close(stop)
	return count, <-finished
}
