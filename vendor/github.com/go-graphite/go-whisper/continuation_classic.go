package whisper

import "fmt"

// VisitClassicArchiveSlots visits physical nonempty slots without applying a
// retention horizon or rotating them into timestamp order. A sidecar can keep
// fine corrections longer than their own retention for coarse replay.
func (w *Whisper) VisitClassicArchiveSlots(index int, visit func(int, TimeSeriesPoint) error) error {
	if w.compressed || index < 0 || index >= len(w.archives) {
		return fmt.Errorf("invalid classic continuation archive %d", index)
	}
	a := w.archives[index]
	end := a.End()
	extents := newArchiveExtentReader(w.file, end, a.Size())
	bufferSize := a.Size()
	if bufferSize > 4096*PointSize {
		bufferSize = 4096 * PointSize
	}
	buffer := make([]byte, bufferSize)
	for offset := a.Offset(); offset < end; {
		from, until, err := extents.next(offset, end, a.Offset())
		if err != nil {
			return err
		}
		offset = from
		if offset == end {
			break
		}
		size := int(until - offset)
		if size > len(buffer) {
			size = len(buffer)
		}
		if size <= 0 || size%PointSize != 0 {
			return fmt.Errorf("invalid classic continuation extent")
		}
		if err := w.fileReadAt(buffer[:size], offset); err != nil {
			return err
		}
		for pos := 0; pos < size; pos += PointSize {
			p := unpackDataPoint(buffer[pos : pos+PointSize])
			if p.interval > 0 {
				if err := visit(int((offset-a.Offset())/PointSize)+pos/PointSize, TimeSeriesPoint{Time: p.interval, Value: p.value}); err != nil {
					return err
				}
			}
		}
		offset += int64(size)
	}
	return nil
}
