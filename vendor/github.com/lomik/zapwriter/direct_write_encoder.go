package zapwriter

import (
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap/buffer"
	"go.uber.org/zap/zapcore"
)

var _directWriteEncoderPool = sync.Pool{
	New: func() interface{} {
		return &directWriteEncoder{buf: nil}
	},
}

func getDirectWriteEncoder() *directWriteEncoder {
	return _directWriteEncoderPool.Get().(*directWriteEncoder)
}

func putDirectWriteEncoder(e *directWriteEncoder) {
	e.buf = nil
	_directWriteEncoderPool.Put(e)
}

type directWriteEncoder struct {
	buf *buffer.Buffer
}

func (s *directWriteEncoder) AppendArray(v zapcore.ArrayMarshaler) error {
	enc := &directWriteEncoder{buf: s.buf}
	err := v.MarshalLogArray(enc)
	return err
}

func (s *directWriteEncoder) AppendObject(v zapcore.ObjectMarshaler) error {
	m := zapcore.NewMapObjectEncoder()
	err := v.MarshalLogObject(m)
	if err != nil {
		return err
	}
	_, err = fmt.Fprint(s.buf, m.Fields)
	return err
}

func (s *directWriteEncoder) AppendReflected(v interface{}) error {
	_, err := fmt.Fprint(s.buf, v)
	return err
}

func (s *directWriteEncoder) AppendBool(v bool)              { fmt.Fprint(s.buf, v) }
func (s *directWriteEncoder) AppendByteString(v []byte)      { fmt.Fprint(s.buf, v) }
func (s *directWriteEncoder) AppendComplex128(v complex128)  { fmt.Fprint(s.buf, v) }
func (s *directWriteEncoder) AppendComplex64(v complex64)    { fmt.Fprint(s.buf, v) }
func (s *directWriteEncoder) AppendDuration(v time.Duration) { fmt.Fprint(s.buf, v) }
func (s *directWriteEncoder) AppendFloat64(v float64)        { fmt.Fprint(s.buf, v) }
func (s *directWriteEncoder) AppendFloat32(v float32)        { fmt.Fprint(s.buf, v) }
func (s *directWriteEncoder) AppendInt(v int)                { fmt.Fprint(s.buf, v) }
func (s *directWriteEncoder) AppendInt64(v int64)            { fmt.Fprint(s.buf, v) }
func (s *directWriteEncoder) AppendInt32(v int32)            { fmt.Fprint(s.buf, v) }
func (s *directWriteEncoder) AppendInt16(v int16)            { fmt.Fprint(s.buf, v) }
func (s *directWriteEncoder) AppendInt8(v int8)              { fmt.Fprint(s.buf, v) }
func (s *directWriteEncoder) AppendString(v string)          { fmt.Fprint(s.buf, v) }
func (s *directWriteEncoder) AppendTime(v time.Time)         { fmt.Fprint(s.buf, v) }
func (s *directWriteEncoder) AppendUint(v uint)              { fmt.Fprint(s.buf, v) }
func (s *directWriteEncoder) AppendUint64(v uint64)          { fmt.Fprint(s.buf, v) }
func (s *directWriteEncoder) AppendUint32(v uint32)          { fmt.Fprint(s.buf, v) }
func (s *directWriteEncoder) AppendUint16(v uint16)          { fmt.Fprint(s.buf, v) }
func (s *directWriteEncoder) AppendUint8(v uint8)            { fmt.Fprint(s.buf, v) }
func (s *directWriteEncoder) AppendUintptr(v uintptr)        { fmt.Fprint(s.buf, v) }
