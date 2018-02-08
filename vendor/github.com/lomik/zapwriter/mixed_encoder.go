// Copyright (c) 2016 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package zapwriter

import (
	"go.uber.org/zap/buffer"
	"go.uber.org/zap/zapcore"
)

var bufferpool = buffer.NewPool()

type mixedEncoder struct {
	*jsonEncoder
}

func NewMixedEncoder(cfg zapcore.EncoderConfig) zapcore.Encoder {
	return mixedEncoder{newJSONEncoder(cfg, true)}
}

func (enc mixedEncoder) Clone() zapcore.Encoder {
	return mixedEncoder{enc.jsonEncoder.Clone().(*jsonEncoder)}
}

func (enc mixedEncoder) EncodeEntry(ent zapcore.Entry, fields []zapcore.Field) (*buffer.Buffer, error) {
	final := enc.clone()

	wr := getDirectWriteEncoder()
	wr.buf = final.buf

	if enc.TimeKey != "" && enc.EncodeTime != nil {
		final.buf.AppendByte('[')
		enc.EncodeTime(ent.Time, wr)
		final.buf.AppendByte(']')
	}

	if enc.LevelKey != "" && enc.EncodeLevel != nil {
		if final.buf.Len() > 0 {
			final.buf.AppendByte(' ')
		}
		enc.EncodeLevel(ent.Level, wr)
	}

	if ent.LoggerName != "" && enc.NameKey != "" {
		if final.buf.Len() > 0 {
			final.buf.AppendByte(' ')
		}
		final.buf.AppendByte('[')
		wr.AppendString(ent.LoggerName)
		final.buf.AppendByte(']')
	}

	if ent.Caller.Defined && enc.CallerKey != "" && enc.EncodeCaller != nil {
		if final.buf.Len() > 0 {
			final.buf.AppendByte(' ')
		}
		enc.EncodeCaller(ent.Caller, wr)
		final.buf.AppendByte(' ')
	}

	putDirectWriteEncoder(wr)

	// Add the message itself.
	if final.MessageKey != "" {
		if final.buf.Len() > 0 {
			final.buf.AppendByte(' ')
		}
		final.buf.AppendString(ent.Message)
	}

	if final.buf.Len() > 0 {
		final.buf.AppendByte(' ')
	}

	final.buf.AppendByte('{')

	if enc.buf.Len() > 0 {
		final.addElementSeparator()
		final.buf.Write(enc.buf.Bytes())
	}
	addFields(final, fields)
	final.closeOpenNamespaces()
	if ent.Stack != "" && final.StacktraceKey != "" {
		final.AddString(final.StacktraceKey, ent.Stack)
	}
	final.buf.AppendByte('}')
	final.buf.AppendByte('\n')

	ret := final.buf
	return ret, nil
}
