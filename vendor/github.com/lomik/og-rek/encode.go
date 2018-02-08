package ogórek

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"math/big"
	"reflect"
)

type TypeError struct {
	typ string
}

// Marshaler is the interface implemented by an object that can marshal itself into a binary form.
type Marshaler interface {
	MarshalPickle() (text []byte, err error)
}

func (te *TypeError) Error() string {
	return fmt.Sprintf("no support for type '%s'", te.typ)
}

// An Encoder encodes Go data structures into pickle byte stream
type Encoder struct {
	w io.Writer
}

// NewEncoder returns a new Encoder struct with default values
func NewEncoder(w io.Writer) *Encoder {
	return &Encoder{w: w}
}

// Encode writes the pickle encoding of v to w, the encoder's writer
func (e *Encoder) Encode(v interface{}) error {
	rv := reflectValueOf(v)
	err := e.encode(rv)
	if err != nil {
		return err
	}
	_, err = e.w.Write([]byte{opStop})
	return err
}

func (e *Encoder) encode(rv reflect.Value) error {

	switch rk := rv.Kind(); rk {

	case reflect.Bool:
		return e.encodeBool(rv.Bool())
	case reflect.Int, reflect.Int8, reflect.Int64, reflect.Int32, reflect.Int16:
		return e.encodeInt(reflect.Int, rv.Int())
	case reflect.Uint8, reflect.Uint64, reflect.Uint, reflect.Uint32, reflect.Uint16:
		return e.encodeInt(reflect.Uint, int64(rv.Uint()))
	case reflect.String:
		return e.encodeString(rv.String())
	case reflect.Array, reflect.Slice:
		if rv.Type().Elem().Kind() == reflect.Uint8 {
			return e.encodeBytes(rv.Bytes())
		} else if _, ok := rv.Interface().(Tuple); ok {
			return e.encodeTuple(rv.Interface().(Tuple))
		} else {
			return e.encodeArray(rv)
		}

	case reflect.Map:
		return e.encodeMap(rv)

	case reflect.Struct:
		return e.encodeStruct(rv)

	case reflect.Float32, reflect.Float64:
		return e.encodeFloat(float64(rv.Float()))

	case reflect.Interface:
		// recurse until we get a concrete type
		// could be optmized into a tail call
		return e.encode(rv.Elem())

	case reflect.Ptr:

		if rv.Elem().Kind() == reflect.Struct {
			if m, ok := rv.Interface().(Marshaler); ok {
				b, err := m.MarshalPickle()
				e.w.Write(b)
				return err
			}

			switch rv.Elem().Interface().(type) {
			case None:
				return e.encodeStruct(rv.Elem())
			}
		}

		return e.encode(rv.Elem())

	case reflect.Invalid:
		_, err := e.w.Write([]byte{opNone})
		return err
	default:
		return &TypeError{typ: rk.String()}
	}

	return nil
}

func (e *Encoder) encodeTuple(t Tuple) error {
	l := len(t)

	switch l {
	case 0:
		_, err := e.w.Write([]byte{opEmptyTuple})
		return err

	// TODO this are protocol 2 opcodes - check e.protocol before using them
	//case 1:
	//case 2:
	//case 3:
	}

	_, err := e.w.Write([]byte{opMark})
	if err != nil {
		return err
	}

	for i := 0; i < l; i++ {
		err = e.encode(reflectValueOf(t[i]))
		if err != nil {
			return err
		}
	}

	_, err = e.w.Write([]byte{opTuple})
	return err
}

func (e *Encoder) encodeArray(arr reflect.Value) error {

	l := arr.Len()

	_, err := e.w.Write([]byte{opEmptyList, opMark})
	if err != nil {
		return err
	}

	for i := 0; i < l; i++ {
		v := arr.Index(i)
		err = e.encode(v)
		if err != nil {
			return err
		}
	}

	_, err = e.w.Write([]byte{opAppends})
	return err
}

func (e *Encoder) encodeBool(b bool) error {
	var err error

	if b {
		_, err = e.w.Write([]byte(opTrue))
	} else {
		_, err = e.w.Write([]byte(opFalse))
	}

	return err
}

func (e *Encoder) encodeBytes(byt []byte) error {

	l := len(byt)

	if l < 256 {
		_, err := e.w.Write([]byte{opShortBinstring, byte(l)})
		if err != nil {
			return err
		}
	} else {
		_, err := e.w.Write([]byte{opBinstring})
		if err != nil {
			return err
		}
		var b [4]byte

		binary.LittleEndian.PutUint32(b[:], uint32(l))
		_, err = e.w.Write(b[:])
		if err != nil {
			return err
		}
	}

	_, err := e.w.Write(byt)
	return err
}

func (e *Encoder) encodeFloat(f float64) error {
	var u uint64
	u = math.Float64bits(f)

	_, err := e.w.Write([]byte{opBinfloat})
	if err != nil {
		return err
	}
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], uint64(u))

	_, err = e.w.Write(b[:])
	return err
}

func (e *Encoder) encodeInt(k reflect.Kind, i int64) error {
	var err error

	// FIXME: need support for 64-bit ints

	switch {
	case i > 0 && i < math.MaxUint8:
		_, err = e.w.Write([]byte{opBinint1, byte(i)})
	case i > 0 && i < math.MaxUint16:
		_, err = e.w.Write([]byte{opBinint2, byte(i), byte(i >> 8)})
	case i >= math.MinInt32 && i <= math.MaxInt32:
		_, err = e.w.Write([]byte{opBinint})
		if err != nil {
			return err
		}
		var b [4]byte
		binary.LittleEndian.PutUint32(b[:], uint32(i))
		_, err = e.w.Write(b[:])
	default: // int64, but as a string :/
		_, err = e.w.Write([]byte{opInt})
		if err != nil {
			return err
		}
		fmt.Fprintf(e.w, "%d\n", i)
	}

	return err
}

func (e *Encoder) encodeLong(b *big.Int) error {
	// TODO if e.protocol >= 2 use opLong1 & opLong4
	_, err := fmt.Fprintf(e.w, "%c%dL\n", opLong, b)
	return err
}

func (e *Encoder) encodeMap(m reflect.Value) error {

	keys := m.MapKeys()

	l := len(keys)

	_, err := e.w.Write([]byte{opEmptyDict})
	if err != nil {
		return err
	}

	if l > 0 {
		_, err := e.w.Write([]byte{opMark})
		if err != nil {
			return err
		}

		for _, k := range keys {
			err = e.encode(k)
			if err != nil {
				return err
			}
			v := m.MapIndex(k)

			err = e.encode(v)
			if err != nil {
				return err
			}
		}

		_, err = e.w.Write([]byte{opSetitems})
		if err != nil {
			return err
		}
	}

	return nil
}

func (e *Encoder) encodeString(s string) error {
	return e.encodeBytes([]byte(s))
}

func (e *Encoder) encodeCall(v *Call) error {
	_, err := fmt.Fprintf(e.w, "%c%s\n%s\n", opGlobal, v.Callable.Module, v.Callable.Name)
	if err != nil {
		return err
	}
	err = e.encodeTuple(v.Args)
	if err != nil {
		return err
	}
	_, err = e.w.Write([]byte{opReduce})
	return err
}

func (e *Encoder) encodeStruct(st reflect.Value) error {

	typ := st.Type()

	// first test if it's one of our internal python structs
	switch v := st.Interface().(type) {
	case None:
		_, err := e.w.Write([]byte{opNone})
		return err
	case Call:
		return e.encodeCall(&v)
	case big.Int:
		return e.encodeLong(&v)
	}

	structTags := getStructTags(st)

	_, err := e.w.Write([]byte{opEmptyDict, opMark})
	if err != nil {
		return err
	}

	if structTags != nil {
		for f, i := range structTags {
			err := e.encodeString(f)
			if err != nil {
				return err
			}

			err = e.encode(st.Field(i))
			if err != nil {
				return err
			}
		}
	} else {
		l := typ.NumField()
		for i := 0; i < l; i++ {
			fty := typ.Field(i)
			if fty.PkgPath != "" {
				continue // skip unexported names
			}

			err := e.encodeString(fty.Name)
			if err != nil {
				return err
			}

			err = e.encode(st.Field(i))
			if err != nil {
				return err
			}
		}
	}

	_, err = e.w.Write([]byte{opSetitems})
	return err
}

func reflectValueOf(v interface{}) reflect.Value {

	rv, ok := v.(reflect.Value)
	if !ok {
		rv = reflect.ValueOf(v)
	}
	return rv
}

func getStructTags(ptr reflect.Value) map[string]int {
	if ptr.Kind() != reflect.Struct {
		return nil
	}

	m := make(map[string]int)

	t := ptr.Type()

	l := t.NumField()
	numTags := 0
	for i := 0; i < l; i++ {
		field := t.Field(i).Tag.Get("pickle")
		if field != "" {
			m[field] = i
			numTags++
		}
	}

	if numTags == 0 {
		return nil
	}

	return m
}
