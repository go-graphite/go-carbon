// Package ogórek is a library for decoding Python's pickle format.
//
// ogórek is Polish for "pickle".
package ogórek

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"math/big"
	"reflect"
	"strconv"
)

// Opcodes
const (
	opMark           byte = '(' // push special markobject on stack
	opStop                = '.' // every pickle ends with STOP
	opPop                 = '0' // discard topmost stack item
	opPopMark             = '1' // discard stack top through topmost markobject
	opDup                 = '2' // duplicate top stack item
	opFloat               = 'F' // push float object; decimal string argument
	opInt                 = 'I' // push integer or bool; decimal string argument
	opBinint              = 'J' // push four-byte signed int
	opBinint1             = 'K' // push 1-byte unsigned int
	opLong                = 'L' // push long; decimal string argument
	opBinint2             = 'M' // push 2-byte unsigned int
	opNone                = 'N' // push None
	opPersid              = 'P' // push persistent object; id is taken from string arg
	opBinpersid           = 'Q' //  "       "         "  ;  "  "   "     "  stack
	opReduce              = 'R' // apply callable to argtuple, both on stack
	opString              = 'S' // push string; NL-terminated string argument
	opBinstring           = 'T' // push string; counted binary string argument
	opShortBinstring      = 'U' //  "     "   ;    "      "       "      " < 256 bytes
	opUnicode             = 'V' // push Unicode string; raw-unicode-escaped"d argument
	opBinunicode          = 'X' //   "     "       "  ; counted UTF-8 string argument
	opAppend              = 'a' // append stack top to list below it
	opBuild               = 'b' // call __setstate__ or __dict__.update()
	opGlobal              = 'c' // push self.find_class(modname, name); 2 string args
	opDict                = 'd' // build a dict from stack items
	opEmptyDict           = '}' // push empty dict
	opAppends             = 'e' // extend list on stack by topmost stack slice
	opGet                 = 'g' // push item from memo on stack; index is string arg
	opBinget              = 'h' //   "    "    "    "   "   "  ;   "    " 1-byte arg
	opInst                = 'i' // build & push class instance
	opLongBinget          = 'j' // push item from memo on stack; index is 4-byte arg
	opList                = 'l' // build list from topmost stack items
	opEmptyList           = ']' // push empty list
	opObj                 = 'o' // build & push class instance
	opPut                 = 'p' // store stack top in memo; index is string arg
	opBinput              = 'q' //   "     "    "   "   " ;   "    " 1-byte arg
	opLongBinput          = 'r' //   "     "    "   "   " ;   "    " 4-byte arg
	opSetitem             = 's' // add key+value pair to dict
	opTuple               = 't' // build tuple from topmost stack items
	opEmptyTuple          = ')' // push empty tuple
	opSetitems            = 'u' // modify dict by adding topmost key+value pairs
	opBinfloat            = 'G' // push float; arg is 8-byte float encoding

	opTrue  = "I01\n" // not an opcode; see INT docs in pickletools.py
	opFalse = "I00\n" // not an opcode; see INT docs in pickletools.py

	// Protocol 2

	opProto    = '\x80' // identify pickle protocol
	opNewobj   = '\x81' // build object by applying cls.__new__ to argtuple
	opExt1     = '\x82' // push object from extension registry; 1-byte index
	opExt2     = '\x83' // ditto, but 2-byte index
	opExt4     = '\x84' // ditto, but 4-byte index
	opTuple1   = '\x85' // build 1-tuple from stack top
	opTuple2   = '\x86' // build 2-tuple from two topmost stack items
	opTuple3   = '\x87' // build 3-tuple from three topmost stack items
	opNewtrue  = '\x88' // push True
	opNewfalse = '\x89' // push False
	opLong1    = '\x8a' // push long from < 256 bytes
	opLong4    = '\x8b' // push really big long

	// Protocol 4
	opShortBinUnicode = '\x8c' // push short string; UTF-8 length < 256 bytes
	opMemoize         = '\x94' // store top of the stack in memo
	opFrame           = '\x95' // indicate the beginning of a new frame
)

var errNotImplemented = errors.New("unimplemented opcode")
var ErrInvalidPickleVersion = errors.New("invalid pickle version")
var errNoMarker = errors.New("no marker in stack")
var errStackUnderflow = errors.New("pickle: stack underflow")

type OpcodeError struct {
	Key byte
	Pos int
}

func (e OpcodeError) Error() string {
	return fmt.Sprintf("Unknown opcode %d (%c) at position %d: %q", e.Key, e.Key, e.Pos, e.Key)
}

// special marker
type mark struct{}

// None is a representation of Python's None.
type None struct{}

// Tuple is a representation of Python's tuple.
type Tuple []interface{}

// Decoder is a decoder for pickle streams.
type Decoder struct {
	r     *bufio.Reader
	stack []interface{}
	memo  map[string]interface{}

	// a reusable buffer that can be used by the various decoding functions
	// functions using this should call buf.Reset to clear the old contents
	buf bytes.Buffer

	// reusable buffer for readLine
	line  []byte
}

// NewDecoder constructs a new Decoder which will decode the pickle stream in r.
func NewDecoder(r io.Reader) *Decoder {
	reader := bufio.NewReader(r)
	return &Decoder{r: reader, stack: make([]interface{}, 0), memo: make(map[string]interface{})}
}

// Decode decodes the pickle stream and returns the result or an error.
func (d *Decoder) Decode() (interface{}, error) {

	insn := 0
loop:
	for {
		key, err := d.r.ReadByte()
		if err != nil {
			if err == io.EOF && insn != 0 {
				err = io.ErrUnexpectedEOF
			}
			return nil, err
		}

		insn++

		switch key {
		case opMark:
			d.mark()
		case opStop:
			break loop
		case opPop:
			_, err = d.pop()
		case opPopMark:
			d.popMark()
		case opDup:
			err = d.dup()
		case opFloat:
			err = d.loadFloat()
		case opInt:
			err = d.loadInt()
		case opBinint:
			err = d.loadBinInt()
		case opBinint1:
			err = d.loadBinInt1()
		case opLong:
			err = d.loadLong()
		case opBinint2:
			err = d.loadBinInt2()
		case opNone:
			err = d.loadNone()
		case opPersid:
			err = d.loadPersid()
		case opBinpersid:
			err = d.loadBinPersid()
		case opReduce:
			err = d.reduce()
		case opString:
			err = d.loadString()
		case opBinstring:
			err = d.loadBinString()
		case opShortBinstring:
			err = d.loadShortBinString()
		case opUnicode:
			err = d.loadUnicode()
		case opBinunicode:
			err = d.loadBinUnicode()
		case opAppend:
			err = d.loadAppend()
		case opBuild:
			err = d.build()
		case opGlobal:
			err = d.global()
		case opDict:
			err = d.loadDict()
		case opEmptyDict:
			err = d.loadEmptyDict()
		case opAppends:
			err = d.loadAppends()
		case opGet:
			err = d.get()
		case opBinget:
			err = d.binGet()
		case opInst:
			err = d.inst()
		case opLong1:
			err = d.loadLong1()
		case opNewfalse:
			err = d.loadBool(false)
		case opNewtrue:
			err = d.loadBool(true)
		case opLongBinget:
			err = d.longBinGet()
		case opList:
			err = d.loadList()
		case opEmptyList:
			d.push([]interface{}{})
		case opObj:
			err = d.obj()
		case opPut:
			err = d.loadPut()
		case opBinput:
			err = d.binPut()
		case opLongBinput:
			err = d.longBinPut()
		case opSetitem:
			err = d.loadSetItem()
		case opTuple:
			err = d.loadTuple()
		case opTuple1:
			err = d.loadTuple1()
		case opTuple2:
			err = d.loadTuple2()
		case opTuple3:
			err = d.loadTuple3()
		case opEmptyTuple:
			d.push(Tuple{})
		case opSetitems:
			err = d.loadSetItems()
		case opBinfloat:
			err = d.binFloat()
		case opFrame:
			err = d.loadFrame()
		case opShortBinUnicode:
			err = d.loadShortBinUnicode()
		case opMemoize:
			err = d.loadMemoize()
		case opProto:
			v, err := d.r.ReadByte()
			if err == nil && v != 2 {
				err = ErrInvalidPickleVersion
			}

		default:
			return nil, OpcodeError{key, insn}
		}

		if err != nil {
			if err == errNotImplemented {
				return nil, OpcodeError{key, insn}
			}
			// EOF from individual opcode decoder is unexpected end of stream
			if err == io.EOF {
				err = io.ErrUnexpectedEOF
			}
			return nil, err
		}
	}
	return d.pop()
}

// readLine reads next line from pickle stream
// returned line is valid only till next call to readLine
func (d *Decoder) readLine() ([]byte, error) {
	var (
		data     []byte
		isPrefix = true
		err      error
	)
	d.line = d.line[:0]
	for isPrefix {
		data, isPrefix, err = d.r.ReadLine()
		if err != nil {
			return d.line, err
		}
		d.line = append(d.line, data...)
	}
	return d.line, nil
}

// Push a marker
func (d *Decoder) mark() {
	d.push(mark{})
}

// Return the position of the topmost marker
func (d *Decoder) marker() (int, error) {
	m := mark{}
	for k := len(d.stack) - 1; k >= 0; k-- {
		if d.stack[k] == m {
			return k, nil
		}
	}
	return 0, errNoMarker
}

// Append a new value
func (d *Decoder) push(v interface{}) {
	d.stack = append(d.stack, v)
}

// Pop a value
// The returned error is errStackUnderflow if decoder stack is empty
func (d *Decoder) pop() (interface{}, error) {
	ln := len(d.stack) - 1
	if ln < 0 {
		return nil, errStackUnderflow
	}
	v := d.stack[ln]
	d.stack = d.stack[:ln]
	return v, nil
}

// Pop a value (when you know for sure decoder stack is not empty)
func (d *Decoder) xpop() interface{} {
	v, err := d.pop()
	if err != nil {
		panic(err)
	}
	return v
}

// Discard the stack through to the topmost marker
func (d *Decoder) popMark() error {
	return errNotImplemented
}

// Duplicate the top stack item
func (d *Decoder) dup() error {
	if len(d.stack) < 1 {
		return errStackUnderflow
	}
	d.stack = append(d.stack, d.stack[len(d.stack)-1])
	return nil
}

// Push a float
func (d *Decoder) loadFloat() error {
	line, err := d.readLine()
	if err != nil {
		return err
	}
	v, err := strconv.ParseFloat(string(line), 64)
	if err != nil {
		return err
	}
	d.push(interface{}(v))
	return nil
}

// Push an int
func (d *Decoder) loadInt() error {
	line, err := d.readLine()
	if err != nil {
		return err
	}

	var val interface{}

	switch string(line) {
	case opFalse[1:3]:
		val = false
	case opTrue[1:3]:
		val = true
	default:
		i, err := strconv.ParseInt(string(line), 10, 64)
		if err != nil {
			return err
		}
		val = i
	}

	d.push(val)
	return nil
}

// Push a four-byte signed int
func (d *Decoder) loadBinInt() error {
	var b [4]byte
	_, err := io.ReadFull(d.r, b[:])
	if err != nil {
		return err
	}
	v := binary.LittleEndian.Uint32(b[:])
	d.push(int64(v))
	return nil
}

// Push a 1-byte unsigned int
func (d *Decoder) loadBinInt1() error {
	b, err := d.r.ReadByte()
	if err != nil {
		return err
	}
	d.push(int64(b))
	return nil
}

// Push a long
func (d *Decoder) loadLong() error {
	line, err := d.readLine()
	if err != nil {
		return err
	}
	l := len(line)
	if l < 1 || line[l-1] != 'L' {
		return io.ErrUnexpectedEOF
	}
	v := new(big.Int)
	_, ok := v.SetString(string(line[:l-1]), 10)
	if !ok {
		return fmt.Errorf("pickle: loadLong: invalid string")
	}
	d.push(v)
	return nil
}

// Push a long1
func (d *Decoder) loadLong1() error {
	rawNum := []byte{}
	b, err := d.r.ReadByte()
	if err != nil {
		return err
	}
	length, err := decodeLong(string(b))
	if err != nil {
		return err
	}
	for i := 0; int64(i) < length.Int64(); i++ {
		b2, err := d.r.ReadByte()
		if err != nil {
			return err
		}
		rawNum = append(rawNum, b2)
	}
	decodedNum, err := decodeLong(string(rawNum))
	d.push(decodedNum)
	return nil
}

// Push a 2-byte unsigned int
func (d *Decoder) loadBinInt2() error {
	var b [2]byte
	_, err := io.ReadFull(d.r, b[:])
	if err != nil {
		return err
	}
	v := binary.LittleEndian.Uint16(b[:])
	d.push(int64(v))
	return nil
}

// Push None
func (d *Decoder) loadNone() error {
	d.push(None{})
	return nil
}

// Push a persistent object id
func (d *Decoder) loadPersid() error {
	return errNotImplemented
}

// Push a persistent object id from items on the stack
func (d *Decoder) loadBinPersid() error {
	return errNotImplemented
}

type Call struct {
	Callable Class
	Args     Tuple
}

func (d *Decoder) reduce() error {
	if len(d.stack) < 2 {
		return errStackUnderflow
	}
	xargs := d.xpop()
	xclass := d.xpop()
	args, ok := xargs.(Tuple)
	if !ok {
		return fmt.Errorf("pickle: reduce: invalid args: %T", xargs)
	}
	class, ok := xclass.(Class)
	if !ok {
		return fmt.Errorf("pickle: reduce: invalid class: %T", xclass)
	}
	d.stack = append(d.stack, Call{Callable: class, Args: args})
	return nil
}

func decodeStringEscape(b []byte) string {
	// TODO
	return string(b)
}

// Push a string
func (d *Decoder) loadString() error {
	line, err := d.readLine()
	if err != nil {
		return err
	}

	if len(line) < 2 {
		return io.ErrUnexpectedEOF
	}

	var delim byte
	switch line[0] {
	case '\'':
		delim = '\''
	case '"':
		delim = '"'
	default:
		return fmt.Errorf("invalid string delimiter: %c", line[0])
	}

	if line[len(line)-1] != delim {
		return io.ErrUnexpectedEOF
	}

	d.push(decodeStringEscape(line[1 : len(line)-1]))
	return nil
}

func (d *Decoder) loadBinString() error {
	var b [4]byte
	_, err := io.ReadFull(d.r, b[:])
	if err != nil {
		return err
	}
	v := binary.LittleEndian.Uint32(b[:])

	d.buf.Reset()
	d.buf.Grow(int(v))
	_, err = io.CopyN(&d.buf, d.r, int64(v))
	if err != nil {
		return err
	}
	d.push(d.buf.String())
	return nil
}

func (d *Decoder) loadShortBinString() error {
	b, err := d.r.ReadByte()
	if err != nil {
		return err
	}

	d.buf.Reset()
	d.buf.Grow(int(b))
	_, err = io.CopyN(&d.buf, d.r, int64(b))
	if err != nil {
		return err
	}
	d.push(d.buf.String())
	return nil
}

func (d *Decoder) loadUnicode() error {
	line, err := d.readLine()

	if err != nil {
		return err
	}
	sline := string(line)

	d.buf.Reset()
	d.buf.Grow(len(line)) // approximation

	for len(sline) > 0 {
		var r rune
		var err error
		for len(sline) > 0 && sline[0] == '\'' {
			d.buf.WriteByte(sline[0])
			sline = sline[1:]
		}
		if len(sline) == 0 {
			break
		}
		r, _, sline, err = strconv.UnquoteChar(sline, '\'')
		if err != nil {
			return err
		}
		d.buf.WriteRune(r)
	}
	if len(sline) > 0 {
		return fmt.Errorf("characters remaining after loadUnicode operation: %s", sline)
	}

	d.push(d.buf.String())
	return nil
}

func (d *Decoder) loadBinUnicode() error {
	var length int32
	for i := 0; i < 4; i++ {
		t, err := d.r.ReadByte()
		if err != nil {
			return err
		}
		length = length | (int32(t) << uint(8*i))
	}
	rawB := []byte{}
	for z := 0; int32(z) < length; z++ {
		n, err := d.r.ReadByte()
		if err != nil {
			return err
		}
		rawB = append(rawB, n)
	}
	d.push(string(rawB))
	return nil
}

func (d *Decoder) loadAppend() error {
	if len(d.stack) < 2 {
		return errStackUnderflow
	}
	v := d.xpop()
	l := d.stack[len(d.stack)-1]
	switch l.(type) {
	case []interface{}:
		l := l.([]interface{})
		d.stack[len(d.stack)-1] = append(l, v)
	default:
		return fmt.Errorf("pickle: loadAppend: expected a list, got %T", l)
	}
	return nil
}

func (d *Decoder) build() error {
	return errNotImplemented
}

type Class struct {
	Module, Name string
}

func (d *Decoder) global() error {
	module, err := d.readLine()
	if err != nil {
		return err
	}
	smodule := string(module)
	name, err := d.readLine()
	if err != nil {
		return err
	}
	sname := string(name)
	d.stack = append(d.stack, Class{Module: smodule, Name: sname})
	return nil
}

func (d *Decoder) loadDict() error {
	k, err := d.marker()
	if err != nil {
		return err
	}

	m := make(map[interface{}]interface{}, 0)
	items := d.stack[k+1:]
	if len(items) % 2 != 0 {
		return fmt.Errorf("pickle: loadDict: odd # of elements")
	}
	for i := 0; i < len(items); i += 2 {
		key := items[i]
		if !reflect.TypeOf(key).Comparable() {
			return fmt.Errorf("pickle: loadDict: invalid key type %T", key)
		}
		m[key] = items[i+1]
	}
	d.stack = append(d.stack[:k], m)
	return nil
}

func (d *Decoder) loadEmptyDict() error {
	m := make(map[interface{}]interface{}, 0)
	d.push(m)
	return nil
}

func (d *Decoder) loadAppends() error {
	k, err := d.marker()
	if err != nil {
		return err
	}
	if k < 1 {
		return errStackUnderflow
	}

	l := d.stack[k-1]
	switch l.(type) {
	case []interface{}:
		l := l.([]interface{})
		for _, v := range d.stack[k+1 : len(d.stack)] {
			l = append(l, v)
		}
		d.stack = append(d.stack[:k-1], l)
	default:
		return fmt.Errorf("pickle: loadAppends: expected a list, got %T", l)
	}
	return nil
}

func (d *Decoder) get() error {
	line, err := d.readLine()
	if err != nil {
		return err
	}
	v, ok := d.memo[string(line)]
	if !ok {
		return fmt.Errorf("pickle: memo: key error %q", line)
	}
	d.push(v)
	return nil
}

func (d *Decoder) binGet() error {
	b, err := d.r.ReadByte()
	if err != nil {
		return err
	}

	v, ok := d.memo[strconv.Itoa(int(b))]
	if !ok {
		return fmt.Errorf("pickle: memo: key error %d", b)
	}
	d.push(v)
	return nil
}

func (d *Decoder) inst() error {
	return errNotImplemented
}

func (d *Decoder) longBinGet() error {
	var b [4]byte
	_, err := io.ReadFull(d.r, b[:])
	if err != nil {
		return err
	}
	v := binary.LittleEndian.Uint32(b[:])
	vv, ok := d.memo[strconv.Itoa(int(v))]
	if !ok {
		return fmt.Errorf("pickle: memo: key error %d", v)
	}
	d.push(vv)
	return nil
}

func (d *Decoder) loadBool(b bool) error {
	d.push(b)
	return nil
}

func (d *Decoder) loadList() error {
	k, err := d.marker()
	if err != nil {
		return err
	}

	v := append([]interface{}{}, d.stack[k+1:]...)
	d.stack = append(d.stack[:k], v)
	return nil
}

func (d *Decoder) loadTuple() error {
	k, err := d.marker()
	if err != nil {
		return err
	}

	v := append(Tuple{}, d.stack[k+1:]...)
	d.stack = append(d.stack[:k], v)
	return nil
}

func (d *Decoder) loadTuple1() error {
	if len(d.stack) < 1 {
		return errStackUnderflow
	}
	k := len(d.stack) - 1
	v := append(Tuple{}, d.stack[k:]...)
	d.stack = append(d.stack[:k], v)
	return nil
}

func (d *Decoder) loadTuple2() error {
	if len(d.stack) < 2 {
		return errStackUnderflow
	}
	k := len(d.stack) - 2
	v := append(Tuple{}, d.stack[k:]...)
	d.stack = append(d.stack[:k], v)
	return nil
}

func (d *Decoder) loadTuple3() error {
	if len(d.stack) < 3 {
		return errStackUnderflow
	}
	k := len(d.stack) - 3
	v := append(Tuple{}, d.stack[k:]...)
	d.stack = append(d.stack[:k], v)
	return nil
}

func (d *Decoder) obj() error {
	return errNotImplemented
}

func (d *Decoder) loadPut() error {
	line, err := d.readLine()
	if err != nil {
		return err
	}
	if len(d.stack) < 1 {
		return errStackUnderflow
	}
	d.memo[string(line)] = d.stack[len(d.stack)-1]
	return nil
}

func (d *Decoder) binPut() error {
	if len(d.stack) < 1 {
		return errStackUnderflow
	}
	b, err := d.r.ReadByte()
	if err != nil {
		return err
	}

	d.memo[strconv.Itoa(int(b))] = d.stack[len(d.stack)-1]
	return nil
}

func (d *Decoder) longBinPut() error {
	if len(d.stack) < 1 {
		return errStackUnderflow
	}
	var b [4]byte
	_, err := io.ReadFull(d.r, b[:])
	if err != nil {
		return err
	}
	v := binary.LittleEndian.Uint32(b[:])
	d.memo[strconv.Itoa(int(v))] = d.stack[len(d.stack)-1]
	return nil
}

func (d *Decoder) loadSetItem() error {
	if len(d.stack) < 3 {
		return errStackUnderflow
	}
	v := d.xpop()
	k := d.xpop()
	m := d.stack[len(d.stack)-1]
	switch m := m.(type) {
	case map[interface{}]interface{}:
		if !reflect.TypeOf(k).Comparable() {
			return fmt.Errorf("pickle: loadSetItem: invalid key type %T", k)
		}
		m[k] = v
	default:
		return fmt.Errorf("pickle: loadSetItem: expected a map, got %T", m)
	}
	return nil
}

func (d *Decoder) loadSetItems() error {
	k, err := d.marker()
	if err != nil {
		return err
	}
	if k < 1 {
		return errStackUnderflow
	}

	l := d.stack[k-1]
	switch m := l.(type) {
	case map[interface{}]interface{}:
		if (len(d.stack) - (k + 1)) % 2 != 0 {
			return fmt.Errorf("pickle: loadSetItems: odd # of elements")
		}
		for i := k + 1; i < len(d.stack); i += 2 {
			key := d.stack[i]
			if !reflect.TypeOf(key).Comparable() {
				return fmt.Errorf("pickle: loadSetItems: invalid key type %T", key)
			}
			m[d.stack[i]] = d.stack[i+1]
		}
		d.stack = append(d.stack[:k-1], m)
	default:
		return fmt.Errorf("pickle: loadSetItems: expected a map, got %T", m)
	}
	return nil
}

func (d *Decoder) binFloat() error {
	var b [8]byte
	_, err := io.ReadFull(d.r, b[:])
	if err != nil {
		return err
	}
	u := binary.BigEndian.Uint64(b[:])
	d.stack = append(d.stack, math.Float64frombits(u))
	return nil
}

// loadFrame discards the framing opcode+information, this information is useful to do one large read (instead of many small reads)
// https://www.python.org/dev/peps/pep-3154/#framing
func (d *Decoder) loadFrame() error {
	var b [8]byte
	_, err := io.ReadFull(d.r, b[:])
	if err != nil {
		return err
	}
	return nil
}

func (d *Decoder) loadShortBinUnicode() error {
	b, err := d.r.ReadByte()
	if err != nil {
		return err
	}

	d.buf.Reset()
	d.buf.Grow(int(b))
	_, err = io.CopyN(&d.buf, d.r, int64(b))
	if err != nil {
		return err
	}
	d.push(d.buf.String())
	return nil
}

func (d *Decoder) loadMemoize() error {
	if len(d.stack) < 1 {
		return errStackUnderflow
	}
	d.memo[strconv.Itoa(len(d.memo))] = d.stack[len(d.stack)-1]
	return nil
}

// decodeLong takes a byte array of 2's compliment little-endian binary words and converts them
// to a big integer
func decodeLong(data string) (*big.Int, error) {
	decoded := big.NewInt(0)
	var negative bool
	switch x := len(data); {
	case x < 1:
		return decoded, nil
	case x > 1:
		if data[x-1] > 127 {
			negative = true
		}
		for i := x - 1; i >= 0; i-- {
			a := big.NewInt(int64(data[i]))
			for n := i; n > 0; n-- {
				a = a.Lsh(a, 8)
			}
			decoded = decoded.Add(a, decoded)
		}
	default:
		if data[0] > 127 {
			negative = true
		}
		decoded = big.NewInt(int64(data[0]))
	}

	if negative {
		// Subtract 1 from the number
		one := big.NewInt(1)
		decoded.Sub(decoded, one)

		// Flip the bits
		bytes := decoded.Bytes()
		for i := 0; i < len(bytes); i++ {
			bytes[i] = ^bytes[i]
		}
		decoded.SetBytes(bytes)

		// Mark as negative now conversion has been completed
		decoded.Neg(decoded)
	}
	return decoded, nil
}
