package carbonserver

import (
	"encoding/binary"
	"math"
)

// Fake single interval set for graphite
type IntervalSet struct {
	Start int32
	End   int32
}

func (i *IntervalSet) MarshalPickle() ([]byte, error) {
	//     0: (    MARK
	//     1: c        GLOBAL     'graphite.intervals IntervalSet'
	//    33: o        OBJ        (MARK at 0)
	//    34: }    EMPTY_DICT
	//    35: (    MARK
	//    36: U        SHORT_BINSTRING 'intervals'
	//    47: ]        EMPTY_LIST
	//    48: (        MARK
	//    49: c            GLOBAL     'graphite.intervals Interval'
	//    78: o            OBJ        (MARK at 48)
	//    79: }        EMPTY_DICT
	//    80: (        MARK
	//    81: U            SHORT_BINSTRING 'start'
	//    88: G            BINFLOAT   1322087998.393128
	//    97: U            SHORT_BINSTRING 'size'
	//   103: G            BINFLOAT   157679977.1475761
	//   112: U            SHORT_BINSTRING 'end'
	//   117: G            BINFLOAT   1479767975.540704
	//   126: U            SHORT_BINSTRING 'tuple'
	//   133: G            BINFLOAT   1322087998.393128
	//   142: G            BINFLOAT   1479767975.540704
	//   151: \x86         TUPLE2
	//   152: u            SETITEMS   (MARK at 80)
	//   153: b        BUILD
	//   154: a        APPEND
	//   155: U        SHORT_BINSTRING 'size'
	//   161: G        BINFLOAT   157679977.1475761
	//   170: u        SETITEMS   (MARK at 35)
	//   171: b    BUILD
	//   172: .    STOP
	b := []byte("(cgraphite.intervals\nIntervalSet\no}(U\tintervals](cgraphite.intervals\nInterval\no}(U\x05startGA\xd3\xb3]\x8f\x99)\x02U\x04sizeGA\xa2\xcc\x02\xd2K\x8f\x18U\x03endGA\xd6\x0c\xdd\xe9\xe2\x9a\xe5U\x05tupleGA\xd3\xb3]\x8f\x99)\x02GA\xd6\x0c\xdd\xe9\xe2\x9a\xe5\x86ubaU\x04sizeGA\xa2\xcc\x02\xd2K\x8f\x18ub")

	binary.BigEndian.PutUint64(b[89:97], uint64(math.Float64bits(float64(i.Start))))
	binary.BigEndian.PutUint64(b[104:112], uint64(math.Float64bits(float64(i.End-i.Start))))
	binary.BigEndian.PutUint64(b[118:126], uint64(math.Float64bits(float64(i.End))))
	binary.BigEndian.PutUint64(b[134:142], uint64(math.Float64bits(float64(i.Start))))
	binary.BigEndian.PutUint64(b[143:151], uint64(math.Float64bits(float64(i.End))))
	binary.BigEndian.PutUint64(b[162:170], uint64(math.Float64bits(float64(i.End-i.Start))))

	return b, nil
}
