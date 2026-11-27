//go:build cgo

package v2part

/*
#include <stddef.h>
typedef struct ZSTD_CCtx_s ZSTD_CCtx;
extern ZSTD_CCtx* ZSTD_createCCtx(void);
extern size_t ZSTD_freeCCtx(ZSTD_CCtx*);
extern size_t ZSTD_compressBound(size_t);
extern size_t ZSTD_compressCCtx(ZSTD_CCtx*, void*, size_t, const void*, size_t, int);
extern unsigned ZSTD_isError(size_t);
extern const char* ZSTD_getErrorName(size_t);
*/
import "C"

import (
	"errors"
	"runtime"
	"unsafe"

	// Link the already-vendored Zstd implementation used by Pebble. The
	// declarations above are its stable public context/compression API.
	_ "github.com/DataDog/zstd"
)

type denseEncoder struct{ context *C.ZSTD_CCtx }

func newDenseEncoder() (*denseEncoder, error) {
	e := &denseEncoder{context: C.ZSTD_createCCtx()}
	if e.context == nil {
		return nil, errors.New("allocate zstd compression context")
	}
	// sync.Pool may drop an idle workspace during GC. Its native context must
	// then be released too; no live writer shares this encoder.
	runtime.SetFinalizer(e, func(e *denseEncoder) { C.ZSTD_freeCCtx(e.context) })
	return e, nil
}

func (e *denseEncoder) append(src, dst []byte, level int) ([]byte, error) {
	prefix := len(dst)
	bound := int(C.ZSTD_compressBound(C.size_t(len(src))))
	if cap(dst)-prefix < bound {
		buf := make([]byte, prefix, prefix+bound)
		copy(buf, dst)
		dst = buf
	}
	dst = dst[:prefix+bound]
	var source unsafe.Pointer
	if len(src) != 0 {
		source = unsafe.Pointer(&src[0])
	}
	n := C.ZSTD_compressCCtx(e.context, unsafe.Pointer(&dst[prefix]), C.size_t(bound), source, C.size_t(len(src)), C.int(level))
	runtime.KeepAlive(e)
	if C.ZSTD_isError(n) != 0 {
		return nil, errors.New(C.GoString(C.ZSTD_getErrorName(n)))
	}
	return dst[:prefix+int(n)], nil
}
