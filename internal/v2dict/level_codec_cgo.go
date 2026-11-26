//go:build cgo

package v2dict

/*
#cgo CFLAGS: -I${SRCDIR}/../../vendor/github.com/DataDog/zstd
#include "zstd.h"
#include <stdint.h>
static size_t decodeScanFrame(ZSTD_DCtx *ctx, uintptr_t dst, size_t dstSize, uintptr_t src, size_t srcSize) {
 return ZSTD_decompressDCtx(ctx, (void *)dst, dstSize, (const void *)src, srcSize);
}
*/
import "C"

import (
	"fmt"
	"runtime"
	"unsafe"

	"github.com/DataDog/zstd"
)

func compressBlock(dst, src []byte) ([]byte, error)   { return zstd.CompressLevel(dst, src, 3) }
func decompressBlock(dst, src []byte) ([]byte, error) { return zstd.Decompress(dst, src) }

// A scan owns one context from the vendored codec and frees it on return.
// ZSTD_decompressDCtx validates each independent frame without retaining names.
type scanDecoder struct{ ctx *C.ZSTD_DCtx }

func (d *scanDecoder) decode(dst, src []byte) ([]byte, error) {
	if len(src) == 0 || len(dst) == 0 {
		return nil, zstd.ErrEmptySlice
	}
	if d.ctx == nil {
		d.ctx = C.ZSTD_createDCtx()
		if d.ctx == nil {
			return nil, fmt.Errorf("v2dict: decoder allocation failed")
		}
	}
	n := C.decodeScanFrame(d.ctx, C.uintptr_t(uintptr(unsafe.Pointer(&dst[0]))), C.size_t(len(dst)), C.uintptr_t(uintptr(unsafe.Pointer(&src[0]))), C.size_t(len(src)))
	runtime.KeepAlive(src)
	runtime.KeepAlive(dst)
	if C.ZSTD_isError(n) != 0 {
		return nil, fmt.Errorf("v2dict: %s", C.GoString(C.ZSTD_getErrorName(n)))
	}
	return dst[:int(n)], nil
}
func (d *scanDecoder) close() {
	if d.ctx != nil {
		C.ZSTD_freeDCtx(d.ctx)
		d.ctx = nil
	}
}
