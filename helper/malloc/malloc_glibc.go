//go:build linux && cgo

package malloc

/*
#cgo LDFLAGS: -ldl
// RTLD_DEFAULT is a GNU extension: glibc declares it only with _GNU_SOURCE.
#define _GNU_SOURCE
#include <dlfcn.h>
#include <stddef.h>

// mallinfo2 (glibc 2.33) has size_t fields; the older mallinfo's ints wrap
// past 2 GiB. It is looked up at run time so binaries built against an older
// glibc still run, without the stats.
struct gc_mallinfo2 {
	size_t arena, ordblks, smblks, hblks, hblkhd, usmblks, fsmblks, uordblks, fordblks, keepcost;
};
typedef struct gc_mallinfo2 (*gc_mallinfo2_fn)(void);

// malloc_trim is glibc-only (musl has none), so it is looked up at run time
// too; without it trim is a no-op.
typedef int (*gc_malloc_trim_fn)(size_t);

static gc_malloc_trim_fn gc_lookup_malloc_trim(void) {
	return (gc_malloc_trim_fn)dlsym(RTLD_DEFAULT, "malloc_trim");
}

static void gc_malloc_trim(gc_malloc_trim_fn fn) {
	if (fn != NULL) {
		fn(0);
	}
}

static gc_mallinfo2_fn gc_lookup_mallinfo2(void) {
	return (gc_mallinfo2_fn)dlsym(RTLD_DEFAULT, "mallinfo2");
}

static int gc_mallinfo2(gc_mallinfo2_fn fn, struct gc_mallinfo2 *out) {
	if (fn == NULL) {
		return 0;
	}
	*out = fn();
	return 1;
}
*/
import "C"

import "sync"

const supported = true

var mallocTrim = sync.OnceValue(func() C.gc_malloc_trim_fn { return C.gc_lookup_malloc_trim() })

func trim() { C.gc_malloc_trim(mallocTrim()) }

var mallinfo2 = sync.OnceValue(func() C.gc_mallinfo2_fn { return C.gc_lookup_mallinfo2() })

func stats() (Stats, bool) {
	var mi C.struct_gc_mallinfo2
	if C.gc_mallinfo2(mallinfo2(), &mi) == 0 {
		return Stats{}, false
	}
	return Stats{
		System: uint64(mi.arena) + uint64(mi.hblkhd),
		InUse:  uint64(mi.uordblks) + uint64(mi.hblkhd),
		Free:   uint64(mi.fordblks),
	}, true
}
