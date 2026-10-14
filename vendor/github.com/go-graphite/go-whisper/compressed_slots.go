package whisper

// The idle pool retains at most 32 MiB on a 64-bit process. A scratch buffer
// belongs to one filter call and is returned only after all slot lookups finish.
const (
	minCompressedSlotOwners = 64
	maxCompressedSlotOwners = 128 * 1024
)

type compressedSlotOwners struct {
	intervals []int
}

var compressedSlotOwnersPool = make(chan *compressedSlotOwners, 32)

func acquireCompressedSlotOwners(size int) *compressedSlotOwners {
	var owners *compressedSlotOwners
	select {
	case owners = <-compressedSlotOwnersPool:
	default:
		owners = &compressedSlotOwners{}
	}
	if cap(owners.intervals) < size {
		owners.intervals = make([]int, size)
	} else {
		owners.intervals = owners.intervals[:size]
		for i := range owners.intervals {
			owners.intervals[i] = 0
		}
	}
	return owners
}

func releaseCompressedSlotOwners(owners *compressedSlotOwners) {
	if cap(owners.intervals) > maxCompressedSlotOwners {
		return
	}
	select {
	case compressedSlotOwnersPool <- owners:
	default:
	}
}
