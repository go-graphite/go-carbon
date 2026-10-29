//go:build !(linux && cgo)

package malloc

const supported = false

func trim() {}

func stats() (Stats, bool) { return Stats{}, false }
