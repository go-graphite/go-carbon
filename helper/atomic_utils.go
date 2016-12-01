package helper

import "sync/atomic"

type StatCallback func(metric string, value float64)

func SendAndSubstractUint64(metric string, v *uint64, send StatCallback) {
	res := atomic.LoadUint64(v)
	atomic.AddUint64(v, ^uint64(res-1))
	send(metric, float64(res))
}

func SendUint64(metric string, v *uint64, send StatCallback) {
	res := atomic.LoadUint64(v)
	send(metric, float64(res))
}

func SendUint32(metric string, v *uint32, send StatCallback) {
	res := atomic.LoadUint32(v)
	send(metric, float64(res))
}

func SendAndSubstractUint32(metric string, v *uint32, send StatCallback) {
	res := atomic.LoadUint32(v)
	atomic.AddUint32(v, ^uint32(res-1))
	send(metric, float64(res))
}

func SendAndZeroIfNotUpdatedUint32(metric string, v *uint32, send StatCallback) {
	res := atomic.LoadUint32(v)
	atomic.CompareAndSwapUint32(v, res, 0)
	send(metric, float64(res))
}
