package v2core

import (
	"testing"
	"unsafe"
)

// BenchmarkPackedRowEscape is the head intake cost per point by row ID kind:
// a 32-bit NumericID, a raw packed ID (an escape per row) and a packed
// identity's dense slot (rowID, what Outbox.Start caches).
func BenchmarkPackedRowEscape(b *testing.B) {
	const names, pointsPer = 100000, 4
	handles := make([]PolicyIdentity, names)
	for n := range handles {
		handles[n] = PolicyIdentity{id: PackID(1, uint16(n%4096), uint32(n/4096)), slot: uint32(n + 1)}
	}
	for _, arm := range []string{"numeric", "packed", "slot"} {
		b.Run("ids="+arm, func(b *testing.B) {
			var p pendingRows
			var bytesPerRow float64
			for i := 0; i < b.N; i++ {
				p.reset()
				order := uint64(0)
				for n := range names {
					var id uint64
					switch arm {
					case "numeric":
						id = uint64(n + 1)
					case "packed":
						id = handles[n].id
					default:
						id = handles[n].rowID()
					}
					order++
					p.startApplication(order, 1, 1000, pointsPer)
					for k := range pointsPer {
						p.addRow(id, int64(1000+k*10), uint64(k), order)
					}
				}
				p.closePacket()
				bytesPerRow = float64(len(p.rows)*int(unsafe.Sizeof(pendingRow{}))+len(p.escapes)*int(unsafe.Sizeof(pendingEscape{}))) / float64(len(p.rows))
			}
			b.ReportMetric(bytesPerRow, "B/row")
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*names*pointsPer), "ns/row")
		})
	}
}
