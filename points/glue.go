package points

import (
	"bytes"
	"fmt"
	"time"
)

func Glue(exit chan bool, in chan *Points, chunkSize int, chunkTimeout time.Duration, callback func([]byte)) {
	var p *Points
	var ok bool

	buf := bytes.NewBuffer(nil)

	flush := func() {
		if buf.Len() == 0 {
			return
		}
		callback(buf.Bytes())
		buf = bytes.NewBuffer(nil)
	}

	ticker := time.NewTicker(chunkTimeout)
	defer ticker.Stop()

	for {
		p = nil
		select {
		case p, ok = <-in:
			if !ok { // in chan closed
				flush()
				return
			}
			// pass
		case <-ticker.C:
			flush()
		case <-exit:
			return
		}

		if p == nil {
			continue
		}

		for _, d := range p.Data {
			s := fmt.Sprintf("%s %v %v\n", p.Metric, d.Value, d.Timestamp)

			if buf.Len()+len(s) > chunkSize {
				flush()
			}
			buf.Write([]byte(s))
		}
	}

}
