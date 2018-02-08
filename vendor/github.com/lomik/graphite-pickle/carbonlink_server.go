package pickle

import (
	"bytes"
	"encoding/binary"
	"math"
	"net"
	"strings"
	"time"

	"github.com/lomik/graphite-pickle/framing"
	"github.com/lomik/stop"
)

type CarbonlinkServer struct {
	stop.Struct
	listener          *net.TCPListener
	readTimeout       time.Duration
	writeTimeout      time.Duration
	cacheQueryHandler func(metric string) ([]DataPoint, error)
}

const InvalidRequestTypePickle = "\x80\x02}q\x00U\x05errorq\x01U\x14Invalid request typeq\x02s."
const InvalidRequestPickle = "\x80\x02}q\x00U\x05errorq\x01U\x0fInvalid requestq\x02s."
const InternalServerErrorPickle = "\x80\x02}q\x00U\x05errorq\x01U\x15Internal server errorq\x02s."

func NewCarbonlinkServer(readTimeout, writeTimeout time.Duration) *CarbonlinkServer {
	return &CarbonlinkServer{
		readTimeout:  readTimeout,
		writeTimeout: writeTimeout,
	}
}

func (cs *CarbonlinkServer) HandleCacheQuery(callback func(metric string) ([]DataPoint, error)) {
	cs.cacheQueryHandler = callback
}

// Addr returns binded socket address. For bind port 0 in tests
func (cs *CarbonlinkServer) Addr() net.Addr {
	if cs.listener == nil {
		return nil
	}
	return cs.listener.Addr()
}

func (cs *CarbonlinkServer) handleConnection(conn framing.Conn) {
	conn.Conn.(*net.TCPConn).SetLinger(0)
	defer conn.Close()

	for {
		conn.SetReadDeadline(time.Now().Add(cs.readTimeout))
		reqData, err := conn.ReadFrame()
		conn.SetReadDeadline(time.Time{})

		if err != nil {
			conn.Conn.(*net.TCPConn).SetLinger(0)
			break
		}

		req, err := UnmarshalCarbonlinkRequest(reqData)

		if err != nil {
			conn.Write([]byte(InvalidRequestPickle))
			break
		}
		if req != nil {
			if req.Type != "cache-query" {
				conn.Write([]byte(InvalidRequestTypePickle))
				break
			}

			if req.Type == "cache-query" {
				if cs.cacheQueryHandler != nil {
					data, err := cs.cacheQueryHandler(req.Metric)

					if err != nil {
						conn.Write([]byte(InternalServerErrorPickle))
						break
					}

					packed := packCacheQueryReply(data)
					if packed == nil {
						conn.Write([]byte(InternalServerErrorPickle))
						break
					}

					if _, err := conn.Write(packed); err != nil {
						break
					}
				}
			}
		}
	}
}

func (cs *CarbonlinkServer) Listen(addr *net.TCPAddr) error {
	return cs.StartFunc(func() error {
		listener, err := net.ListenTCP("tcp", addr)
		if err != nil {
			return err
		}

		cs.listener = listener

		cs.Go(func(exit chan struct{}) {
			select {
			case <-exit:
				listener.Close()
			}
		})

		cs.Go(func(exit chan struct{}) {
			defer listener.Close()

			for {
				conn, err := listener.Accept()
				if err != nil {
					if strings.Contains(err.Error(), "use of closed network connection") {
						break
					}
					continue
				}
				framedConn, _ := framing.NewConn(conn, byte(4), binary.BigEndian)
				framedConn.MaxFrameSize = 1048576 // 1MB max frame size for read and write
				go cs.handleConnection(*framedConn)
			}
		})

		return nil
	})
}

func pickleDataPoint(b *bytes.Buffer, p DataPoint) {
	var buf [8]byte
	s := buf[:]

	b.WriteByte('J')
	binary.LittleEndian.PutUint32(s, uint32(p.Timestamp))
	b.Write(s[:4])

	b.WriteByte('G')
	binary.BigEndian.PutUint64(s, uint64(math.Float64bits(p.Value)))
	b.Write(s)

	b.WriteByte('\x86') // assemble 2 element tuple
}

func packCacheQueryReply(data []DataPoint) []byte {

	numPoints := len(data)

	buf := bytes.NewBuffer([]byte("\x80\x02}U\ndatapoints]"))

	if numPoints > 1 {
		buf.WriteByte('(')
	}

	if data != nil {
		for _, point := range data {
			pickleDataPoint(buf, point)
		}
	}

	if numPoints == 0 {
		buf.Write([]byte{'s', '.'})
	} else if numPoints == 1 {
		buf.Write([]byte{'a', 's', '.'})
	} else if numPoints > 1 {
		buf.Write([]byte{'e', 's', '.'})
	}

	return buf.Bytes()
}
