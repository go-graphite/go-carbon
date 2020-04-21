package cache

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"net"
	"strings"
	"time"

	"go.uber.org/zap"

	"github.com/lomik/go-carbon/helper"
	"github.com/lomik/go-carbon/points"
	"github.com/lomik/graphite-pickle/framing"
	"github.com/lomik/zapwriter"
)

// CarbonlinkRequest ...
type CarbonlinkRequest struct {
	Type   string
	Metric string
	Key    string
	Value  string
}

// NewCarbonlinkRequest creates instance of CarbonlinkRequest
func NewCarbonlinkRequest() *CarbonlinkRequest {
	return &CarbonlinkRequest{}
}

func pickleMaybeMemo(b *[]byte) bool { //"consumes" memo tokens
	if len(*b) > 1 && (*b)[0] == 'q' {
		*b = (*b)[2:]
	} else if len(*b) > 1 && bytes.Index(*b, []byte("\x94")) == 0 {
		*b = (*b)[1:]
	}
	return true
}

func pickleGetStr(buf *[]byte) (string, bool) {
	if len(*buf) == 0 {
		return "", false
	}
	b := *buf

	if b[0] == 'U' { // short string
		if len(b) >= 2 {
			sLen := int(uint8(b[1]))
			if len(b) >= 2+sLen {
				*buf = b[2+sLen:]
				return string(b[2 : 2+sLen]), true
			}
		}
	} else if b[0] == 'T' || b[0] == 'X' { //long string or utf8 string
		if len(b) >= 5 {
			sLen := int(binary.LittleEndian.Uint32(b[1:]))
			if len(b) >= 5+sLen {
				*buf = b[5+sLen:]
				return string(b[5 : 5+sLen]), true
			}
		}
	} else if bytes.Index(b, []byte("\x8c")) == 0 {
		if len(b) >= 2 {
			sLen := int(uint8(b[1]))
			if len(b) >= 2+sLen {
				*buf = b[2+sLen:]
				return string(b[2 : 2+sLen]), true
			}
		}
	}
	return "", false
}

func expectBytes(b *[]byte, v []byte) bool {
	if bytes.Index(*b, v) == 0 {
		*b = (*b)[len(v):]
		return true
	} else {
		return false
	}
}

func protocolFourOrFiveFirstBytes(b *[]byte) bool {
	// Parse and drop first 12 bytes of Pickle that is using protocol 4 and 5
	if (bytes.Index(*b, []byte("\x80\x04")) == 0 ||
		bytes.Index(*b, []byte("\x80\x05")) == 0) && bytes.Index(*b, []byte("}")) == 11 {
		*b = (*b)[12:]
		return true
	} else {
		return false
	}
}

var badErr error = fmt.Errorf("Bad pickle message")

// ParseCarbonlinkRequest from pickle encoded data
func ParseCarbonlinkRequest(d []byte) (*CarbonlinkRequest, error) {

	var unicodePklMetricBytes, unicodePklTypeBytes []byte

	if (expectBytes(&d, []byte("\x80\x02}")) ||
		expectBytes(&d, []byte("\x80\x03}"))) && pickleMaybeMemo(&d) && expectBytes(&d, []byte("(")) {
		// message using pickle protocol 2 or 3
		unicodePklMetricBytes = []byte("X\x06\x00\x00\x00metric")
		unicodePklTypeBytes = []byte("X\x04\x00\x00\x00type")
	} else if protocolFourOrFiveFirstBytes(&d) && pickleMaybeMemo(&d) && expectBytes(&d, []byte("(")) {
		// message using pickle protocol 4, or 5
		unicodePklMetricBytes = []byte("\x8c\x06metric")
		unicodePklTypeBytes = []byte("\x8c\x04type")
	} else {
		return nil, fmt.Errorf("Bad pickle message, unknown pickle protocol")
	}

	req := NewCarbonlinkRequest()

	var Metric, Type string
	var ok bool

	if expectBytes(&d, []byte("U\x06metric")) || expectBytes(&d, unicodePklMetricBytes) {
		if !pickleMaybeMemo(&d) {
			return nil, badErr
		}
		if Metric, ok = pickleGetStr(&d); !ok {
			return nil, badErr
		}

		if !(pickleMaybeMemo(&d) && (expectBytes(&d, []byte("U\x04type")) ||
			expectBytes(&d, unicodePklTypeBytes)) && pickleMaybeMemo(&d)) {
			return nil, badErr
		}

		if Type, ok = pickleGetStr(&d); !ok {
			return nil, badErr
		}

		if !pickleMaybeMemo(&d) {
			return nil, badErr
		}
		req.Metric = Metric
		req.Type = Type
	} else if expectBytes(&d, []byte("U\x04type")) || expectBytes(&d, unicodePklTypeBytes) {
		if !pickleMaybeMemo(&d) {
			return nil, badErr
		}

		if Type, ok = pickleGetStr(&d); !ok {
			return nil, badErr
		}

		if !(pickleMaybeMemo(&d) && (expectBytes(&d, []byte("U\x06metric")) ||
			expectBytes(&d, unicodePklMetricBytes)) && pickleMaybeMemo(&d)) {
			return nil, badErr
		}

		if Metric, ok = pickleGetStr(&d); !ok {
			return nil, badErr
		}

		if !pickleMaybeMemo(&d) {
			return nil, badErr
		}
		req.Metric = Metric
		req.Type = Type
	} else {
		return nil, badErr
	}

	return req, nil
}

// CarbonlinkListener receive cache Carbonlinkrequests from graphite-web
type CarbonlinkListener struct {
	helper.Stoppable
	cache       *Cache
	readTimeout time.Duration
	tcpListener *net.TCPListener
}

// NewCarbonlinkListener create new instance of CarbonlinkListener
func NewCarbonlinkListener(cache *Cache) *CarbonlinkListener {
	return &CarbonlinkListener{
		cache:       cache,
		readTimeout: 30 * time.Second,
	}
}

// SetReadTimeout for read request from client
func (listener *CarbonlinkListener) SetReadTimeout(timeout time.Duration) {
	listener.readTimeout = timeout
}

func pickleWriteMemo(b *bytes.Buffer, memo *uint32) {
	if *memo < 256 {
		b.WriteByte('q')
		b.WriteByte(uint8(*memo))
	} else {
		b.WriteByte('r')
		var buf [4]byte
		s := buf[:]
		binary.LittleEndian.PutUint32(s, *memo)
		b.Write(s)
	}
	*memo += 1
}

func picklePoint(b *bytes.Buffer, p points.Point) {
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

func packReply(data []points.Point) []byte {

	numPoints := len(data)

	buf := bytes.NewBuffer([]byte("\x80\x02}U\ndatapoints]"))

	if numPoints > 1 {
		buf.WriteByte('(')
	}

	if data != nil {
		for _, point := range data {
			picklePoint(buf, point)
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

func (listener *CarbonlinkListener) HandleConnection(conn framing.Conn) {
	defer conn.Close()
	logger := zapwriter.Logger("carbonlink").With(zap.String("peer", conn.RemoteAddr().String()))

	for {
		conn.SetReadDeadline(time.Now().Add(listener.readTimeout))
		reqData, err := conn.ReadFrame()

		if err != nil {
			conn.Conn.(*net.TCPConn).SetLinger(0)
			logger.Debug("request read failed", zap.Error(err))
			break
		}

		req, err := ParseCarbonlinkRequest(reqData)

		if err != nil {
			conn.Conn.(*net.TCPConn).SetLinger(0)
			logger.Warn("request parse failed", zap.Error(err))
			break
		}
		if req != nil {
			if req.Type != "cache-query" {
				logger.Warn("unknown query", zap.String("type", req.Type))
				conn.Write([]byte(fmt.Sprintf("\x80\x02}q\x00U\x05errorq\x01U\x1aInvalid request type %qq\x02s.", req.Type)))
				break
			}

			if req.Type == "cache-query" {
				data := listener.cache.Get(req.Metric)

				packed := packReply(data)
				if packed == nil {
					break
				}
				if _, err := conn.Write(packed); err != nil {
					logger.Info("reply error", zap.Error(err))
					break
				}
			}
		}
	}
}

// Addr returns binded socket address. For bind port 0 in tests
func (listener *CarbonlinkListener) Addr() net.Addr {
	if listener.tcpListener == nil {
		return nil
	}
	return listener.tcpListener.Addr()
}

// Listen bind port. Receive messages and send to out channel
func (listener *CarbonlinkListener) Listen(addr *net.TCPAddr) error {
	return listener.StartFunc(func() error {
		tcpListener, err := net.ListenTCP("tcp", addr)
		if err != nil {
			return err
		}

		listener.tcpListener = tcpListener

		listener.Go(func(exit chan bool) {
			select {
			case <-exit:
				tcpListener.Close()
			}
		})

		listener.Go(func(exit chan bool) {
			defer tcpListener.Close()

			for {
				conn, err := tcpListener.Accept()
				if err != nil {
					if strings.Contains(err.Error(), "use of closed network connection") {
						break
					}
					zapwriter.Logger("carbonlink").Error("failed to accept connection", zap.Error(err))
					continue
				}
				framedConn, _ := framing.NewConn(conn, byte(4), binary.BigEndian)
				framedConn.MaxFrameSize = 1048576 // 1MB max frame size for read and write
				go listener.HandleConnection(*framedConn)
			}
		})

		return nil
	})
}
