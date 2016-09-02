package cache

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"net"
	"strings"
	"time"

	"github.com/Sirupsen/logrus"

	"github.com/lomik/go-carbon/helper"
	"github.com/lomik/go-carbon/points"
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
	} else if b[0] == 'T' { //long string
		if len(b) >= 5 {
			sLen := int(binary.LittleEndian.Uint32(b[1:]))
			if len(b) >= 5+sLen {
				*buf = b[5+sLen:]
				return string(b[5 : 5+sLen]), true
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

var badErr error = fmt.Errorf("Bad pickle message")

// ParseCarbonlinkRequest from pickle encoded data
func ParseCarbonlinkRequest(d []byte) (*CarbonlinkRequest, error) {

	if !(expectBytes(&d, []byte("\x80\x02}")) && pickleMaybeMemo(&d) && expectBytes(&d, []byte("("))) {
		return nil, badErr
	}

	req := NewCarbonlinkRequest()

	var Metric, Type string
	var ok bool

	if expectBytes(&d, []byte("U\x06metric")) {
		if !pickleMaybeMemo(&d) {
			return nil, badErr
		}
		if Metric, ok = pickleGetStr(&d); !ok {
			return nil, badErr
		}

		if !(pickleMaybeMemo(&d) && expectBytes(&d, []byte("U\x04type")) && pickleMaybeMemo(&d)) {
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
	} else if expectBytes(&d, []byte("U\x04type")) {
		if !pickleMaybeMemo(&d) {
			return nil, badErr
		}

		if Type, ok = pickleGetStr(&d); !ok {
			return nil, badErr
		}

		if !(pickleMaybeMemo(&d) && expectBytes(&d, []byte("U\x06metric")) && pickleMaybeMemo(&d)) {
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

// ReadCarbonlinkRequest from socket/buffer
func ReadCarbonlinkRequest(reader io.Reader) ([]byte, error) {
	var msgLen uint32

	if err := binary.Read(reader, binary.BigEndian, &msgLen); err != nil {
		return nil, fmt.Errorf("Can't read message length: %s", err.Error())
	}

	if msgLen > 1024 {
		return nil, fmt.Errorf("Too big carbonlink request")
	}

	data := make([]byte, msgLen)

	if err := binary.Read(reader, binary.BigEndian, data); err != nil {
		return nil, fmt.Errorf("Can't read message body: %s", err.Error())
	}

	return data, nil
}

// CarbonlinkListener receive cache Carbonlinkrequests from graphite-web
type CarbonlinkListener struct {
	helper.Stoppable
	queryChan    chan *Query
	readTimeout  time.Duration
	queryTimeout time.Duration
	tcpListener  *net.TCPListener
}

// NewCarbonlinkListener create new instance of CarbonlinkListener
func NewCarbonlinkListener(queryChan chan *Query) *CarbonlinkListener {
	return &CarbonlinkListener{
		queryChan:    queryChan,
		readTimeout:  30 * time.Second,
		queryTimeout: 100 * time.Millisecond,
	}
}

// SetReadTimeout for read request from client
func (listener *CarbonlinkListener) SetReadTimeout(timeout time.Duration) {
	listener.readTimeout = timeout
}

// SetQueryTimeout for queries to cache
func (listener *CarbonlinkListener) SetQueryTimeout(timeout time.Duration) {
	listener.queryTimeout = timeout
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

func packReply(query *Query) []byte {

	numPoints := 0

	if query != nil {
		numPoints += len(query.InFlightData)
		if query.CacheData != nil {
			numPoints += len(query.CacheData.Data)
		}
	}

	buf := bytes.NewBuffer([]byte("\x00\x00\x00\x00\x80\x02}U\ndatapoints]"))

	if numPoints > 1 {
		buf.WriteByte('(')
	}

	if query != nil && query.InFlightData != nil {
		for _, points := range query.InFlightData {
			for _, item := range points.Data {
				picklePoint(buf, item)
			}
		}
	}

	if query != nil && query.CacheData != nil {
		for _, item := range query.CacheData.Data {
			picklePoint(buf, item)
		}
	}

	if numPoints == 0 {
		buf.Write([]byte{'s', '.'})
	} else if numPoints == 1 {
		buf.Write([]byte{'a', 's', '.'})
	} else if numPoints > 1 {
		buf.Write([]byte{'e', 's', '.'})
	}

	result := buf.Bytes()
	binary.BigEndian.PutUint32(result[:4], uint32(buf.Len()-4))

	return result
}

func (listener *CarbonlinkListener) HandleConnection(conn net.Conn) {
	defer conn.Close()
	reader := bufio.NewReader(conn)

	for {
		conn.SetReadDeadline(time.Now().Add(listener.readTimeout))

		reqData, err := ReadCarbonlinkRequest(reader)
		if err != nil {
			conn.(*net.TCPConn).SetLinger(0)
			logrus.Debugf("[carbonlink] read carbonlink request from %s: %s", conn.RemoteAddr().String(), err.Error())
			break
		}

		req, err := ParseCarbonlinkRequest(reqData)

		if err != nil {
			conn.(*net.TCPConn).SetLinger(0)
			logrus.Warningf("[carbonlink] parse carbonlink request from %s: %s", conn.RemoteAddr().String(), err.Error())
			break
		}
		if req != nil {
			if req.Type != "cache-query" {
				logrus.Warningf("[carbonlink] unknown query type: %#v", req.Type)
				buf := bytes.NewBuffer([]byte(fmt.Sprintf("\x00\x00\x00\x00\x80\x02}q\x00U\x05errorq\x01U\x1aInvalid request type %qq\x02s.", req.Type)))
				result := buf.Bytes()
				binary.BigEndian.PutUint32(result[:4], uint32(buf.Len()-4))
				conn.Write(result)
				break
			}

			if req.Type == "cache-query" {
				query := NewQuery(req.Metric)
				listener.queryChan <- query

				select {
				case <-query.Wait:
					// pass
				case <-time.After(listener.queryTimeout):
					logrus.Infof("[carbonlink] Cache no reply (%s timeout)", listener.queryTimeout)
					query = nil // empty reply
				}

				packed := packReply(query)
				if packed == nil {
					break
				}
				if _, err := conn.Write(packed); err != nil {
					logrus.Infof("[carbonlink] reply error: %s", err)
					break
				}
				// pp.Println(reply)
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
					logrus.Warningf("[carbonlink] Failed to accept connection: %s", err)
					continue
				}

				go listener.HandleConnection(conn)
			}
		})

		return nil
	})
}
