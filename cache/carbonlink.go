package cache

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"strings"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/hydrogen18/stalecucumber"
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

// ParseCarbonlinkRequest from pickle encoded data
func ParseCarbonlinkRequest(data []byte) (*CarbonlinkRequest, error) {
	reader := bytes.NewReader(data)
	req := NewCarbonlinkRequest()

	if err := stalecucumber.UnpackInto(req).From(stalecucumber.Unpickle(reader)); err != nil {
		return nil, err
	}

	return req, nil
}

// ReadCarbonlinkRequest from socket/buffer and unpack
func ReadCarbonlinkRequest(reader io.Reader) (*CarbonlinkRequest, error) {
	var msgLen uint32

	if err := binary.Read(reader, binary.BigEndian, &msgLen); err != nil {
		return nil, fmt.Errorf("[carbonlink] Can't read message length: %s", err.Error())
	}

	data := make([]byte, msgLen)

	if err := binary.Read(reader, binary.BigEndian, data); err != nil {
		return nil, fmt.Errorf("[carbonlink] Can't read message body: %s", err.Error())
	}

	return ParseCarbonlinkRequest(data)
}

// CarbonlinkListener receive cache Carbonlinkrequests from graphite-web
type CarbonlinkListener struct {
	queryChan    chan *Query
	exit         chan bool
	readTimeout  time.Duration
	queryTimeout time.Duration
}

// NewCarbonlinkListener create new instance of CarbonlinkListener
func NewCarbonlinkListener(queryChan chan *Query) *CarbonlinkListener {
	return &CarbonlinkListener{
		exit:         make(chan bool),
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

func (listener *CarbonlinkListener) packReply(reply *Reply) []byte {
	buf := new(bytes.Buffer)

	var datapoints []interface{}

	for _, item := range reply.Points.Data {
		datapoints = append(datapoints, stalecucumber.NewTuple(item.Timestamp, item.Value))
	}

	r := make(map[string][]interface{})
	r["datapoints"] = datapoints

	_, err := stalecucumber.NewPickler(buf).Pickle(r)

	if err != nil { // unknown wtf error
		return nil
	}

	resultBuf := new(bytes.Buffer)
	if err := binary.Write(resultBuf, binary.BigEndian, int32(buf.Len())); err != nil {
		return nil
	}

	resultBuf.Write(buf.Bytes())

	return resultBuf.Bytes()
}

func (listener *CarbonlinkListener) handleConnection(conn net.Conn) {
	defer conn.Close()
	reader := bufio.NewReader(conn)

	for {
		conn.SetReadDeadline(time.Now().Add(listener.readTimeout))

		req, err := ReadCarbonlinkRequest(reader)

		if err != nil {
			break
		}
		if req != nil {
			if req.Type != "cache-query" {
				logrus.Warningf("[carbonlink] unknown query type: %#v", req.Type)
				break
			}

			if req.Type == "cache-query" {
				cacheReq := NewQuery(req.Metric)
				listener.queryChan <- cacheReq

				var reply *Reply

				select {
				case reply = <-cacheReq.ReplyChan:
				case <-time.After(listener.queryTimeout):
					logrus.Infof("[carbonlink] Cache no reply (%s timeout)", listener.queryTimeout)
					reply = NewReply()
				}
				packed := listener.packReply(reply)
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

// Listen bind port. Receive messages and send to out channel
func (listener *CarbonlinkListener) Listen(addr *net.TCPAddr) error {
	sock, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return err
	}

	go func() {
		select {
		case <-listener.exit:
			sock.Close()
		}
	}()

	go func() {
		defer sock.Close()

		for {

			conn, err := sock.Accept()
			if err != nil {
				if strings.Contains(err.Error(), "use of closed network connection") {
					break
				}
				logrus.Warningf("[carbonlink] Failed to accept connection: %s", err)
				continue
			}

			go listener.handleConnection(conn)
		}

	}()

	return nil
}

// Stop all listeners
func (listener *CarbonlinkListener) Stop() {
	close(listener.exit)
}
