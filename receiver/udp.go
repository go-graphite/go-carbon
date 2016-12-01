package receiver

import (
	"bytes"
	"io"
	"net"
	"strings"
	"sync/atomic"
	"time"

	"github.com/lomik/go-carbon/helper"
	"github.com/lomik/go-carbon/points"

	"github.com/Sirupsen/logrus"
)

// UDP receive metrics from UDP socket
type UDP struct {
	helper.Stoppable
	out                func(*points.Points)
	name               string
	metricsReceived    uint32
	incompleteReceived uint32
	errors             uint32
	logIncomplete      bool
	conn               *net.UDPConn
	buffer             chan *points.Points
}

// Name returns receiver name (for store internal metrics)
func (rcv *UDP) Name() string {
	return rcv.name
}

type incompleteRecord struct {
	deadline time.Time
	data     []byte
}

// incompleteStorage store incomplete lines
type incompleteStorage struct {
	Records   map[string]*incompleteRecord
	Expires   time.Duration
	NextPurge time.Time
	MaxSize   int
}

func newIncompleteStorage() *incompleteStorage {
	return &incompleteStorage{
		Records:   make(map[string]*incompleteRecord, 0),
		Expires:   5 * time.Second,
		MaxSize:   10000,
		NextPurge: time.Now().Add(time.Second),
	}
}

func (storage *incompleteStorage) store(addr string, data []byte) {
	storage.Records[addr] = &incompleteRecord{
		deadline: time.Now().Add(storage.Expires),
		data:     data,
	}
	storage.checkAndClear()
}

func (storage *incompleteStorage) pop(addr string) []byte {
	if record, ok := storage.Records[addr]; ok {
		delete(storage.Records, addr)
		if record.deadline.Before(time.Now()) {
			return nil
		}
		return record.data
	}
	return nil
}

func (storage *incompleteStorage) purge() {
	now := time.Now()
	for key, record := range storage.Records {
		if record.deadline.Before(now) {
			delete(storage.Records, key)
		}
	}
	storage.NextPurge = time.Now().Add(time.Second)
}

func (storage *incompleteStorage) checkAndClear() {
	if len(storage.Records) < storage.MaxSize {
		return
	}
	if storage.NextPurge.After(time.Now()) {
		return
	}
	storage.purge()
}

// Addr returns binded socket address. For bind port 0 in tests
func (rcv *UDP) Addr() net.Addr {
	if rcv.conn == nil {
		return nil
	}
	return rcv.conn.LocalAddr()
}

func logIncomplete(peer *net.UDPAddr, message []byte, lastLine []byte) {
	p1 := bytes.IndexByte(message, 0xa) // find first "\n"

	if p1 != -1 && p1+len(lastLine) < len(message)-10 { // print short version
		logrus.Warningf(
			"[udp] incomplete message from %s: \"%s\\n...(%d bytes)...\\n%s\"",
			peer.String(),
			string(message[:p1]),
			len(message)-p1-len(lastLine)-2,
			string(lastLine),
		)
	} else { // print full
		logrus.Warningf(
			"[udp] incomplete message from %s: %#v",
			peer.String(),
			string(message),
		)
	}
}

func (rcv *UDP) Stat(send helper.StatCallback) {
	metricsReceived := atomic.LoadUint32(&rcv.metricsReceived)
	atomic.AddUint32(&rcv.metricsReceived, -metricsReceived)
	send("metricsReceived", float64(metricsReceived))

	incompleteReceived := atomic.LoadUint32(&rcv.incompleteReceived)
	atomic.AddUint32(&rcv.incompleteReceived, -incompleteReceived)
	send("incompleteReceived", float64(incompleteReceived))

	errors := atomic.LoadUint32(&rcv.errors)
	atomic.AddUint32(&rcv.errors, -errors)
	send("errors", float64(errors))

	if rcv.buffer != nil {
		send("bufferLen", float64(len(rcv.buffer)))
		send("bufferCap", float64(cap(rcv.buffer)))
	}
}

func (rcv *UDP) receiveWorker(exit chan bool) {
	defer rcv.conn.Close()

	var buf [65535]byte

	var data *bytes.Buffer

	lines := newIncompleteStorage()

	for {
		rlen, peer, err := rcv.conn.ReadFromUDP(buf[:])
		if err != nil {
			if strings.Contains(err.Error(), "use of closed network connection") {
				break
			}
			atomic.AddUint32(&rcv.errors, 1)
			logrus.Error(err)
			continue
		}

		prev := lines.pop(peer.String())

		if prev != nil {
			data = bytes.NewBuffer(prev)
			data.Write(buf[:rlen])
		} else {
			data = bytes.NewBuffer(buf[:rlen])
		}

		for {
			line, err := data.ReadBytes('\n')

			if err != nil {
				if err == io.EOF {
					if len(line) > 0 { // incomplete line received

						if rcv.logIncomplete {
							logIncomplete(peer, buf[:rlen], line)
						}

						lines.store(peer.String(), line)
						atomic.AddUint32(&rcv.incompleteReceived, 1)
					}
				} else {
					atomic.AddUint32(&rcv.errors, 1)
					logrus.Error(err)
				}
				break
			}
			if len(line) > 0 { // skip empty lines
				if msg, err := points.ParseText(string(line)); err != nil {
					atomic.AddUint32(&rcv.errors, 1)
					logrus.Info(err)
				} else {
					atomic.AddUint32(&rcv.metricsReceived, 1)
					rcv.out(msg)
				}
			}
		}
	}
}

// Listen bind port. Receive messages and send to out channel
func (rcv *UDP) Listen(addr *net.UDPAddr) error {
	return rcv.StartFunc(func() error {
		var err error
		rcv.conn, err = net.ListenUDP("udp", addr)
		if err != nil {
			return err
		}

		rcv.Go(func(exit chan bool) {
			<-exit
			rcv.conn.Close()
		})

		if rcv.buffer != nil {
			originalOut := rcv.out

			rcv.Go(func(exit chan bool) {
				for {
					select {
					case <-exit:
						return
					case p := <-rcv.buffer:
						originalOut(p)
					}
				}
			})

			rcv.out = func(p *points.Points) {
				rcv.buffer <- p
			}
		}

		rcv.Go(rcv.receiveWorker)

		return nil
	})
}
