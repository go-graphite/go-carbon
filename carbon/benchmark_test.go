package carbon

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math/rand"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lomik/go-carbon/cache"
	"github.com/lomik/go-carbon/helper/framing"
	"github.com/lomik/go-carbon/persister"
	"github.com/lomik/go-carbon/points"
	"github.com/lomik/go-carbon/receiver"
	"github.com/stretchr/testify/assert"
)

import (
	"net/http"
	_ "net/http/pprof"
)

type BenchApp struct {
	cache        *cache.Cache
	receiver     *receiver.TCP
	persister    *persister.Whisper
	cl           *cache.CarbonlinkListener
	wgInitDone   *sync.WaitGroup
	wgBenchStart *sync.WaitGroup
	countsChan   chan int
	exitChan     chan struct{}
}

type StringConn struct {
	net.Conn
	r       *strings.Reader
	i       int // current repetition number
	repeats int
	closed  bool
}

type MockAddr struct {
	net.Addr
}

func (a *MockAddr) String() string { return "mock" }

func NewMockStringConn(s string, repeats int) io.ReadWriteCloser {
	return &StringConn{
		r:       strings.NewReader(s),
		i:       0,
		repeats: repeats - 1,
		closed:  false,
	}
}

func (c *StringConn) Read(b []byte) (n int, err error) {
	if c.closed {
		return 0, io.EOF
	}
	n, err = c.r.Read(b)
	if err == io.EOF && c.i < c.repeats {
		c.i++
		c.r.Seek(0, 0)
		n, err = c.r.Read(b)
	}
	return
}

func (c *StringConn) Write(b []byte) (n int, err error) { return len(b), nil }
func (c *StringConn) Close() error {
	c.closed = true
	return nil
}
func (c *StringConn) LocalAddr() net.Addr                { return &MockAddr{} }
func (c *StringConn) RemoteAddr() net.Addr               { return &MockAddr{} }
func (c *StringConn) SetDeadline(t time.Time) error      { return nil }
func (c *StringConn) SetReadDeadline(t time.Time) error  { return nil }
func (c *StringConn) SetWriteDeadline(t time.Time) error { return nil }

func receiverWorker(i int, metrics []string, r *receiver.TCP, numConnections int, wgInitDone, wgBenchStart *sync.WaitGroup, iterCount int) {
	priv := make([]string, len(metrics))
	copy(priv, metrics) // make a private copy

	// shuffle metrics
	rnd := rand.New(rand.NewSource(int64(i)))
	for i := range priv {
		j := rnd.Intn(i + 1)
		priv[i], priv[j] = priv[j], priv[i]
	}

	priv = priv[:len(priv)/numConnections] // keep workload stable and not related to number of receivers
	s := strings.Join(priv, "")
	priv = nil // free memory

	wgInitDone.Done() // notify that we are ready to start loop
	wgBenchStart.Wait()
	r.HandleConnection(NewMockStringConn(s, iterCount).(net.Conn))
}

func startReceivers(b *testing.B, r *receiver.TCP, metrics []string, numConnections int, wgInitDone, wgBenchStart *sync.WaitGroup) {
	wgInitDone.Add(numConnections)
	for i := 0; i < numConnections; i++ {
		go receiverWorker(i, metrics, r, numConnections, wgInitDone, wgBenchStart, b.N)
	}
}

func startPersisters(t *testing.B, cache *cache.Cache, numPersisters int, exitChan chan struct{}) (p *persister.Whisper, countReport chan int) {
	p = persister.NewWhisper("", nil, nil, cache.Out(), cache.Confirm())

	p.SetWorkers(numPersisters)

	countReport = make(chan int)

	p.SetMockStore(func() (persister.StoreFunc, func()) {
		receivedCount := 0

		store := func(_ *persister.Whisper, v *points.Points) {
			receivedCount += len(v.Data)
		}

		batchDone := func() {
			select {
			case <-exitChan:
			case countReport <- receivedCount:
			}
			receivedCount = 0

		}
		return store, batchDone
	})
	return
}

// how many metrics to create
// key - number of datapoints per unique metrics
// value - how many metrics to create with a given number of datapoints
type metricSpec map[int]int

func (m *metricSpec) Count() (c int) {
	for numCounts, numMetrics := range *m {
		c += numCounts * numMetrics
	}
	return
}

func (m *metricSpec) Len() (c int) {
	for _, numMetrics := range *m {
		c += numMetrics
	}
	return
}

var gMetrics []string

func genMetrics(spec metricSpec) []string {
	if gMetrics != nil {
		return gMetrics
	}

	gMetrics = make([]string, 0, spec.Count())
	now := time.Now().Second()

	id := 0
	for numDataPoints, metricCount := range spec {
		for i := 0; i < metricCount; i++ {
			for j := 0; j < numDataPoints; j++ {
				m := fmt.Sprintf("this.is.a.test.metric.%d.%d.count %d %d\n", id, i, j, now)
				gMetrics = append(gMetrics, m)
			}
		}
		id++
	}

	return gMetrics
}

var clReqs map[int]string
var clReqsLock sync.Mutex

func genCLReqs(workerId int, metrics []string, numCLClients int) string {
	priv := make([]string, len(metrics))
	copy(priv, metrics) // make a private copy

	// shuffle metrics
	rnd := rand.New(rand.NewSource(int64(workerId + 69))) //different shuffle from in persisters, but still reproducible
	for i := range priv {
		j := rnd.Intn(i + 1)
		priv[i], priv[j] = priv[j], priv[i]
	}
	priv = priv[:len(priv)/numCLClients] // keep workload stable and not related to number of CL clients

	// preparing pickled query: "uint32(len),pickle.dumps(dict(type='cache-query', metric="ABC"), protocol=2)"
	// assumes that metric name is <256 bytes
	msgParts := []string{
		"", // message lengths goes here
		"\x80\x02}q\x00(U\x06metricq\x01U",
		"", // metric len and metric go here
		"q\x02U\x04typeq\x03U\x0bcache-queryq\x04u.",
	}

	msgPartsLen := len(msgParts[1]) + len(msgParts[3])

	var buf bytes.Buffer
	for idx, s := range priv {
		m := strings.SplitN(s, " ", 2)[0] // get a metric as in carbon protocol and extract metric name
		l := msgPartsLen + 1 + len(m)

		binary.Write(&buf, binary.BigEndian, uint32(l))
		msgParts[0] = buf.String()
		buf.Reset()

		binary.Write(&buf, binary.BigEndian, uint8(len(m)))
		buf.WriteString(m)
		msgParts[2] = buf.String()
		buf.Reset()

		priv[idx] = strings.Join(msgParts, "")
	}

	return strings.Join(priv, "")
}

func carbonLinkWorker(i int, metrics []string, l *cache.CarbonlinkListener, numCLClients int, wgInitDone, wgBenchStart *sync.WaitGroup) {
	clReqsLock.Lock()
	if _, ok := clReqs[i]; !ok {
		clReqsLock.Unlock()
		s := genCLReqs(i, metrics, numCLClients)
		clReqsLock.Lock()
		if clReqs == nil {
			clReqs = make(map[int]string)
		}
		clReqs[i] = s
	}
	clReqsLock.Unlock()

	wgInitDone.Done() // notify that we are ready to start loop
	wgBenchStart.Wait()
	framedConn, _ := framing.NewConn(NewMockStringConn(clReqs[i], 9999999).(net.Conn), byte(4), binary.BigEndian)
	l.HandleConnection(*framedConn)
}

func startCarbonLink(b *testing.B, c *cache.Cache, metrics []string, numCLClients int, wgInitDone, wgBenchStart *sync.WaitGroup) *cache.CarbonlinkListener {
	carbonlink := cache.NewCarbonlinkListener(c.Query())

	wgInitDone.Add(numCLClients)
	for i := 0; i < numCLClients; i++ {
		go carbonLinkWorker(i, metrics, carbonlink, numCLClients, wgInitDone, wgBenchStart)
	}
	return carbonlink
}

func startAll(b *testing.B, qStrategy string, numPersisters, numReceivers, numCLClients int, spec metricSpec) BenchApp {
	exitChan := make(chan struct{})

	var wgBenchStart, wgInitDone sync.WaitGroup
	wgBenchStart.Add(1)

	c := cache.New()
	c.SetMaxSize(0)
	assert.NoError(b, c.SetWriteStrategy(qStrategy))

	metrics := genMetrics(spec)
	assert.Equal(b, spec.Count(), len(metrics))

	rcv, _ := receiver.New("tcp://:0", c)
	r := rcv.(*receiver.TCP)
	r.Start() // populates r.Go callback, doesn't do much beyond that

	startReceivers(b, r, metrics, numReceivers, &wgInitDone, &wgBenchStart)

	p, countsChan := startPersisters(b, c, numPersisters, exitChan)

	cl := startCarbonLink(b, c, metrics, numCLClients, &wgInitDone, &wgBenchStart)

	return BenchApp{
		cache:        c,
		persister:    p,
		receiver:     r,
		cl:           cl,
		wgInitDone:   &wgInitDone,
		wgBenchStart: &wgBenchStart,
		countsChan:   countsChan,
		exitChan:     exitChan,
	}
}

func bench(b *testing.B, qStrategy string, numPersisters, numReceivers, numCLClients int, metricSpec metricSpec) {

	app := startAll(b, qStrategy, numPersisters, numReceivers, numCLClients, metricSpec)
	defer app.cache.Stop()
	defer app.receiver.Stop()

	app.persister.Start()
	defer app.persister.Stop()
	defer close(app.exitChan)

	expectedCount := (metricSpec.Count() / numReceivers) * b.N * numReceivers // due to integer division order is important
	b.Logf("%d(N=%d) datapoints, qStrategy=%s numPersisters=%d, numClients=%d, numCarbonClients=%d", expectedCount, b.N, qStrategy, numPersisters, numReceivers, numCLClients)

	app.wgInitDone.Wait() // wait for all workers to complete init
	b.ResetTimer()
	app.wgBenchStart.Done() // unleash workers

	// start cache after we unleashed senders so that writeout queue has something to work with
	app.cache.Start()

	ticker := time.NewTicker(time.Duration(60*b.N) * time.Second)

	defer ticker.Stop()
	b.Logf("Receiving confirmations from persisters")
	var i = 0
LOOP:
	for i < expectedCount {
		select {
		case c := <-app.countsChan:
			i += c
		case <-ticker.C:
			b.Logf("Exiting due to timer, test most likely going to fail")
			break LOOP
		}
	}

	b.StopTimer()
	cacheStats := make(map[string]float64, 16)
	app.cache.Stat(func(m string, v float64) { cacheStats[m] = v })
	b.Logf("Cache stats %v", cacheStats)

	ticker = time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	select {
	case i := <-app.countsChan:
		if i != 0 {
			b.Fatalf("No more metrics should be persisted, something is wrong")
		}
	case <-ticker.C:
	}

	if i != expectedCount {
		b.Fatalf("Expected %d received confirmation for %d", expectedCount, i)
	}
}

func BenchmarkApp(b *testing.B) {
	go func() {
		http.ListenAndServe("localhost:6060", nil)
	}()

	spec := metricSpec{
		1:  1000000,
		50: 20000,
	}

	NUM_PERSISTERS := 4
	NUM_CLIENTS := 4
	NUM_CARBONLINK_CLIENTS := 2
	QUEUE_WRITE_STRATEGY := "noop"

	if i, err := strconv.Atoi(os.Getenv("NUM_PERSISTERS")); err == nil && i >= 0 {
		NUM_PERSISTERS = i
	}
	if i, err := strconv.Atoi(os.Getenv("NUM_CLIENTS")); err == nil && i >= 0 {
		NUM_CLIENTS = i
	}
	if i, err := strconv.Atoi(os.Getenv("NUM_CARBONLINK_CLIENTS")); err == nil && i >= 0 {
		NUM_CARBONLINK_CLIENTS = i
	}

	if s := os.Getenv("QUEUE_WRITE_STRATEGY"); s != "" {
		QUEUE_WRITE_STRATEGY = s
	}

	bench(b, QUEUE_WRITE_STRATEGY, NUM_PERSISTERS, NUM_CLIENTS, NUM_CARBONLINK_CLIENTS, spec)
}
