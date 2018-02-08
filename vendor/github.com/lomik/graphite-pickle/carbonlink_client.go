package pickle

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/lomik/graphite-pickle/framing"
	ogórek "github.com/lomik/og-rek"
)

type CarbonlinkClient struct {
	retries        int
	threads        int
	addr           string
	pool           chan net.Conn
	dialer         *net.Dialer
	connectTimeout time.Duration
	queryTimeout   time.Duration
}

func NewCarbonlinkClient(addr string, retries int, threads int, connectTimeout time.Duration, queryTimeout time.Duration) *CarbonlinkClient {
	if retries < 1 {
		retries = 1
	}
	if threads < 1 {
		threads = 1
	}

	return &CarbonlinkClient{
		retries: retries,
		threads: threads,
		addr:    addr,
		pool:    make(chan net.Conn, threads*retries*2),
		dialer: &net.Dialer{
			Timeout: time.Second,
		},
		connectTimeout: connectTimeout,
		queryTimeout:   queryTimeout,
	}
}

func (client *CarbonlinkClient) connect(ctx context.Context) (net.Conn, error) {
	select {
	case c := <-client.pool:
		return c, nil
	default:
	}

	ctx, cancel := context.WithTimeout(ctx, client.connectTimeout)
	c, err := client.dialer.DialContext(ctx, "tcp", client.addr)
	cancel()
	return c, err
}

func (client *CarbonlinkClient) keepalive(conn net.Conn) {
	conn.SetDeadline(time.Time{})

	select {
	case client.pool <- conn:
		// pass
	default:
		conn.Close()
	}
}

func (client *CarbonlinkClient) CacheQuery(ctx context.Context, metric string) ([]DataPoint, error) {
	req := &CarbonlinkRequest{
		Type:   "cache-query",
		Metric: metric,
	}

	reqBody, err := MarshalCarbonlinkRequest(req)
	if err != nil {
		return nil, err
	}

RetryLoop:
	for i := 0; i < client.retries; i++ {
		// check context Done
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		// connect
		conn, err := client.connect(ctx)
		if err != nil {
			continue RetryLoop
		}

		// check context Done
		if ctx.Err() != nil && conn != nil {
			conn.Close()
			return nil, ctx.Err()
		}

		// calculate deadline
		deadline := time.Now().Add(client.queryTimeout)
		ctxDeadline, ctxHasDeadline := ctx.Deadline()
		if ctxHasDeadline && ctxDeadline.Before(deadline) {
			deadline = ctxDeadline
		}

		conn.SetDeadline(deadline)

		framedConn, _ := framing.NewConn(conn, byte(4), binary.BigEndian)
		framedConn.MaxFrameSize = 1048576 // 1MB max frame size for read and write

		_, err = framedConn.Write(reqBody)
		if err != nil {
			conn.Close()
			continue RetryLoop
		}

		resBody, err := framedConn.ReadFrame()
		if err != nil {
			conn.Close()
			continue RetryLoop
		}
		client.keepalive(conn)

		// data received
		return unpackCacheQueryReply(resBody)
	}

	return nil, errors.New("cache query failed")
}

func (client *CarbonlinkClient) CacheQueryMulti(ctx context.Context, metrics []string) (map[string][]DataPoint, error) {
	result := make(map[string][]DataPoint)
	var rm sync.Mutex

	var wg sync.WaitGroup
	threads := len(metrics)
	if threads > client.threads {
		threads = client.threads
	}

	queue := make(chan string, len(metrics))
	errors := make(chan error, 1)

	for i := 0; i < threads; i++ {
		wg.Add(1)
		go func() {
			for {
				if ctx.Err() != nil {
					break
				}

				m, ok := <-queue
				if !ok {
					break
				}

				res, err := client.CacheQuery(ctx, m)

				if res != nil {
					rm.Lock()
					result[m] = res
					rm.Unlock()
				}

				if err != nil {
					select {
					case errors <- err:
						// pass
					default:
						// pass
					}
				}

			}
			wg.Done()
		}()
	}

	for i := 0; i < len(metrics); i++ {
		queue <- metrics[i]
	}
	close(queue)

	wg.Wait()

	var err error
	select {
	case err = <-errors:
		// pass
	default:
		// pass
	}

	if err == nil {
		err = ctx.Err()
	}

	return result, err
}

func unpackCacheQueryReply(body []byte) ([]DataPoint, error) {
	d := ogórek.NewDecoder(bytes.NewReader(body))

	v, err := d.Decode()
	if err != nil {
		return nil, err
	}

	m, ok := ToMap(v)
	if !ok {
		return nil, errors.New("response is not map")
	}

	if m["error"] != nil {
		msg, _ := ToString(m["error"])
		return nil, fmt.Errorf("error received from server: %s", msg)
	}

	dp := m["datapoints"]
	if dp == nil {
		return nil, nil
	}

	datapoints, ok := ToList(dp)
	if !ok {
		return nil, errors.New("datapoints is not list or tuple")
	}

	// empty
	if len(datapoints) == 0 {
		return nil, nil
	}

	points := make([]DataPoint, len(datapoints))

	for i := 0; i < len(datapoints); i++ {
		p, ok := ToList(datapoints[i])
		if !ok {
			return nil, errors.New("point is not list or tuple")
		}

		if len(p) != 2 {
			return nil, errors.New("len(point) != 2")
		}

		if points[i].Timestamp, ok = ToInt64(p[0]); !ok {
			return nil, errors.New("bad timestamp")
		}

		if points[i].Value, ok = ToFloat64(p[1]); !ok {
			return nil, errors.New("bad value")
		}

	}

	return points, nil
}
