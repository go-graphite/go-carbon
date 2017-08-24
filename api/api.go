package api

import (
	"net"
	"sync/atomic"

	"golang.org/x/net/context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	"github.com/lomik/go-carbon/cache"
	"github.com/lomik/go-carbon/helper"
	"github.com/lomik/go-carbon/helper/carbonpb"
	"github.com/lomik/stop"
)

// Api receive metrics from GRPC connections
type Api struct {
	stop.Struct
	stat struct {
		cacheRequests        uint32 // atomic
		cacheRequestMetrics  uint32 // atomic
		cacheResponseMetrics uint32 // atomic
		cacheResponsePoints  uint32 // atomic
	}
	cache    *cache.Cache
	listener *net.TCPListener
}

func New(c *cache.Cache) *Api {
	return &Api{
		cache: c,
	}
}

// Addr returns binded socket address. For bind port 0 in tests
func (api *Api) Addr() net.Addr {
	if api.listener == nil {
		return nil
	}
	return api.listener.Addr()
}

// Collect cache metrics
func (api *Api) Stat(send helper.StatCallback) {
	helper.SendAndSubstractUint32("cacheRequests", &api.stat.cacheRequests, send)
	helper.SendAndSubstractUint32("cacheRequestMetrics", &api.stat.cacheRequestMetrics, send)
	helper.SendAndSubstractUint32("cacheResponseMetrics", &api.stat.cacheResponseMetrics, send)
	helper.SendAndSubstractUint32("cacheResponsePoints", &api.stat.cacheResponsePoints, send)
}

// Listen bind port. Receive messages and send to out channel
func (api *Api) Listen(addr *net.TCPAddr) error {
	return api.StartFunc(func() error {

		tcpListener, err := net.ListenTCP("tcp", addr)
		if err != nil {
			return err
		}

		s := grpc.NewServer()
		carbonpb.RegisterCarbonServer(s, api)
		// Register reflection service on gRPC server.
		reflection.Register(s)

		api.Go(func(exit chan struct{}) {
			<-exit
			s.Stop()
		})

		api.Go(func(exit chan struct{}) {
			defer s.Stop()

			if err := s.Serve(tcpListener); err != nil {
				// may be stopped - not error
				// zapwriter.Logger("api").Fatal("failed to serve", zap.Error(err))
			}

		})

		api.listener = tcpListener

		return nil
	})
}

func (api *Api) CacheQuery(ctx context.Context, req *carbonpb.CacheRequest) (*carbonpb.Payload, error) {
	res := &carbonpb.Payload{
		Metrics: make([]*carbonpb.Metric, 0),
	}

	resMetrics := 0
	resPoints := 0

	for i := 0; i < len(req.Metrics); i++ {
		data := api.cache.Get(req.Metrics[i])

		if len(data) > 0 {
			resMetrics++
			resPoints += len(data)

			m := &carbonpb.Metric{
				Metric: req.Metrics[i],
				Points: make([]carbonpb.Point, len(data)),
			}
			for j := 0; j < len(data); j++ {
				m.Points[j].Timestamp = uint32(data[j].Timestamp)
				m.Points[j].Value = data[j].Value
			}

			res.Metrics = append(res.Metrics, m)
		}
	}

	atomic.AddUint32(&api.stat.cacheRequests, 1)
	atomic.AddUint32(&api.stat.cacheRequestMetrics, uint32(len(req.Metrics)))
	atomic.AddUint32(&api.stat.cacheResponseMetrics, uint32(resMetrics))
	atomic.AddUint32(&api.stat.cacheResponsePoints, uint32(resPoints))

	return res, nil
}
