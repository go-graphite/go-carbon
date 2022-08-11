package grpcutil

import (
	"context"
	"fmt"
	"path"
	"strings"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

func UnaryServerTimeHandler(cb func(payload, peer string, t time.Duration)) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp interface{}, err error) {
		t0 := time.Now()
		defer func() {
			t := time.Since(t0)
			var payload string
			if reqStringer, ok := req.(fmt.Stringer); ok {
				payload = reqStringer.String()
			}
			var reqPeer string
			if p, ok := peer.FromContext(ctx); ok {
				reqPeer = p.Addr.String()
			}
			cb(payload, reqPeer, t)
		}()
		return handler(ctx, req)
	}
}

func StreamServerTimeHandler(cb func(payload, peer string, t time.Duration)) grpc.StreamServerInterceptor {
	return func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		t0 := time.Now()
		wss := GetWrappedStream(ss)
		defer func() {
			t := time.Since(t0)
			var reqPeer string
			if p, ok := peer.FromContext(wss.Context()); ok {
				reqPeer = p.Addr.String()
			}
			cb(wss.Payload(), reqPeer, t)
		}()
		return handler(srv, wss)
	}
}

func GetWrappedStream(ss grpc.ServerStream) *WrappedStream {
	if wss, ok := ss.(*WrappedStream); ok {
		return wss
	}
	return &WrappedStream{
		ServerStream: ss,
		ctx:          ss.Context(),
	}
}

type WrappedStream struct {
	grpc.ServerStream
	ctx      context.Context
	payload  string
	gotFirst bool
}

func (ws *WrappedStream) RecvMsg(m interface{}) error {
	err := ws.ServerStream.RecvMsg(m)
	if err != nil {
		return err
	}
	if ws.gotFirst {
		return nil
	}
	if p, ok := m.(fmt.Stringer); ok {
		ws.payload = p.String()
	}
	ws.gotFirst = true
	return nil
}

func (ws *WrappedStream) Context() context.Context {
	return ws.ctx
}

func (ws *WrappedStream) SetContext(ctx context.Context) {
	ws.ctx = ctx
}

func (ws *WrappedStream) Payload() string {
	return ws.payload
}

func StreamServerStatusMetricHandler(statusCodes map[string][]uint64, promRequest func(string, int)) grpc.StreamServerInterceptor {
	return func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		err := handler(srv, ss)
		_, methodName := path.Split(info.FullMethod)
		endpoint := strings.ToLower(methodName)
		var major int
		if err != nil {
			s, _ := status.FromError(err)
			major = getHTTPStatusCodeMajorFromGrpcStatusCode(s.Code())
		} else {
			major = getHTTPStatusCodeMajorFromGrpcStatusCode(codes.OK)
		}
		if globalStatusCodes := statusCodes["combined"]; major < len(globalStatusCodes) {
			atomic.AddUint64(&globalStatusCodes[major], 1)
			if handlerStatusCodes, ok := statusCodes[endpoint]; ok {
				atomic.AddUint64(&handlerStatusCodes[major], 1)
			}
		}
		promRequest(endpoint, major)
		return err
	}
}

func getHTTPStatusCodeMajorFromGrpcStatusCode(code codes.Code) int {
	switch code {
	case codes.OK:
		return 1
	case codes.Canceled,
		codes.InvalidArgument,
		codes.NotFound,
		codes.AlreadyExists,
		codes.PermissionDenied,
		codes.ResourceExhausted,
		codes.FailedPrecondition,
		codes.Unauthenticated:
		return 3
	default:
		return 4
	}
}
