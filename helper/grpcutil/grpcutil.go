package grpcutil

import (
	"context"
	"github.com/golang/protobuf/proto"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/peer"
)

func UnaryServerTimeHandler(cb func(payload, peer string, t time.Duration)) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp interface{}, err error) {
		t0 := time.Now()
		defer func() {
			t := time.Since(t0)
			var payload string
			if reqStringer, ok := req.(proto.Message); ok {
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
		wss := &wrappedStream{
			ServerStream: ss,
		}
		defer func() {
			t := time.Since(t0)
			var reqPeer string
			if p, ok := peer.FromContext(wss.Context()); ok {
				reqPeer = p.Addr.String()
			}
			cb(wss.payload, reqPeer, t)
		}()
		return handler(srv, wss)
	}
}

type wrappedStream struct {
	grpc.ServerStream
	payload  string
	gotFirst bool
}

func (ws *wrappedStream) RecvMsg(m interface{}) error {
	err := ws.ServerStream.RecvMsg(m)
	if err != nil {
		return err
	}
	if ws.gotFirst {
		return nil
	}
	if p, ok := m.(proto.Message); ok {
		ws.payload = p.String()
	}
	ws.gotFirst = true
	return nil
}
