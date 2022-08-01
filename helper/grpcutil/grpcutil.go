package grpcutil

import (
	"context"
	"fmt"
	"time"
	
	"google.golang.org/grpc"
	"google.golang.org/grpc/peer"
)

func StreamServerTimeHandler(cb func(payload, peer string, t time.Duration)) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp interface{}, err error) {
		t0 := time.Now()
		defer func() {
			t := time.Since(t0)
			var payload string
			if reqStringer, ok := req.(fmt.Stringer); ok {
				payload = reqStringer.String()
			}
			var reqPeer string
			p, ok := peer.FromContext(ctx)
			if ok {
				reqPeer = p.Addr.String()
			}
			fmt.Println("INTERCEPTED!")
			cb(payload, reqPeer, t)
		}()
		return handler(ctx, req)
	}
}
