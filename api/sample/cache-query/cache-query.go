package main

// Usage: ./cache-query <metric1> <metric2>

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	"google.golang.org/grpc"

	"github.com/lomik/go-carbon/helper/carbonpb"
)

func main() {
	server := flag.String("server", "127.0.0.1:7003", "go-carbon GRPC <host:port>")
	timeout := flag.Duration("timeout", time.Second, "connect and read timeout")
	flag.Parse()

	conn, err := grpc.Dial(*server, grpc.WithInsecure(), grpc.WithTimeout(*timeout))
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()

	c := carbonpb.NewCarbonClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	res, err := c.CacheQuery(ctx, &carbonpb.CacheRequest{Metrics: flag.Args()})
	if err != nil {
		log.Fatal(err)
	}

	for i := 0; i < len(res.GetMetrics()); i++ {
		m := res.GetMetrics()[i]
		for j := 0; j < len(m.GetPoints()); j++ {
			p := m.GetPoints()[j]
			fmt.Printf("%s %#v %d\n", m.Metric, p.Value, p.Timestamp)
		}
	}
}
