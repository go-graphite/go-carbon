package main

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"time"

	"github.com/lomik/go-carbon/points"
	"github.com/lomik/go-carbon/receiver"
)

import _ "net/http/pprof"

func main() {
	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	rcv, err := receiver.New(
		"tcp://127.0.0.1:0",
		receiver.OutFunc(func(p *points.Points) {}),
	)
	defer rcv.Stop()

	if err != nil {
		log.Fatal(err)
	}

	conn, err := net.Dial("tcp", rcv.(*receiver.TCP).Addr().String())
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	text := []byte(`carbon.agents.localhost.cache.size 1412351 1479933789
carbon.agents.localhost.cache.size 1412351 1479933789
carbon.agents.localhost.cache.size 1412351 1479933789
carbon.agents.localhost.cache.size 1412351 1479933789
carbon.agents.localhost.cache.size 1412351 1479933789
carbon.agents.localhost.cache.size 1412351 1479933789
carbon.agents.localhost.cache.size 1412351 1479933789
carbon.agents.localhost.cache.size 1412351 1479933789
carbon.agents.localhost.cache.size 1412351 1479933789
carbon.agents.localhost.cache.size 1412351 1479933789
`)

	cnt := 0
	t := time.Now()
	for {
		if _, err := conn.Write([]byte(text)); err != nil {
			log.Fatal(err)
		}

		cnt++
		if cnt%100000 == 0 {
			fmt.Printf("%.2f p/s\n", 1000000.0/time.Since(t).Seconds())
			t = time.Now()
		}
	}
}
