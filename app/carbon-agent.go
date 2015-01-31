package main

import (
	"log"
	"net"

	"devroom.ru/lomik/carbon"
)

func main() {
	udpAddr, err := net.ResolveUDPAddr("udp", ":2003")
	if err != nil {
		log.Fatal(err)
	}

	cache := carbon.NewCache()
	cache.Run()
	defer cache.Stop()

	udpListener := carbon.NewUdpReceiver(cache.In())
	defer udpListener.Stop()
	if err = udpListener.Listen(udpAddr); err != nil {
		log.Fatal(err)
	}

	select {}
}
