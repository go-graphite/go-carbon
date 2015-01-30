package main

import (
	"log"
	"net"

	"devroom.ru/lomik/carbon"
)

func main() {
	udpAddr, err := net.ResolveUDPAddr("udp", ":12004")
	if err != nil {
		log.Fatal(err)
	}

	cache := carbon.NewCache()

	udpListener := carbon.NewUdpReceiver(cache.In())
	if err = udpListener.Listen(udpAddr); err != nil {
		log.Fatal(err)
	}
}
