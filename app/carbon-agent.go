package main

import (
	"log"
	"net"

	"devroom.ru/lomik/carbon"
)

func main() {

	whisperSchemas, err := carbon.ReadWhisperSchemas("schemas")
	if err != nil {
		log.Fatal(err)
	}

	udpAddr, err := net.ResolveUDPAddr("udp", ":2003")
	if err != nil {
		log.Fatal(err)
	}

	cache := carbon.NewCache()
	cache.Start()
	defer cache.Stop()

	udpListener := carbon.NewUdpReceiver(cache.In())
	defer udpListener.Stop()
	if err = udpListener.Listen(udpAddr); err != nil {
		log.Fatal(err)
	}

	whisperPersister := carbon.NewWhisperPersister(whisperSchemas, cache.Out())

	whisperPersister.Start()
	defer whisperPersister.Stop()

	select {}
}
