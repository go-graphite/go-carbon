package main

import (
	"io"
	"log"
	"net"
	"os"
)

import "bufio"

func main() {
	reader := bufio.NewReader(os.Stdin)

	addr, err := net.ResolveUDPAddr("udp", "localhost:2003")
	if err != nil {
		log.Fatal(err)
	}

	conn, err := net.Dial("udp", addr.String())
	if err != nil {
		log.Fatal(err)
	}

	for {
		line, err := reader.ReadString('\n')

		if err != nil {
			if err == io.EOF {
				break
			}
			log.Fatal(err)
			break
		}

		if _, err := conn.Write([]byte(line)); err != nil {
			log.Fatal(err)
		}

	}
}
