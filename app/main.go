package main

import (
	"fmt"
	"net"
	"os"

	kafka "github.com/codecrafters-io/kafka-starter-go/app/kafka"
)

func main() {

	l, err := net.Listen("tcp", "0.0.0.0:9092")
	if err != nil {
		fmt.Println("Failed to bind to port 9092")
		os.Exit(1)
	}
	defer l.Close()
	fmt.Println("Listening on port 9092...")
	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Failed to accept connection")
			continue
		}
		go kafka.HandleConnection(conn)
	}
}
