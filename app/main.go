package main

import (
	"fmt"
	"net"
	"os"
)

func main() {

	messageSize := []byte{0, 0, 0, 4}

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
		go handleConnection(conn, messageSize)
	}
}

func handleConnection(conn net.Conn, messageSize []byte) {
	defer conn.Close()

	message := make([]byte, 1024)

	_, err := conn.Read(message)
	if err != nil {
		fmt.Println("Failed to read message")
		return
	}
	//read message 6, 7 bytes as api version
	apiVersion := message[6:8]

	conn.Write(messageSize)
	conn.Write(message[8:12])

	apiVersionInt := int(apiVersion[0])<<8 + int(apiVersion[1])
	if apiVersionInt < 0 || apiVersionInt > 4 {
		conn.Write([]byte{0, 35})
	} else {
		conn.Write([]byte{0, 0})
	}

}
