package main

import (
	"fmt"
	"net"
	"os"
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
		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	defer conn.Close()

	message := make([]byte, 1024)
	_, err := conn.Read(message)
	if err != nil {
		fmt.Println("Failed to read message")
		return
	}
	conn.Write([]byte{0, 0, 0, 4})
	conn.Write(message[8:12])

}
