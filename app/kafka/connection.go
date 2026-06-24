package kafka

import (
	"bytes"
	"fmt"
	"net"

	protocol "github.com/codecrafters-io/kafka-starter-go/app/protocol"
)

func HandleConnection(conn net.Conn) {
	defer conn.Close()
	decoder := protocol.NewDecoder(conn)

	for {

		messageLength, err := decoder.Int32()
		if err != nil {
			fmt.Println("Failed to read message length")
			return
		}

		if messageLength < 0 {
			fmt.Println("Invalid message length")
			return
		}

		requestBytes, err := decoder.Bytes(int(messageLength))
		if err != nil {
			fmt.Println("Failed to read request bytes")
			return
		}

		requestDecoder := protocol.NewDecoder(bytes.NewReader(requestBytes))
		requestHeader, err := ParseHeader(requestDecoder)
		if err != nil {
			fmt.Println("Failed to parse request header:", err)
			return
		}

		switch requestHeader.ApiKey {
		case 18: // API Versions
			response, err := HandleAPIVersions(requestHeader, decoder)
			if err != nil {
				fmt.Println("Failed to handle API versions:", err)
				return
			}

			_, err = conn.Write(response)
			if err != nil {
				fmt.Println("Failed to send response:", err)
				return
			}
		}

	}
}
