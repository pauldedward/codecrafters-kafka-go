package kafka

import (
	"fmt"
	"net"

	protocol "github.com/codecrafters-io/kafka-starter-go/app/protocol"
)

func HandleConnection(conn net.Conn) {
	defer conn.Close()
	decoder := protocol.NewDecoder(conn)

	for {

		_, err := decoder.Int32()
		if err != nil {
			fmt.Println("Failed to read message length")
			return
		}

		requestHeader, err := ParseHeader(decoder)
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
