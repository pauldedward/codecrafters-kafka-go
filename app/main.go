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

// var supportedAPIVersions = map[int][]int{
// 	18: {0, 1, 2, 3, 4},
// }

// func getSupportedVersions(apiKey byte) []int {
// 	return supportedAPIVersions[(int(apiKey))]
// }

// func isSupportedVersion(apiKey byte, apiVersion int) bool {
// 	supportedVersions := getSupportedVersions(apiKey)
// 	if supportedVersions == nil {
// 		return false
// 	}

// 	for _, v := range supportedVersions {
// 		if v == apiVersion {
// 			return true
// 		}
// 	}
// 	return false
// }

// handle everything as bytes
// func handleSupportedVersions(apiKey byte, apiVersion int) (int, int, int) {
// 	if !isSupportedVersion(apiKey, apiVersion) {
// 		return 35, 0, 0 // UNSUPPORTED_VERSION
// 	}
// 	return 0, getSupportedVersions(apiKey)[0], getSupportedVersions(apiKey)[len(getSupportedVersions(apiKey))-1]
// }

// func parseRequest(message []byte) ([]byte, []byte, []byte, []byte, []byte) {
// 	apiKey := message[4]
// 	apiVersion := int(message[6])<<8 + int(message[7])
// 	correlationID := message[8:12]

// 	//handle supported versions and return error code if not supported and if supported return min and max version
// 	errorCode, minVersion, maxVersion := handleSupportedVersions(apiKey, apiVersion)

// 	return apiKey, correlationID, errorCode, minVersion, maxVersion
// }

// func handleConnection(conn net.Conn) {

// 	defer conn.Close()

// 	reader := protocol.NewDecoder(conn)
// 	messageSize, err := reader.Int32()
// 	if err != nil {
// 		fmt.Println("Failed to read message size")
// 		return
// 	}

// 	message, err := reader.Bytes(int(messageSize))
// 	if err != nil {
// 		fmt.Println("Failed to read message")
// 		return
// 	}

// 	correlationID, err := message[4:8], nil

// reader := protocol.NewDecoder(conn)

// sizeBuffer := make([]byte, 4)

// _, err := io.ReadFull(conn, sizeBuffer)

// if err != nil {
// 	fmt.Println("Failed to read message size")
// 	return
// }

// sizeBuffer = make([]byte, binary.BigEndian.Uint32(sizeBuffer))

// message := make([]byte, 1024)

// _, err = io.ReadFull(conn, message)
// if err != nil {
// 	fmt.Println("Failed to read message")
// 	return
// }

// response := make([]byte, 0)
// apiKey, correlationID, errorCode, minVersion, maxVersion := parseRequest(message)

// sizeBytes := make([]byte, 4)
// binary.BigEndian.PutUint32(sizeBytes, uint32(0))
// response = append(response, sizeBytes...) // placeholder for message size
// response = append(response, correlationID...)
// response = append(response, byte(errorCode))

// response = append(response, byte(2))
// response = append(response, byte(apiKey))
// response = append(response, byte(minVersion))
// response = append(response, byte(maxVersion))
// response = append(response, []byte{0, 0}...)
// response = append(response, byte(0))

// messageSize := len(response)

// response[0] = byte(messageSize >> 24)
// response[1] = byte((messageSize >> 16) & 0xff)
// response[2] = byte((messageSize >> 8) & 0xff)
// response[3] = byte(messageSize & 0xff)

// conn.Write(response)

// }
