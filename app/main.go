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


func isSupportedVersion(apiKey byte, apiVersion int) bool {
	supportedVersions := getSupportedVersions(apiKey)
	if supportedVersions == nil {
		return false
	}
	
	for _, v := range supportedVersions {
		if v == apiVersion {
			return true
		}
	}
	return false
}

func getSupportedVersions(apiKey byte) []int {
	return apikeys.SupportedAPIVersions(int(apiKey))
}

func parseRequest(message []byte) (byte, int, []byte) {
	apiKey := message[4]
	apiVersion := int(message[6])<<8 + int(message[7])
	correlationID := message[8:12]
	return apiKey, apiVersion, correlationID
}


func handleConnection(conn net.Conn) {
	/*
		00 00 00 13  // message_size:      19 bytes
		ab cd ef 12  // correlation_id:    (matches request)
		00 00        // error_code:        0 (no error)
		02           // api_keys array length:    1 element
		00 12        // api_key:           18 (ApiVersions)
		00 00        // min_version:       0
		00 04        // max_version:       4
		00           // TAG_BUFFER:        empty
		00 00 00 00  // throttle_time_ms:  0
		00           // TAG_BUFFER:        empty
	*/
	defer conn.Close()

	message := make([]byte, 1024)

	_, err := conn.Read(message)
	if err != nil {
		fmt.Println("Failed to read message")
		return
	}
	//read api key from connection 
	//read message 6, 7 bytes as api version
	response := make([]byte, 0)
	apiKey, apiVersion, correlationID := parseRequest(message)
	errorCode := 0
	if !isSupportedVersion(apiKey, apiVersion) {
		errorCode = 35 // UNSUPPORTED_VERSION
	}
	response = append(response, correlationID...)
	response = append(response, byte(errorCode>>8), byte(errorCode&0xff))
	
	messageSize := len(response)
	//add message size to the beginning of the response
	response = append([]byte{byte(messageSize >> 24), byte((messageSize >> 16) & 0xff), byte((messageSize >> 8) & 0xff), byte(messageSize & 0xff)}, response...)
	conn.Write([]byte{0, 0, 0, 19})
	conn.Write(correlationID)


	apiVersionInt := int(apiVersion[0])<<8 + int(apiVersion[1])
	if apiVersionInt < 0 || apiVersionInt > 4 {
		conn.Write([]byte{0, 35})
	} else {
		conn.Write([]byte{0, 0})
	}

}





