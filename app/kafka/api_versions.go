package kafka

import (
	binary "encoding/binary"

	protocol "github.com/codecrafters-io/kafka-starter-go/app/protocol"
)

var supportedAPIVersions = []int16{0, 1, 2, 3, 4}

func contains(slice []int16, val int16) bool {
	for _, v := range slice {
		if v == val {
			return true
		}
	}
	return false
}

func HandleAPIVersions(requestHeader RequestHeader, decoder *protocol.Decoder) ([]byte, error) {
	encoder := protocol.NewEncoder()
	encoder.Int32(0)                           // placeholder for message length
	encoder.Int32(requestHeader.CorrelationID) // correlation_id
	if len(supportedAPIVersions) == 0 || !contains(supportedAPIVersions, int16(requestHeader.ApiVersion)) {
		encoder.Int16(35) // error_code: UNSUPPORTED_VERSION
	} else {
		encoder.Int16(0) // error_code: NO_ERROR
	}
	encoder.Int32(2)                                                 // api_keys array length: 1 element
	encoder.Int16(18)                                                // api_key: 18 (ApiVersions)
	encoder.Int16(supportedAPIVersions[0])                           // min_version: 0
	encoder.Int16(supportedAPIVersions[len(supportedAPIVersions)-1]) // max_version: 4
	encoder.Uint8(0)                                                 // TAG_BUFFER: empty
	encoder.Int32(0)                                                 // throttle_time_ms: 0
	encoder.Uint8(0)                                                 // TAG_BUFFER: empty

	messageBytes := encoder.GetBytes()
	binary.BigEndian.PutUint32(messageBytes[0:4], uint32(len(messageBytes)-4))

	return messageBytes, nil
}
