package kafka

import (
	binary "encoding/binary"

	protocol "github.com/codecrafters-io/kafka-starter-go/app/protocol"
)

var supportedAPIs = []int16{18, 75} // Supported APIs
var supportedAPIVersionMap = map[int16][2]int16{
	18: {0, 4},
	75: {0, 2},
}

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
	encoder.Int32(0)                                   // placeholder for message length
	encoder.Int32(requestHeader.CorrelationID)         // correlation_id
	supportedAPIVersions := supportedAPIVersionMap[18] // Get the supported versions for API key 18 (ApiVersions)
	if contains(supportedAPIs, int16(requestHeader.ApiKey)) &&
		requestHeader.ApiVersion >= supportedAPIVersions[0] &&
		requestHeader.ApiVersion <= supportedAPIVersions[1] {
		encoder.Int16(0) // error_code: NO_ERROR
	} else {
		encoder.Int16(35) // error_code: UNSUPPORTED_VERSION
	}

	encoder.Uint8(uint8(len(supportedAPIs) + 1))
	for _, apiKey := range supportedAPIs {
		encoder.Int16(apiKey)
		supportedVersions := supportedAPIVersionMap[apiKey]
		encoder.Int16(supportedVersions[0])
		encoder.Int16(supportedVersions[1])
		encoder.Uint8(0) // TAG_BUFFER: empty
	}

	encoder.Int32(0)
	encoder.Uint8(0) // TAG_BUFFER: empty

	messageBytes := encoder.GetBytes()
	binary.BigEndian.PutUint32(messageBytes[0:4], uint32(len(messageBytes)-4))

	return messageBytes, nil
}
