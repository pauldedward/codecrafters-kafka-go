package kafka

import (
	binary "encoding/binary"

	protocol "github.com/codecrafters-io/kafka-starter-go/app/protocol"
)

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
	HandleSupportedVersions(requestHeader, encoder)
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
