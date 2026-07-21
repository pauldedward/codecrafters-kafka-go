package kafka

import (
	binary "encoding/binary"

	protocol "github.com/codecrafters-io/kafka-starter-go/app/protocol"
)

func HandleFetch(requestHeader RequestHeader, decoder *protocol.Decoder) ([]byte, error) {
	encoder := protocol.NewEncoder()
	encoder.Int32(0)                           // placeholder for message length
	encoder.Int32(requestHeader.CorrelationID) // correlation_id
	encoder.Uint8(0)                           // TAG_BUFFER: empty (response header v1)
	encoder.Int32(0)                           // throttle_time_ms
	encoder.Uint8(1)                           // responses compact array: 0 elements
	encoder.Uint8(0)                           // TAG_BUFFER: empty (response body)
	messageBytes := encoder.GetBytes()
	binary.BigEndian.PutUint32(messageBytes[0:4], uint32(len(messageBytes)-4))
	return messageBytes, nil
}
