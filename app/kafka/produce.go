package kafka

import (
	binary "encoding/binary"

	protocol "github.com/codecrafters-io/kafka-starter-go/app/protocol"
)

func HandleProduce(requestHeader RequestHeader, decoder *protocol.Decoder) ([]byte, error) {
	encoder := protocol.NewEncoder()
	encoder.Int32(0)                           // placeholder for message length
	encoder.Int32(requestHeader.CorrelationID) // correlation_id
	messageBytes := encoder.GetBytes()
	binary.BigEndian.PutUint32(messageBytes[0:4], uint32(len(messageBytes)-4))

	return messageBytes, nil
}
