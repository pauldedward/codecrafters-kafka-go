package kafka

import (
	binary "encoding/binary"

	protocol "github.com/codecrafters-io/kafka-starter-go/app/protocol"
)

func HandleDescribeTopicPartitions(requestHeader RequestHeader, decoder *protocol.Decoder) ([]byte, error) {

	encoder := protocol.NewEncoder()
	encoder.Int32(0)                           // placeholder for message length
	encoder.Int32(requestHeader.CorrelationID) // correlation_id
	encoder.Uint8(0)                           // TAG_BUFFER: empty
	encoder.Int32(0)                           // throttle_time_ms

	//parse request
	topicsLength, err := decoder.Uint8()
	if err != nil {
		return nil, err
	}

	topicNames := make([]string, topicsLength-1)
	for i := 1; i < int(topicsLength); i++ {
		topicNameLength, err := decoder.Uint8()
		if err != nil {
			return nil, err
		}
		topicName, err := decoder.String(int(topicNameLength - 1))
		if err != nil {
			return nil, err
		}
		_, err = decoder.Uint8() // Read the tag buffer for each topic
		if err != nil {
			return nil, err
		}
		topicNames[i-1] = topicName
	}

	_, err = decoder.Int32()
	if err != nil {
		return nil, err
	}
	_, err = decoder.Uint8() // Read the cursor
	if err != nil {
		return nil, err
	}
	_, err = decoder.Uint8() // Read the tag buffer for each topic
	if err != nil {
		return nil, err
	}
	//request parsing ended

	//write response
	//Topic array length
	encoder.Uint8(uint8(len(topicNames) + 1))
	for _, topicName := range topicNames {
		encoder.Int16(3) // error_code: UNKNOWN ERROR
		encoder.String(topicName)
		//16 byte UUID for topic_id
		encoder.Bytes(make([]byte, 16)) // Placeholder for topic_id
		encoder.Uint8(0)                // is_internal: false
		encoder.Uint8(0)                // partitions array length
		encoder.Int32(0)                // topic_authorized_operations
		encoder.Uint8(0)                // TAG_BUFFER: empty
	}
	//set ff indicating null
	encoder.Bytes([]byte{0xff}) // next_cursor: null
	encoder.Uint8(0)            // TAG_BUFFER: empty

	messageBytes := encoder.GetBytes()
	binary.BigEndian.PutUint32(messageBytes[0:4], uint32(len(messageBytes)-4))

	return messageBytes, nil
}
