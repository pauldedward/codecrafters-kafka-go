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
		topicName, err := decoder.String(int(topicNameLength))
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
		encoder.Int32(0)                // partitions array length
		encoder.Int32(0)                // topic_authorized_operations
		encoder.Uint8(0)                // TAG_BUFFER: empty
	}
	//set ff indicating null
	encoder.Bytes([]byte{0xff}) // next_cursor: null
	encoder.Uint8(0)            // TAG_BUFFER: empty

	messageBytes := encoder.GetBytes()
	binary.BigEndian.PutUint32(messageBytes[0:4], uint32(len(messageBytes)-4))

	return messageBytes, nil

	/*

			DescribeTopicPartitions Request (Version: 0) => [topics] response_partition_limit cursor
		  topics => name
		    name => COMPACT_STRING
		  response_partition_limit => INT32
		  cursor => topic_name partition_index
		    topic_name => COMPACT_STRING
		    partition_index => INT32
	*/

	/*
			DescribeTopicPartitions Response (Version: 0) => throttle_time_ms [topics] next_cursor
		  throttle_time_ms => INT32
		  topics => error_code name topic_id is_internal [partitions] topic_authorized_operations
		    error_code => INT16
		    name => COMPACT_NULLABLE_STRING
		    topic_id => UUID
		    is_internal => BOOLEAN
		    partitions => error_code partition_index leader_id leader_epoch [replica_nodes] [isr_nodes] [eligible_leader_replicas] [last_known_elr] [offline_replicas]
		      error_code => INT16
		      partition_index => INT32
		      leader_id => INT32
		      leader_epoch => INT32
		      replica_nodes => INT32
		      isr_nodes => INT32
		      eligible_leader_replicas => INT32
		      last_known_elr => INT32
		      offline_replicas => INT32
		    topic_authorized_operations => INT32
		  next_cursor => topic_name partition_index
		    topic_name => COMPACT_STRING
		    partition_index => INT32
	*/
}
