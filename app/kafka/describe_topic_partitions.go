package kafka

import (
	binary "encoding/binary"
	"sort"

	protocol "github.com/codecrafters-io/kafka-starter-go/app/protocol"
)

func HandleDescribeTopicPartitions(requestHeader RequestHeader, decoder *protocol.Decoder) ([]byte, error) {

	/*
		00 00 00 4a  // message_size:                 74 bytes
		ab cd ef 12  // correlation_id:               (matches request)
		00           // TAG_BUFFER:                   empty (response header v1)
		00 00 00 00  // throttle_time_ms:             0
		02           // topics array:                 1 element
		00 00        // error_code:                   0 (no error)
		04           // name length:                  3 (compact string)
		66 6f 6f     // name:                         "foo"
		a1 b2 c3 d4  // topic_id:                     (actual UUID from metadata)
		e5 f6 a7 b8  //                               (16 bytes total)
		c9 d0 e1 f2  //
		a3 b4 c5 d6  //
		00           // is_internal:                  false
		02           // partitions array:             1 element
		00 00        // error_code:                   0 (no error)
		00 00 00 00  // partition_index:              0
		00 00 00 01  // leader_id:                    1
		00 00 00 00  // leader_epoch:                 0
		02           // replica_nodes:                1 element
		00 00 00 01  //                               broker 1
		02           // isr_nodes:                    1 element
		00 00 00 01  //                               broker 1
		01           // eligible_leader_replicas:     0 elements (empty)
		01           // last_known_elr:               0 elements (empty)
		01           // offline_replicas:             0 elements (empty)
		00           // TAG_BUFFER:                   empty
		00 00 00 00  // topic_authorized_operations:  0
		00           // TAG_BUFFER:                   empty
		ff           // next_cursor:                  -1 (null)
		00           // TAG_BUFFER:                   empty
	*/

	//meta-data file /tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log

	encoder := protocol.NewEncoder()
	encoder.Int32(0)                           // placeholder for message length
	encoder.Int32(requestHeader.CorrelationID) // correlation_id
	encoder.Uint8(0)                           // TAG_BUFFER: empty
	encoder.Int32(0)                           // throttle_time_ms

	//parse request
	rawTopicsLength, err := decoder.VarUInt()
	topicsLength := int(rawTopicsLength - 1) // subtract 1 for the compact array length encoding

	topicNames := make([]string, 0, topicsLength)
	for i := 0; i < topicsLength; i++ {
		topicName, _ := decoder.CompactString()
		_, err = decoder.Uint8() // Read the tag buffer for each topic
		if err != nil {
			return nil, err
		}
		topicNames = append(topicNames, topicName)
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

	HandleTopicResponse(encoder, topicNames)

	messageBytes := encoder.GetBytes()
	binary.BigEndian.PutUint32(messageBytes[0:4], uint32(len(messageBytes)-4))

	return messageBytes, nil
}

func SortTopicNames(topicNames []string) []string {
	//sort alphabetically to ensure consistent ordering in the response
	sortedTopicNames := make([]string, len(topicNames))
	copy(sortedTopicNames, topicNames)
	sort.Strings(sortedTopicNames)
	return sortedTopicNames
}

func HandleTopicResponse(encoder *protocol.Encoder, topicNames []string) {

	clusterMetaData, _ := GetClusterMetadataFromFile("/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log")

	encoder.Uint8(uint8(len(topicNames) + 1)) // topics array length

	//sort topic names to ensure consistent ordering in the response
	sortedTopicNames := SortTopicNames(topicNames)
	for _, topicName := range sortedTopicNames {
		topic, found := clusterMetaData.TopicsByName[topicName]
		if !found {
			encoder.Int16(3)                // error_code
			encoder.String(topicName)       // compact string
			encoder.Bytes(make([]byte, 16)) // topic_id: 16 zero bytes
			encoder.Uint8(0)                // is_internal: false
			encoder.Uint8(1)                // partitions compact array: 0 partitions → write 1
			encoder.Int32(0)                // topic_authorized_operations
			encoder.Uint8(0)                // tag_buffer
			continue                        // Skip if topic not found
		}

		encoder.Int16(0)                                // error_code: 0 (no error)
		encoder.String(topic.Name)                      // compact string
		encoder.Bytes(topic.TopicID[:])                 // topic_id: 16 bytes
		encoder.Uint8(0)                                // is_internal: false
		encoder.Uint8(uint8(len(topic.Partitions) + 1)) // partitions compact array length

		for _, partition := range topic.Partitions {
			encoder.Int16(0) // error_code: 0 (no error)
			encoder.Int32(partition.PartitionIndex)
			encoder.Int32(partition.LeaderID)
			encoder.Int32(partition.LeaderEpoch)
			encoder.Uint8(uint8(len(partition.Replicas) + 1))
			for _, replica := range partition.Replicas {
				encoder.Int32(replica)
			}
			encoder.Uint8(uint8(len(partition.ISR) + 1))
			for _, isr := range partition.ISR {
				encoder.Int32(isr)
			}

			encoder.Uint8(1) // eligible_leader_replicas:
			encoder.Uint8(1) // last_known_elr:
			encoder.Uint8(1) // offline_replicas:
			encoder.Uint8(0) // TAG_BUFFER: empty
		}

		encoder.Int32(0) // topic_authorized_operations
		encoder.Uint8(0) // TAG_BUFFER: empty
	}

	encoder.Bytes([]byte{0xff}) // next_cursor: null
	encoder.Uint8(0)            // TAG_BUFFER: empty
}
