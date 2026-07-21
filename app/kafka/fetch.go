package kafka

import (
	binary "encoding/binary"

	protocol "github.com/codecrafters-io/kafka-starter-go/app/protocol"
)

type FetchRequestBody struct {
	ReplicaID   int32
	MaxWaitTime int32
	MinBytes    int32
	Topics      []FetchTopic
}

type FetchTopic struct {
	Name       string
	Partitions []FetchPartition
}

type FetchPartition struct {
	Partition      int32
	Offset         int64
	logStartOffset int64
	MaxBytes       int32
}

func parseFetchRequestBody(decoder *protocol.Decoder) (FetchRequestBody, error) {
	replicaID, err := decoder.Int32()
	if err != nil {
		return FetchRequestBody{}, err
	}
	maxWaitTime, err := decoder.Int32()
	if err != nil {
		return FetchRequestBody{}, err
	}
	minBytes, err := decoder.Int32()
	if err != nil {
		return FetchRequestBody{}, err
	}
	topicsLength, err := decoder.Int32()
	if err != nil {
		return FetchRequestBody{}, err
	}
	topics := make([]FetchTopic, topicsLength)
	for i := int32(0); i < topicsLength; i++ {
		topicNameLength, err := decoder.Int16()
		if err != nil {
			return FetchRequestBody{}, err
		}
		topicName, err := decoder.String(int(topicNameLength))
		if err != nil {
			return FetchRequestBody{}, err
		}
		partitionsLength, err := decoder.Int32()
		if err != nil {
			return FetchRequestBody{}, err
		}
		partitions := make([]FetchPartition, partitionsLength)
		for j := int32(0); j < partitionsLength; j++ {
			partition, err := decoder.Int32()
			if err != nil {
				return FetchRequestBody{}, err
			}
			offset, err := decoder.Int64()
			if err != nil {
				return FetchRequestBody{}, err
			}
			logStartOffset, err := decoder.Int64()
			if err != nil {
				return FetchRequestBody{}, err
			}
			maxBytes, err := decoder.Int32()
			if err != nil {
				return FetchRequestBody{}, err
			}
			partitions[j] = FetchPartition{
				Partition:      partition,
				Offset:         offset,
				logStartOffset: logStartOffset,
				MaxBytes:       maxBytes,
			}
		}
		topics[i] = FetchTopic{
			Name:       topicName,
			Partitions: partitions,
		}
	}
	return FetchRequestBody{
		ReplicaID:   replicaID,
		MaxWaitTime: maxWaitTime,
		MinBytes:    minBytes,
		Topics:      topics,
	}, nil
}

func HandleFetch(requestHeader RequestHeader, decoder *protocol.Decoder) ([]byte, error) {
	//decode request body
	fetchRequestBody, err := parseFetchRequestBody(decoder)
	if err != nil {
		return nil, err
	}
	topicNames := make([]string, len(fetchRequestBody.Topics))
	for i, topic := range fetchRequestBody.Topics {
		topicNames[i] = topic.Name
	}
	encoder := protocol.NewEncoder()
	encoder.Int32(0)                           // placeholder for message length
	encoder.Int32(requestHeader.CorrelationID) // correlation_id
	encoder.Uint8(0)                           // TAG_BUFFER: empty (response header v1)
	encoder.Int32(0)                           // throttle_time_ms
	encoder.Int16(0)                           // error_code: 0 (no error)
	encoder.Int32(0)                           // session_id
	//replase empty compact array with a compact array of 1 element
	encoder.Uint8(uint8(len(topicNames) + 1)) // topics array length
	for _, topicName := range topicNames {
		encoder.String(topicName)
		encoder.Uint8(2)
		encoder.Int32(0)
		encoder.Int32(100)
	}

	encoder.Uint8(0) // TAG_BUFFER: empty (response body)
	messageBytes := encoder.GetBytes()
	binary.BigEndian.PutUint32(messageBytes[0:4], uint32(len(messageBytes)-4))
	return messageBytes, nil
}
