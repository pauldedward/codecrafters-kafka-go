package kafka

import (
	binary "encoding/binary"
	"fmt"
	"os"

	protocol "github.com/codecrafters-io/kafka-starter-go/app/protocol"
)

type FetchRequestBody struct {
	MaxWaitMs           int32
	MinBytes            int32
	MaxBytes            int32
	IsolationLevel      int8
	SessionID           int32
	SessionEpoch        int32
	Topics              []FetchTopic
	ForgottenTopicsData []ForgottenTopicData
	RackId              string
}

type ReplicaState struct {
	ReplicaId    int32
	ReplicaEpoch int64
}

type ForgottenTopicData struct {
	TopicId    []byte
	Partitions []int32
}
type FetchTopic struct {
	TopicId    []byte
	Partitions []FetchPartition
}

type FetchPartition struct {
	Partition          int32
	CurrentLeaderEpoch int32
	FetchOffset        int64
	LastFetchedEpoch   int32
	LogStartOffset     int64
	PartitionMaxBytes  int32
}

func parseFetchPartition(decoder *protocol.Decoder) (FetchPartition, error) {
	partition, err := decoder.Int32()
	if err != nil {
		return FetchPartition{}, err
	}
	currentLeaderEpoch, err := decoder.Int32()
	if err != nil {
		return FetchPartition{}, err
	}
	fetchOffset, err := decoder.Int64()
	if err != nil {
		return FetchPartition{}, err
	}
	lastFetchedEpoch, err := decoder.Int32()
	if err != nil {
		return FetchPartition{}, err
	}
	logStartOffset, err := decoder.Int64()
	if err != nil {
		return FetchPartition{}, err
	}
	partitionMaxBytes, err := decoder.Int32()
	if err != nil {
		return FetchPartition{}, err
	}
	if err := skipTaggedFields(decoder); err != nil {
		return FetchPartition{}, err
	}

	return FetchPartition{
		Partition:          partition,
		CurrentLeaderEpoch: currentLeaderEpoch,
		FetchOffset:        fetchOffset,
		LastFetchedEpoch:   lastFetchedEpoch,
		LogStartOffset:     logStartOffset,
		PartitionMaxBytes:  partitionMaxBytes,
	}, nil
}

func parseFetchPartitions(decoder *protocol.Decoder) ([]FetchPartition, error) {
	rawCount, err := decoder.VarUInt()
	if err != nil {
		return nil, err
	}
	if rawCount == 0 {
		return []FetchPartition{}, nil
	}

	partitionCount := int(rawCount - 1)
	partitions := make([]FetchPartition, partitionCount)
	for i := 0; i < partitionCount; i++ {
		partition, err := parseFetchPartition(decoder)
		if err != nil {
			return nil, err
		}
		partitions[i] = partition
	}
	return partitions, nil
}

func parseFetchTopic(decoder *protocol.Decoder) (FetchTopic, error) {
	topicID, err := decoder.Bytes(16)
	if err != nil {
		return FetchTopic{}, err
	}
	partitions, err := parseFetchPartitions(decoder)
	if err != nil {
		return FetchTopic{}, err
	}
	if err := skipTaggedFields(decoder); err != nil {
		return FetchTopic{}, err
	}

	return FetchTopic{TopicId: topicID, Partitions: partitions}, nil
}

func parseFetchTopics(decoder *protocol.Decoder) ([]FetchTopic, error) {
	rawCount, err := decoder.VarUInt()
	if err != nil {
		return nil, err
	}
	if rawCount == 0 {
		return []FetchTopic{}, nil
	}

	topicCount := int(rawCount - 1)
	topics := make([]FetchTopic, topicCount)
	for i := 0; i < topicCount; i++ {
		topic, err := parseFetchTopic(decoder)
		if err != nil {
			return nil, err
		}
		topics[i] = topic
	}
	return topics, nil
}

func parseForgottenTopicData(decoder *protocol.Decoder) ([]ForgottenTopicData, error) {
	rawCount, err := decoder.VarUInt()
	if err != nil {
		return nil, err
	}
	if rawCount == 0 {
		return []ForgottenTopicData{}, nil
	}

	topicCount := int(rawCount - 1)
	forgottenTopics := make([]ForgottenTopicData, topicCount)
	for i := 0; i < topicCount; i++ {
		topicID, err := decoder.Bytes(16)
		if err != nil {
			return nil, err
		}
		partitions, err := decoder.CompactInt32Array()
		if err != nil {
			return nil, err
		}
		if err := skipTaggedFields(decoder); err != nil {
			return nil, err
		}

		forgottenTopics[i] = ForgottenTopicData{
			TopicId:    topicID,
			Partitions: partitions,
		}
	}
	return forgottenTopics, nil
}

func skipTaggedFields(decoder *protocol.Decoder) error {
	tagCount, err := decoder.VarUInt()
	if err != nil {
		return err
	}

	for i := uint64(0); i < tagCount; i++ {
		_, err := decoder.VarUInt() // tag id
		if err != nil {
			return err
		}
		size, err := decoder.VarUInt()
		if err != nil {
			return err
		}
		_, err = decoder.Bytes(int(size))
		if err != nil {
			return err
		}
	}

	return nil
}

func parseFetchRequestBody(decoder *protocol.Decoder) (FetchRequestBody, error) {
	//version 16
	fetchRequestBody := FetchRequestBody{}
	var err error
	fetchRequestBody.MaxWaitMs, err = decoder.Int32()
	if err != nil {
		return FetchRequestBody{}, err
	}
	fetchRequestBody.MinBytes, err = decoder.Int32()
	if err != nil {
		return FetchRequestBody{}, err
	}
	fetchRequestBody.MaxBytes, err = decoder.Int32()
	if err != nil {
		return FetchRequestBody{}, err
	}
	fetchRequestBody.IsolationLevel, err = decoder.Int8()
	if err != nil {
		return FetchRequestBody{}, err
	}
	fetchRequestBody.SessionID, err = decoder.Int32()
	if err != nil {
		return FetchRequestBody{}, err
	}
	fetchRequestBody.SessionEpoch, err = decoder.Int32()
	if err != nil {
		return FetchRequestBody{}, err
	}
	fetchRequestBody.Topics, err = parseFetchTopics(decoder)
	if err != nil {
		return FetchRequestBody{}, err
	}
	fetchRequestBody.ForgottenTopicsData, err = parseForgottenTopicData(decoder)
	if err != nil {
		return FetchRequestBody{}, err
	}
	fetchRequestBody.RackId, err = decoder.CompactString()
	if err != nil {
		return FetchRequestBody{}, err
	}
	if err := skipTaggedFields(decoder); err != nil {
		return FetchRequestBody{}, err
	}
	return fetchRequestBody, nil
}

func HandleFetch(requestHeader RequestHeader, decoder *protocol.Decoder) ([]byte, error) {
	//decode request body
	fetchRequestBody, err := parseFetchRequestBody(decoder)
	if err != nil {
		return nil, err
	}
	//get cluster metadata
	clusterMetaData, err := GetClusterMetadataFromFile("/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log")

	if err != nil {
		return nil, err
	}
	encoder := protocol.NewEncoder()
	encoder.Int32(0)                           // placeholder for message length
	encoder.Int32(requestHeader.CorrelationID) // correlation_id
	encoder.Uint8(0)                           // TAG_BUFFER: empty (response header v1)
	encoder.Int32(0)                           // throttle_time_ms
	encoder.Int16(0)                           // error_code: 0 (no error)
	encoder.Int32(0)                           // session_id

	// topics compact array length
	encoder.Uint8(uint8(len(fetchRequestBody.Topics)) + 1)
	for _, topic := range fetchRequestBody.Topics {
		encoder.Bytes(topic.TopicId)
		encoder.Uint8(uint8(len(topic.Partitions)) + 1)

		var topicID [16]byte
		copy(topicID[:], topic.TopicId)

		topicName, topicExists := clusterMetaData.TopicsByID[topicID]

		for _, partition := range topic.Partitions {
			encoder.Int32(partition.Partition)

			var recordBytes []byte = nil

			if !topicExists {
				encoder.Int16(100) // UNKNOWN_TOPIC_ID
			} else {
				encoder.Int16(0) // no error
				filePath := fmt.Sprintf("/tmp/kraft-combined-logs/%s-%d/00000000000000000000.log", topicName, partition.Partition)
				data, err := os.ReadFile(filePath)
				if err == nil && len(data) > 0 {
					recordBytes = data
				}
			}

			encoder.Int64(0)  // high_watermark
			encoder.Int64(0)  // last_stable_offset
			encoder.Int64(0)  // log_start_offset
			encoder.Uint8(1)  // aborted_transactions: empty compact array
			encoder.Int32(-1) // preferred_read_replica
			encoder.NullableCompactBytes(recordBytes)
			encoder.Uint8(0) // tagged fields
		}
		encoder.Uint8(0) // topic tagged fields
	}
	encoder.Uint8(0) // TAG_BUFFER: empty (response body)
	messageBytes := encoder.GetBytes()
	binary.BigEndian.PutUint32(messageBytes[0:4], uint32(len(messageBytes)-4))
	return messageBytes, nil
}
