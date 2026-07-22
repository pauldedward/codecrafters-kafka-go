package kafka

import (
	binary "encoding/binary"

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
	ClusterId           string
	ReplicaStates       []ReplicaState
}

type ReplicaState struct {
	ReplicaId    int32
	ReplicaEpoch int64
}

type ForgottenTopicData struct {
	TopicId    string
	Partitions []int32
}
type FetchTopic struct {
	TopicId    string
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

func parseFetchPartition(decoder *protocol.Decoder) FetchPartition {
	return FetchPartition{
		Partition:          func() int32 { v, _ := decoder.Int32(); return v }(),
		CurrentLeaderEpoch: func() int32 { v, _ := decoder.Int32(); return v }(),
		FetchOffset:        func() int64 { v, _ := decoder.Int64(); return v }(),
		LastFetchedEpoch:   func() int32 { v, _ := decoder.Int32(); return v }(),
		LogStartOffset:     func() int64 { v, _ := decoder.Int64(); return v }(),
		PartitionMaxBytes:  func() int32 { v, _ := decoder.Int32(); return v }(),
	}
}

func parseFetchPartitions(decoder *protocol.Decoder) []FetchPartition {
	partitionCount, _ := decoder.VarUInt()
	partitions := make([]FetchPartition, partitionCount-1)
	for i := 0; i < int(partitionCount-1); i++ {
		partitions[i] = parseFetchPartition(decoder)
	}
	return partitions
}

func parseFetchTopic(decoder *protocol.Decoder) FetchTopic {
	return FetchTopic{
		TopicId:    func() string { v, _ := decoder.CompactString(); return v }(),
		Partitions: parseFetchPartitions(decoder),
	}
}

func parseFetchTopics(decoder *protocol.Decoder) []FetchTopic {
	topicCount, _ := decoder.VarUInt()
	topics := make([]FetchTopic, topicCount-1)
	for i := 0; i < int(topicCount-1); i++ {
		topics[i] = parseFetchTopic(decoder)
	}
	return topics
}

func parseForgottenTopicData(decoder *protocol.Decoder) []ForgottenTopicData {
	topicCount, _ := decoder.VarUInt()
	forgottenTopics := make([]ForgottenTopicData, topicCount-1)
	for i := 0; i < int(topicCount-1); i++ {
		forgottenTopics[i] = ForgottenTopicData{
			TopicId:    func() string { v, _ := decoder.CompactString(); return v }(),
			Partitions: func() []int32 { v, _ := decoder.CompactInt32Array(); return v }(),
		}
	}
	return forgottenTopics
}

func parseReplicaStates(decoder *protocol.Decoder) []ReplicaState {
	replicaCount, _ := decoder.VarUInt()
	replicas := make([]ReplicaState, replicaCount-1)
	for i := 0; i < int(replicaCount-1); i++ {
		replicas[i] = ReplicaState{
			ReplicaId:    func() int32 { v, _ := decoder.Int32(); return v }(),
			ReplicaEpoch: func() int64 { v, _ := decoder.Int64(); return v }(),
		}
	}
	return replicas
}

func parseFetchRequestBody(decoder *protocol.Decoder) (FetchRequestBody, error) {
	//version 16
	fetchRequestBody := FetchRequestBody{}
	fetchRequestBody.MaxWaitMs, _ = decoder.Int32()
	fetchRequestBody.MinBytes, _ = decoder.Int32()
	fetchRequestBody.MaxBytes, _ = decoder.Int32()
	fetchRequestBody.IsolationLevel, _ = decoder.Int8()
	fetchRequestBody.SessionID, _ = decoder.Int32()
	fetchRequestBody.SessionEpoch, _ = decoder.Int32()
	fetchRequestBody.Topics = parseFetchTopics(decoder)
	fetchRequestBody.ForgottenTopicsData = parseForgottenTopicData(decoder)
	fetchRequestBody.RackId, _ = decoder.CompactString()
	fetchRequestBody.ClusterId, _ = decoder.CompactString()
	fetchRequestBody.ReplicaStates = parseReplicaStates(decoder)
	return fetchRequestBody, nil
}

func HandleFetch(requestHeader RequestHeader, decoder *protocol.Decoder) ([]byte, error) {
	//decode request body
	fetchRequestBody, err := parseFetchRequestBody(decoder)
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
	//replase empty compact array with a compact array of 1 element
	// topics array length
	encoder.Uint8(uint8(len(fetchRequestBody.Topics)) + 1)
	for _, topic := range fetchRequestBody.Topics {
		encoder.String(topic.TopicId)
		encoder.Uint8(2)
		encoder.Int32(0)
		encoder.Int32(100)
	}

	encoder.Uint8(0) // TAG_BUFFER: empty (response body)
	messageBytes := encoder.GetBytes()
	binary.BigEndian.PutUint32(messageBytes[0:4], uint32(len(messageBytes)-4))
	return messageBytes, nil
}
