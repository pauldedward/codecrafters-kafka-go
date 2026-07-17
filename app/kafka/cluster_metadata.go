package kafka

import (
	"github.com/codecrafters-io/kafka-starter-go/app/protocol"
)

type PartitionMeta struct {
	PartitionIndex int32
	LeaderID       int32
	LeaderEpoch    int32
	Replicas       []int32
	ISR            []int32
}

type TopicMeta struct {
	Name       string
	TopicID    [16]byte
	Partitions map[int32]PartitionMeta
}

type ClusterMeta struct {
	TopicsByName map[string]TopicMeta
	TopicsByID   map[[16]byte]string
}

func (r *RecordBatch) GetRecords() []Record[any] {
	return r.Records
}

func (r *RecordBatch) GetPartitionById(partitionId int32) *Record[any] {
	for _, record := range r.Records {
		if record.Value != nil {

		}
	}
	return nil
}

func GetClusterMetadataFromFile(filePath string) (*ClusterMeta, error) {
	decoder, err := protocol.NewDecoderFromFile(filePath)
	if err != nil {
		return nil, err
	}

	metadata := &ClusterMeta{
		TopicsByName: make(map[string]TopicMeta),
		TopicsByID:   make(map[[16]byte]string),
	}

	for {
		err := decodeOneRecordBatch(decoder, metadata)
		if err != nil {
			break
		}
	}
	return metadata, nil
}

func decodeOneRecordBatch(decoder *protocol.Decoder, metadata *ClusterMeta) error {
	// Read the record batch header
	_, err := decoder.Int64() // base_offset
	if err != nil {
		return err
	}

	batchLength, err := decoder.Int32() // batch_length
	if err != nil {
		return err
	}

	batchData, err := decoder.Bytes(int(batchLength))
	if err != nil {
		return err
	}

	batchDecoder := protocol.NewDecoderFromBytes(batchData)

	// Now read the fixed batch header fields from the bounded decoder
	batchDecoder.Int32() // PartitionLeaderEpoch (skip)
	batchDecoder.Int8()  // MagicByte (skip, should be 2)
	batchDecoder.Int32() // CRC (skip)
	batchDecoder.Int16() // Attributes (skip)
	batchDecoder.Int32() // LastOffsetDelta (skip)
	batchDecoder.Int64() // BaseTimestamp (skip)
	batchDecoder.Int64() // MaxTimestamp (skip)
	batchDecoder.Int64() // ProducerId (skip)
	batchDecoder.Int16() // ProducerEpoch (skip)
	batchDecoder.Int32() // BaseSequence (skip)

	recordsLength, err := batchDecoder.Int32() // Records array length
	if err != nil {
		return err
	}

	for i := 0; i < int(recordsLength); i++ {
		// Read each record
		decodeRecord(batchDecoder, metadata)
	}

	return nil
}

func decodeRecord(decoder *protocol.Decoder, metadata *ClusterMeta) error {
	recordLength, err := decoder.VarInt()
	if err != nil {
		return err
	}

	recordData, err := decoder.Bytes(int(recordLength))
	if err != nil {
		return err
	}

	recordDecoder := protocol.NewDecoderFromBytes(recordData)
	// Further decoding of the record can be done here
	recordDecoder.Int8()   // Attributes (skip)
	recordDecoder.VarInt() // TimestampDelta (skip)
	recordDecoder.VarInt() // OffsetDelta (skip)

	keyLength, err := recordDecoder.VarInt32()
	if err != nil {
		return err
	}

	if keyLength > 0 {
		_, err = recordDecoder.Bytes(int(keyLength))
		if err != nil {
			return err
		}
	}

	valueLength, err := recordDecoder.VarInt32()
	if err != nil {
		return err
	}

	if valueLength > 0 {
		value, _ := recordDecoder.Bytes(int(valueLength))
		decodeRecordValue(value, metadata)
	}
	return nil
}

func decodeRecordValue(value []byte, metadata *ClusterMeta) {
	decoder := protocol.NewDecoderFromBytes(value)
	decoder.Int8()
	recordType, _ := decoder.Int8()
	_, _ = decoder.Int8() // Version

	switch recordType {
	case 2: // Topic Record
		decodeTopicRecord(decoder, metadata)
	case 3: // Partition Record
		decodePartitionRecord(decoder, metadata)
	}
}

func decodeTopicRecord(decoder *protocol.Decoder, metadata *ClusterMeta) {
	name, _ := decoder.CompactString()

	idBytes, _ := decoder.Bytes(16)
	var topicID [16]byte
	copy(topicID[:], idBytes)

	metadata.TopicsByID[topicID] = name
	metadata.TopicsByName[name] = TopicMeta{
		Name:       name,
		TopicID:    topicID,
		Partitions: make(map[int32]PartitionMeta),
	}
}

func decodePartitionRecord(decoder *protocol.Decoder, metadata *ClusterMeta) {
	partitionId, _ := decoder.Int32()
	topicIdBytes, _ := decoder.Bytes(16)
	var topicID [16]byte
	copy(topicID[:], topicIdBytes)

	replicas, _ := decoder.CompactInt32Array()
	isr, _ := decoder.CompactInt32Array()

	decoder.CompactInt32Array()
	decoder.CompactInt32Array()

	leader, _ := decoder.Int32()

	leaderEpoch, _ := decoder.Int32()

	decoder.Int32() // PartitionEpoch (skip)

	decoder.CompactBytes() // DirectoryArray (skip)

	decoder.VarInt32() // TaggedFieldsCount (skip)

	if topicName, exists := metadata.TopicsByID[topicID]; exists {
		topicMeta := metadata.TopicsByName[topicName]
		topicMeta.Partitions[partitionId] = PartitionMeta{
			PartitionIndex: partitionId,
			LeaderID:       leader,
			LeaderEpoch:    leaderEpoch,
			Replicas:       replicas,
			ISR:            isr,
		}
		metadata.TopicsByName[topicName] = topicMeta
	}

}
