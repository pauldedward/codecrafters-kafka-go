package kafka

type FeatureLevelRecord struct {
	FrameVersion      int8
	Type              int8
	Version           int8
	NameLength        uint32
	Name              []byte
	FeatureLevel      int16
	TaggedFieldsCount uint32
}

type TopicRecord struct {
	FrameVersion int8
	Type         int8
	Version      int8
	//Namelength unsigned varint
	NameLength        uint32
	TopicName         []byte
	TopicId           []byte
	TaggedFieldsCount uint32
}

type PartitionRecord struct {
	FrameVersion                  int8
	Type                          int8
	Version                       int8
	PartitionId                   int32
	TopicUUID                     []byte
	LengthOfReplicaArray          uint32
	ReplicaArray                  []int32
	LengthOfISRArray              uint32
	ISRArray                      []int32
	LengthOfRemovingReplicasArray uint32
	LengthOfAddingReplicasArray   uint32
	Leader                        int32
	LeaderEpoch                   int32
	PartitionEpoch                int32
	LengthOfDirectoryArray        uint32
	DirectoryArray                []byte
	TaggedFieldsCount             uint32
}

type Record[T any] struct {
	Length            int32
	Attributes        int8
	TimestampDelta    int64
	OffsetDelta       int32
	Key               []byte
	ValueLength       int32
	Value             T
	HeadersArrayCount int32
}

//store value to struct type of record values

// func decodeRecord[T any](decoder *protocol.Decoder) (Record[T], error) {
// 	record := Record[T]{}
// 	record.Length, _ = decoder.Int32()
// 	record.Attributes, _ = decoder.Int8()
// 	record.TimestampDelta, _ = decoder.Int64()
// 	record.OffsetDelta, _ = decoder.Int32()
// 	record.Key, _ = decoder.Bytes(int(record.Length))
// 	record.ValueLength, _ = decoder.Int32()
// 	// read frame version of record value
// 	frameVersion, _ := decoder.Int8()
// 	// read type of record value
// 	recordType, _ := decoder.Int8()
// 	record.Value, _ =
// 	record.HeadersArrayCount, _ = decoder.Int32()
// 	return record, nil
// }
