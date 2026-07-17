package kafka

import (
	"github.com/codecrafters-io/kafka-starter-go/app/protocol"
)

type RecordBatch struct {
	BaseOffset           int64
	BatchLength          int32
	PartitionLeaderEpoch int32
	MagicByte            int8
	CRC                  int32
	Attributes           int16
	LastOffsetDelta      int32
	BaseTimestamp        int64
	MaxTimestamp         int64
	ProducerId           int64
	ProducerEpoch        int16
	BaseSequence         int32
	RecordsLength        int32
	Records              []Record[any]
}

func decodeRecordBatch(decoder *protocol.Decoder) (RecordBatch, error) {
	recordBatch := RecordBatch{}
	recordBatch.BaseOffset, _ = decoder.Int64()
	recordBatch.BatchLength, _ = decoder.Int32()
	recordBatch.PartitionLeaderEpoch, _ = decoder.Int32()
	recordBatch.MagicByte, _ = decoder.Int8()
	recordBatch.CRC, _ = decoder.Int32()
	recordBatch.Attributes, _ = decoder.Int16()
	recordBatch.LastOffsetDelta, _ = decoder.Int32()
	recordBatch.BaseTimestamp, _ = decoder.Int64()
	recordBatch.MaxTimestamp, _ = decoder.Int64()
	recordBatch.ProducerId, _ = decoder.Int64()
	recordBatch.ProducerEpoch, _ = decoder.Int16()
	recordBatch.BaseSequence, _ = decoder.Int32()
	recordBatch.RecordsLength, _ = decoder.Int32()
	recordBatch.Records = make([]Record[any], recordBatch.RecordsLength)
	return recordBatch, nil
}

// write a method to decode record batch from file and return a slice of record batches
func getRecordBatchFromFile(filePath string) ([]RecordBatch, error) {
	decoder, err := protocol.NewDecoderFromFile(filePath)
	if err != nil {
		return nil, err
	}
	recordBatches := []RecordBatch{}
	for {
		recordBatch, err := decodeRecordBatch(decoder)
		if err != nil {
			break
		}
		recordBatches = append(recordBatches, recordBatch)
	}
	return recordBatches, nil
}

// func getRecordBatchFromFile(data []byte) ([]RecordBatch, error) {
// 	var recordBatches []RecordBatch
// 	if err := json.Unmarshal(data, &recordBatches); err != nil {
// 		return nil, err
// 	}
// 	return recordBatches, nil
// }
