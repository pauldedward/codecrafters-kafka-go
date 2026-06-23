package kafka

import (
	decoder "github.com/codecrafters-io/kafka-starter-go/app/protocol"
)

type RequestHeader struct {
	ApiKey        int16
	ApiVersion    int16
	CorrelationID int32
	TagBuffer     byte
}

type Request struct {
	MessageLength int32
	Header        RequestHeader
	Body          []byte
}

func ParseHeader(decoder *decoder.Decoder) (RequestHeader, error) {
	apiKey, err := decoder.Int16()
	if err != nil {
		return RequestHeader{}, err
	}

	apiVersion, err := decoder.Int16()
	if err != nil {
		return RequestHeader{}, err
	}

	correlationID, err := decoder.Int32()
	if err != nil {
		return RequestHeader{}, err
	}

	return RequestHeader{
		ApiKey:        apiKey,
		ApiVersion:    apiVersion,
		CorrelationID: correlationID,
	}, nil
}
