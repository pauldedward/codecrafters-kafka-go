package protocol

import (
	"encoding/binary"
	"io"
	"os"
)

type Decoder struct {
	reader io.Reader
}

func NewDecoder(r io.Reader) *Decoder {
	return &Decoder{reader: r}
}

func NewDecoderFromFile(filePath string) (*Decoder, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	return &Decoder{reader: file}, nil
}

func (d *Decoder) Int16() (int16, error) {
	var value int16
	err := binary.Read(d.reader, binary.BigEndian, &value)
	return value, err
}

func (d *Decoder) Int32() (int32, error) {
	var value int32
	err := binary.Read(d.reader, binary.BigEndian, &value)
	return value, err
}

func (d *Decoder) Uint8() (uint8, error) {
	var value uint8
	err := binary.Read(d.reader, binary.BigEndian, &value)
	return value, err
}

func (d *Decoder) Bytes(n int) ([]byte, error) {
	buf := make([]byte, n)
	_, err := io.ReadFull(d.reader, buf)
	return buf, err
}

func (d *Decoder) String(n int) (string, error) {
	buf := make([]byte, n)
	_, err := io.ReadFull(d.reader, buf)
	return string(buf), err
}

func (d *Decoder) varUInt() (uint64, error) {
	var value uint64
	var shift uint

	for {
		b, err := d.Uint8()
		if err != nil {
			return 0, err
		}

		value |= uint64(b&0x7F) << shift
		if b&0x80 == 0 {
			break
		}
		shift += 7
	}

	return value, nil
}

func (d *Decoder) VarInt32() (int32, error) {
	value, err := d.varUInt()
	if err != nil {
		return 0, err
	}
	return int32(value), nil
}

func (d *Decoder) VarInt64() (int64, error) {
	value, err := d.varUInt()
	if err != nil {
		return 0, err
	}
	return int64(value), nil
}

func (d *Decoder) VarInt8() (int8, error) {
	value, err := d.varUInt()
	if err != nil {
		return 0, err
	}
	return int8(value), nil
}
