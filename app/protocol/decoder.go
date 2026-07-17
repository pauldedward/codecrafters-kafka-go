package protocol

import (
	"bytes"
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

func NewDecoderFromBytes(data []byte) *Decoder {
	return &Decoder{reader: bytes.NewReader(data)}
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

func (d *Decoder) Int64() (int64, error) {
	var value int64
	err := binary.Read(d.reader, binary.BigEndian, &value)
	return value, err
}

func (d *Decoder) Int8() (int8, error) {
	var value int8
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

func (d *Decoder) VarUInt() (uint64, error) {
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

func (d *Decoder) VarInt() (int64, error) {
	value, err := d.VarUInt()
	if err != nil {
		return 0, err
	}
	return zigzagDecode(value), nil
}

func zigzagDecode(value uint64) int64 {
	return int64((value >> 1) ^ uint64((int64(value&1)<<63)>>63))
}

func (d *Decoder) VarInt32() (int32, error) {
	value, err := d.VarUInt()
	if err != nil {
		return 0, err
	}
	return int32(zigzagDecode(value)), nil
}

func (d *Decoder) VarInt64() (int64, error) {
	value, err := d.VarUInt()
	if err != nil {
		return 0, err
	}
	return zigzagDecode(value), nil
}

func (d *Decoder) VarInt8() (int8, error) {
	value, err := d.VarUInt()
	if err != nil {
		return 0, err
	}
	return int8(zigzagDecode(value)), nil
}

func (d *Decoder) CompactString() (string, error) {
	length, err := d.VarUInt()
	if err != nil {
		return "", err
	}
	return d.String(int(length - 1))
}

func (d *Decoder) CompactInt32Array() ([]int32, error) {
	length, err := d.VarUInt()
	if err != nil {
		return nil, err
	}
	arr := make([]int32, length-1)
	for i := 0; i < int(length-1); i++ {
		value, err := d.Int32()
		if err != nil {
			return nil, err
		}
		arr[i] = value
	}
	return arr, nil
}

func (d *Decoder) CompactBytes() ([]byte, error) {
	length, err := d.VarUInt()
	if err != nil {
		return nil, err

	}
	return d.Bytes(int(length - 1))
}
