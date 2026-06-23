package protocol

import (
	"encoding/binary"
	"io"
)

type Decoder struct {
	reader io.Reader
}

func NewDecoder(r io.Reader) *Decoder {
	return &Decoder{reader: r}
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
