package protocol

import (
	"bytes"
	"encoding/binary"
)

type Encoder struct {
	buffer bytes.Buffer
}

func NewEncoder() *Encoder {
	return &Encoder{buffer: bytes.Buffer{}}
}

func (e *Encoder) Int16(value int16) {
	binary.Write(&e.buffer, binary.BigEndian, value)
}

func (e *Encoder) Int32(value int32) {
	binary.Write(&e.buffer, binary.BigEndian, value)
}

func (e *Encoder) Int64(value int64) {
	binary.Write(&e.buffer, binary.BigEndian, value)
}

func (e *Encoder) Uint8(value uint8) {
	e.buffer.WriteByte(value)
}

func (e *Encoder) Int8(value int8) {
	binary.Write(&e.buffer, binary.BigEndian, value)
}

func (e *Encoder) Bytes(data []byte) {
	e.buffer.Write(data)
}

func (e *Encoder) GetBytes() []byte {
	return e.buffer.Bytes()
}

func (e *Encoder) String(value string) {
	e.Uint8(uint8(len(value) + 1))
	e.buffer.WriteString(value)
}
