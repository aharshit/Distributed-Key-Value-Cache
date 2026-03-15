package store

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"

	"github.com/user/kvcache/utils"
)

const headerSize = 17

type KeyEntry struct {
	TimeStamp     uint32
	ValuePosition uint32
	EntrySize     uint32
}

type Header struct {
	CheckSum  uint32
	Tombstone uint8
	TimeStamp uint32
	KeySize   uint32
	ValueSize uint32
}

type Record struct {
	Header     Header
	Key        string
	Value      string
	RecordSize uint32
}

func NewKeyEntry(timestamp, position, size uint32) KeyEntry {
	return KeyEntry{
		TimeStamp:     timestamp,
		ValuePosition: position,
		EntrySize:     size,
	}
}

func NewHeader(buf []byte) (*Header, error) {
	header := &Header{}
	err := header.DecodeHeader(buf)

	if err != nil {
		return nil, err
	}

	return header, err
}

func (h *Header) EncodeHeader(buf *bytes.Buffer) error {
	err := binary.Write(buf, binary.LittleEndian, &h.CheckSum)
	if err != nil {
		return utils.ErrEncodingHeaderFailed
	}
	err = binary.Write(buf, binary.LittleEndian, &h.Tombstone)
	if err != nil {
		return utils.ErrEncodingHeaderFailed
	}
	err = binary.Write(buf, binary.LittleEndian, &h.TimeStamp)
	if err != nil {
		return utils.ErrEncodingHeaderFailed
	}
	err = binary.Write(buf, binary.LittleEndian, &h.KeySize)
	if err != nil {
		return utils.ErrEncodingHeaderFailed
	}
	err = binary.Write(buf, binary.LittleEndian, &h.ValueSize)
	if err != nil {
		return utils.ErrEncodingHeaderFailed
	}

	return nil
}

func (h *Header) DecodeHeader(buf []byte) error {
	_, err := binary.Decode(buf[:4], binary.LittleEndian, &h.CheckSum)
	if err != nil {
		return utils.ErrEncodingHeaderFailed
	}
	_, err = binary.Decode(buf[4:5], binary.LittleEndian, &h.Tombstone)
	if err != nil {
		return utils.ErrEncodingHeaderFailed
	}
	_, err = binary.Decode(buf[5:9], binary.LittleEndian, &h.TimeStamp)
	if err != nil {
		return utils.ErrEncodingHeaderFailed
	}
	_, err = binary.Decode(buf[9:13], binary.LittleEndian, &h.KeySize)
	if err != nil {
		return utils.ErrEncodingHeaderFailed
	}
	_, err = binary.Decode(buf[13:17], binary.LittleEndian, &h.ValueSize)
	if err != nil {
		return utils.ErrEncodingHeaderFailed
	}

	return err
}

func (h *Header) MarkTombstone() {
	h.Tombstone = 1
}

func (r *Record) EncodeKV(buf *bytes.Buffer) error {
	err := r.Header.EncodeHeader(buf)
	if err != nil {
		return err
	}
	buf.WriteString(r.Key)
	_, err = buf.WriteString(r.Value)
	return err
}

func (r *Record) DecodeKV(buf []byte) error {
	err := r.Header.DecodeHeader(buf[:headerSize])
	r.Key = string(buf[headerSize : headerSize+r.Header.KeySize])
	r.Value = string(buf[headerSize+r.Header.KeySize : headerSize+r.Header.KeySize+r.Header.ValueSize])
	r.RecordSize = headerSize + r.Header.KeySize + r.Header.ValueSize
	return err
}

func (r *Record) Size() uint32 {
	return r.RecordSize
}

func (r *Record) CalculateChecksum() (uint32, error) {
	headerBuf := new(bytes.Buffer)
	err := binary.Write(headerBuf, binary.LittleEndian, &r.Header.Tombstone)
	if err != nil {
		return 0, err
	}
	err = binary.Write(headerBuf, binary.LittleEndian, &r.Header.TimeStamp)
	if err != nil {
		return 0, err
	}
	err = binary.Write(headerBuf, binary.LittleEndian, &r.Header.KeySize)
	if err != nil {
		return 0, err
	}
	err = binary.Write(headerBuf, binary.LittleEndian, &r.Header.ValueSize)
	if err != nil {
		return 0, err
	}

	kvBuf := append([]byte(r.Key), []byte(r.Value)...)
	buf := append(headerBuf.Bytes(), kvBuf...)

	return crc32.ChecksumIEEE(buf), nil
}
