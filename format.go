package diskkv

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
)

const (
	// MaxKeySize is the maximum key length in bytes. Keys must not be empty.
	MaxKeySize = 4 << 10
	// MaxValueSize is the maximum value length in bytes. Empty values are valid.
	MaxValueSize = 1 << 20

	fileHeaderSize   = 16
	recordHeaderSize = 24
	formatVersion    = 1
)

var (
	fileMagic   = [8]byte{'D', 'I', 'S', 'K', 'K', 'V', 0, 0}
	recordMagic = [4]byte{'D', 'K', 'V', 'R'}
	crcTable    = crc32.MakeTable(crc32.Castagnoli)
)

type recordKind byte

const (
	kindSet    recordKind = 1
	kindDelete recordKind = 2
)

type record struct {
	kind       recordKind
	key, value []byte
}

func encodeFileHeader() []byte {
	header := make([]byte, fileHeaderSize)
	copy(header[:8], fileMagic[:])
	binary.BigEndian.PutUint32(header[8:12], formatVersion)
	binary.BigEndian.PutUint32(header[12:16], crc32.Checksum(header[:12], crcTable))
	return header
}

func validateFileHeader(header []byte) error {
	if len(header) < fileHeaderSize {
		return fmt.Errorf("%w: incomplete file header", ErrTruncated)
	}
	if len(header) != fileHeaderSize {
		return fmt.Errorf("%w: invalid file header size", ErrCorrupt)
	}
	if !bytes.Equal(header[:8], fileMagic[:]) {
		return fmt.Errorf("%w: invalid file magic", ErrCorrupt)
	}
	if binary.BigEndian.Uint32(header[12:16]) != crc32.Checksum(header[:12], crcTable) {
		return fmt.Errorf("%w: file header checksum mismatch", ErrCorrupt)
	}
	if version := binary.BigEndian.Uint32(header[8:12]); version != formatVersion {
		return fmt.Errorf("%w: file format version %d", ErrUnsupportedVersion, version)
	}
	return nil
}

func encodeRecord(kind recordKind, key, value []byte) ([]byte, error) {
	if len(key) == 0 || len(key) > MaxKeySize {
		return nil, ErrInvalidKey
	}
	if len(value) > MaxValueSize {
		return nil, ErrValueTooLarge
	}
	if kind != kindSet && kind != kindDelete {
		return nil, fmt.Errorf("%w: invalid record kind %d", ErrCorrupt, kind)
	}
	if kind == kindDelete && len(value) != 0 {
		return nil, fmt.Errorf("%w: delete record has a value", ErrCorrupt)
	}
	encoded := make([]byte, recordHeaderSize+len(key)+len(value))
	copy(encoded[:4], recordMagic[:])
	encoded[4] = byte(kind)
	binary.BigEndian.PutUint32(encoded[8:12], uint32(len(key)))
	binary.BigEndian.PutUint32(encoded[12:16], uint32(len(value)))
	binary.BigEndian.PutUint32(encoded[16:20], crc32.Checksum(encoded[:16], crcTable))
	copy(encoded[recordHeaderSize:], key)
	copy(encoded[recordHeaderSize+len(key):], value)
	binary.BigEndian.PutUint32(encoded[20:24], crc32.Checksum(encoded[recordHeaderSize:], crcTable))
	return encoded, nil
}

// readRecordAt reads a single bounded record without modifying a shared file
// cursor. end is the exclusive upper bound known to the caller. The fixed-size
// header has its own checksum so corrupt lengths are rejected before allocation.
func readRecordAt(r io.ReaderAt, offset, end int64) (record, int64, error) {
	if offset < 0 || end < offset {
		return record{}, 0, fmt.Errorf("%w: invalid record bounds", ErrCorrupt)
	}
	if offset == end {
		return record{}, offset, io.EOF
	}
	if end-offset < recordHeaderSize {
		return record{}, 0, fmt.Errorf("%w: record header at offset %d", ErrTruncated, offset)
	}
	var header [recordHeaderSize]byte
	if err := readExactAt(r, header[:], offset); err != nil {
		return record{}, 0, fmt.Errorf("record header at offset %d: %w", offset, err)
	}
	if !bytes.Equal(header[:4], recordMagic[:]) {
		return record{}, 0, fmt.Errorf("%w: record magic at offset %d", ErrCorrupt, offset)
	}
	if binary.BigEndian.Uint32(header[16:20]) != crc32.Checksum(header[:16], crcTable) {
		return record{}, 0, fmt.Errorf("%w: record header checksum at offset %d", ErrCorrupt, offset)
	}
	kind := recordKind(header[4])
	if kind != kindSet && kind != kindDelete {
		return record{}, 0, fmt.Errorf("%w: record kind at offset %d", ErrCorrupt, offset)
	}
	if header[5] != 0 || header[6] != 0 || header[7] != 0 {
		return record{}, 0, fmt.Errorf("%w: reserved record flags at offset %d", ErrCorrupt, offset)
	}
	keyLen := binary.BigEndian.Uint32(header[8:12])
	valueLen := binary.BigEndian.Uint32(header[12:16])
	if keyLen == 0 || keyLen > MaxKeySize || valueLen > MaxValueSize || (kind == kindDelete && valueLen != 0) {
		return record{}, 0, fmt.Errorf("%w: invalid record lengths at offset %d", ErrCorrupt, offset)
	}
	// Convert only after enforcing the format limits. Comparing the span against
	// end-offset avoids overflowing offset when inputs approach MaxInt64.
	payloadLen := int64(keyLen) + int64(valueLen)
	span := int64(recordHeaderSize) + payloadLen
	if span > end-offset {
		return record{}, 0, fmt.Errorf("%w: record payload at offset %d", ErrTruncated, offset)
	}
	payload := make([]byte, int(payloadLen))
	if err := readExactAt(r, payload, offset+recordHeaderSize); err != nil {
		return record{}, 0, fmt.Errorf("record payload at offset %d: %w", offset, err)
	}
	if binary.BigEndian.Uint32(header[20:24]) != crc32.Checksum(payload, crcTable) {
		return record{}, 0, fmt.Errorf("%w: record payload checksum at offset %d", ErrCorrupt, offset)
	}
	return record{kind: kind, key: payload[:int(keyLen)], value: payload[int(keyLen):]}, offset + span, nil
}

func readExactAt(r io.ReaderAt, buf []byte, offset int64) error {
	n, err := r.ReadAt(buf, offset)
	if n == len(buf) && (err == nil || err == io.EOF) {
		return nil
	}
	if err == io.EOF || err == io.ErrUnexpectedEOF {
		return ErrTruncated
	}
	if err != nil {
		return err
	}
	// A short read without an error violates io.ReaderAt's contract. Do not
	// classify it as a repairable tail: the underlying reader has malfunctioned.
	return io.ErrNoProgress
}
