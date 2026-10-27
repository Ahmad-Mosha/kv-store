package diskkv

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"hash/crc32"
	"io"
	"math"
	"testing"
)

func TestFormatGolden(t *testing.T) {
	// Fixed vectors protect the persisted format against accidental byte-order,
	// checksum-polynomial, and layout changes.
	if got := hex.EncodeToString(encodeFileHeader()); got != "4449534b4b56000000000001b84d6035" {
		t.Fatalf("file header = %s", got)
	}
	encoded, err := encodeRecord(kindSet, []byte{0}, []byte{0xff})
	if err != nil {
		t.Fatal(err)
	}
	if got := hex.EncodeToString(encoded); got != "444b5652010000000000000100000001a6b7cd435c1c248300ff" {
		t.Fatalf("record = %s", got)
	}
}

func TestFileHeaderValidation(t *testing.T) {
	header := encodeFileHeader()
	if err := validateFileHeader(header); err != nil {
		t.Fatal(err)
	}
	for length := 0; length < fileHeaderSize; length++ {
		if err := validateFileHeader(header[:length]); !errors.Is(err, ErrTruncated) {
			t.Errorf("length %d: expected truncated header, got %v", length, err)
		}
	}
	if err := validateFileHeader(append(bytes.Clone(header), 0)); !errors.Is(err, ErrCorrupt) {
		t.Errorf("oversized header: got %v", err)
	}
	for index := range header {
		broken := bytes.Clone(header)
		broken[index] ^= 1
		if err := validateFileHeader(broken); !errors.Is(err, ErrCorrupt) {
			t.Errorf("byte %d: expected corruption, got %v", index, err)
		}
	}
	unknown := bytes.Clone(header)
	binary.BigEndian.PutUint32(unknown[8:12], formatVersion+1)
	binary.BigEndian.PutUint32(unknown[12:16], crc32.Checksum(unknown[:12], crcTable))
	if err := validateFileHeader(unknown); !errors.Is(err, ErrUnsupportedVersion) {
		t.Errorf("unknown version: got %v", err)
	}
}

func TestRecordRoundTrip(t *testing.T) {
	cases := []struct {
		name       string
		kind       recordKind
		key, value []byte
	}{
		{"binary", kindSet, []byte{0, 0xff, '\n'}, []byte{0xff, 0, '\n', '\r'}},
		{"empty value", kindSet, []byte("empty"), nil},
		{"maximum sizes", kindSet, bytes.Repeat([]byte{'k'}, MaxKeySize), bytes.Repeat([]byte{'v'}, MaxValueSize)},
		{"delete", kindDelete, []byte("gone"), nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			encoded, err := encodeRecord(tc.kind, tc.key, tc.value)
			if err != nil {
				t.Fatal(err)
			}
			// Prefix and suffix exercise nonzero offsets and exact record bounds.
			data := append([]byte("prefix"), encoded...)
			end := int64(len(data))
			data = append(data, []byte("suffix")...)
			got, next, err := readRecordAt(bytes.NewReader(data), 6, end)
			if err != nil {
				t.Fatal(err)
			}
			if next != end || got.kind != tc.kind || !bytes.Equal(got.key, tc.key) || !bytes.Equal(got.value, tc.value) {
				t.Fatalf("round trip mismatch: next=%d want=%d, kind=%d", next, end, got.kind)
			}
			if _, _, err := readRecordAt(bytes.NewReader(data), end, end); !errors.Is(err, io.EOF) {
				t.Errorf("exact EOF: got %v", err)
			}
		})
	}
}

func TestEncodeRecordRejectsInvalidInput(t *testing.T) {
	cases := []struct {
		name       string
		kind       recordKind
		key, value []byte
		want       error
	}{
		{"empty key", kindSet, nil, nil, ErrInvalidKey},
		{"large key", kindSet, make([]byte, MaxKeySize+1), nil, ErrInvalidKey},
		{"large value", kindSet, []byte("k"), make([]byte, MaxValueSize+1), ErrValueTooLarge},
		{"unknown operation", 0xff, []byte("k"), nil, ErrCorrupt},
		{"delete payload", kindDelete, []byte("k"), []byte("v"), ErrCorrupt},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := encodeRecord(tc.kind, tc.key, tc.value); !errors.Is(err, tc.want) {
				t.Fatalf("got %v, want %v", err, tc.want)
			}
		})
	}
}

func TestRecordTruncationAtEveryByte(t *testing.T) {
	encoded, err := encodeRecord(kindSet, []byte("key"), []byte("value"))
	if err != nil {
		t.Fatal(err)
	}
	for length := 1; length < len(encoded); length++ {
		_, _, err := readRecordAt(bytes.NewReader(encoded[:length]), 0, int64(length))
		if !errors.Is(err, ErrTruncated) {
			t.Errorf("length %d: got %v, want truncation", length, err)
		}
	}
	// A stale upper bound must also detect EOF from the underlying reader.
	for _, length := range []int{recordHeaderSize - 1, recordHeaderSize, len(encoded) - 1} {
		_, _, err := readRecordAt(bytes.NewReader(encoded[:length]), 0, int64(len(encoded)))
		if !errors.Is(err, ErrTruncated) {
			t.Errorf("stale bound length %d: got %v", length, err)
		}
	}
}

func TestRecordCorruptionAtEveryByte(t *testing.T) {
	encoded, err := encodeRecord(kindSet, []byte("key"), []byte("value"))
	if err != nil {
		t.Fatal(err)
	}
	for index := range encoded {
		broken := bytes.Clone(encoded)
		broken[index] ^= 1
		_, _, err := readRecordAt(bytes.NewReader(broken), 0, int64(len(broken)))
		if !errors.Is(err, ErrCorrupt) {
			t.Errorf("byte %d: got %v, want corruption", index, err)
		}
	}
}

func TestRecordRejectsInvalidMetadataBeforePayloadRead(t *testing.T) {
	encoded, err := encodeRecord(kindSet, []byte("k"), nil)
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name   string
		mutate func([]byte)
	}{
		{"empty key", func(b []byte) { binary.BigEndian.PutUint32(b[8:12], 0) }},
		{"large key", func(b []byte) { binary.BigEndian.PutUint32(b[8:12], MaxKeySize+1) }},
		{"large value", func(b []byte) { binary.BigEndian.PutUint32(b[12:16], math.MaxUint32) }},
		{"delete payload", func(b []byte) { b[4] = byte(kindDelete); binary.BigEndian.PutUint32(b[12:16], 1) }},
		{"unknown operation", func(b []byte) { b[4] = 0xff }},
		{"reserved flag", func(b []byte) { b[5] = 1 }},
		{"magic", func(b []byte) { b[0] = 0 }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			broken := bytes.Clone(encoded)
			tc.mutate(broken)
			binary.BigEndian.PutUint32(broken[16:20], crc32.Checksum(broken[:16], crcTable))
			r := &headerOnlyReader{data: broken}
			if _, _, err := readRecordAt(r, 0, math.MaxInt64); !errors.Is(err, ErrCorrupt) {
				t.Fatalf("got %v, want corruption", err)
			}
			if r.payloadRead {
				t.Fatal("invalid metadata caused a payload read")
			}
		})
	}
}

type headerOnlyReader struct {
	data        []byte
	payloadRead bool
}

func (r *headerOnlyReader) ReadAt(p []byte, offset int64) (int, error) {
	if offset != 0 || len(p) != recordHeaderSize {
		r.payloadRead = true
		return 0, errors.New("unexpected payload read")
	}
	return copy(p, r.data), nil
}

func TestRecordBoundsDoNotOverflow(t *testing.T) {
	encoded, err := encodeRecord(kindSet, []byte("k"), nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, bounds := range [][2]int64{{-1, 0}, {1, 0}, {0, -1}} {
		if _, _, err := readRecordAt(bytes.NewReader(encoded), bounds[0], bounds[1]); !errors.Is(err, ErrCorrupt) {
			t.Errorf("bounds %v: got %v", bounds, err)
		}
	}
	r := relocatedReader{data: encoded, base: math.MaxInt64 - int64(len(encoded))}
	if _, next, err := readRecordAt(r, r.base, math.MaxInt64); err != nil || next != math.MaxInt64 {
		t.Fatalf("valid upper-bound record: next=%d err=%v", next, err)
	}
	if _, _, err := readRecordAt(r, math.MaxInt64-1, math.MaxInt64); !errors.Is(err, ErrTruncated) {
		t.Errorf("near MaxInt64: got %v", err)
	}
}

type relocatedReader struct {
	data []byte
	base int64
}

func (r relocatedReader) ReadAt(p []byte, offset int64) (int, error) {
	return bytes.NewReader(r.data).ReadAt(p, offset-r.base)
}

func TestRecordPreservesIOErrors(t *testing.T) {
	encoded, err := encodeRecord(kindSet, []byte("k"), []byte("v"))
	if err != nil {
		t.Fatal(err)
	}
	injected := errors.New("injected read failure")
	for _, at := range []int64{0, recordHeaderSize} {
		r := failingCodecReader{data: encoded, at: at, err: injected}
		_, _, err := readRecordAt(r, 0, int64(len(encoded)))
		if !errors.Is(err, injected) || errors.Is(err, ErrTruncated) {
			t.Errorf("failure at %d: got %v", at, err)
		}
	}
	r := failingCodecReader{data: encoded, at: 0}
	if _, _, err := readRecordAt(r, 0, int64(len(encoded))); !errors.Is(err, io.ErrNoProgress) || errors.Is(err, ErrTruncated) {
		t.Errorf("short nil read: got %v", err)
	}
}

type failingCodecReader struct {
	data []byte
	at   int64
	err  error
}

func (r failingCodecReader) ReadAt(p []byte, offset int64) (int, error) {
	if offset == r.at {
		return 0, r.err
	}
	return bytes.NewReader(r.data).ReadAt(p, offset)
}

func FuzzDecodeRecord(f *testing.F) {
	set, _ := encodeRecord(kindSet, []byte{0, 0xff}, []byte("value"))
	del, _ := encodeRecord(kindDelete, []byte("deleted"), nil)
	f.Add(set)
	f.Add(del)
	f.Add([]byte("not a record"))
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, data []byte) {
		decoded, next, err := readRecordAt(bytes.NewReader(data), 0, int64(len(data)))
		if err != nil {
			return
		}
		if next <= 0 || next > int64(len(data)) {
			t.Fatalf("invalid next offset: %d", next)
		}
		if len(decoded.key) == 0 || len(decoded.key) > MaxKeySize || len(decoded.value) > MaxValueSize {
			t.Fatal("decoded lengths outside format bounds")
		}
		encoded, err := encodeRecord(decoded.kind, decoded.key, decoded.value)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(encoded, data[:int(next)]) {
			t.Fatal("successful decode did not preserve canonical record bytes")
		}
	})
}
