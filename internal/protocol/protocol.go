package protocol

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

const (
	MaxFrameBytes = 2 << 20
	MaxKeyBytes   = 4 << 10
	MaxValueBytes = 1 << 20

	CodeBadRequest    = "BAD_REQUEST"
	CodeFrameTooLarge = "FRAME_TOO_LARGE"
	CodeNotFound      = "NOT_FOUND"
	CodeRateLimited   = "RATE_LIMITED"
	CodeUnavailable   = "UNAVAILABLE"
	CodeInternal      = "INTERNAL_ERROR"
)

var (
	ErrFrameTooLarge = errors.New("protocol: frame too large")
	ErrInvalidFrame  = errors.New("protocol: invalid frame")
)

type Request struct {
	Op    string `json:"op"`
	Key   []byte `json:"key"`
	Value []byte `json:"value,omitempty"`
}

type Response struct {
	OK     bool    `json:"ok"`
	Value  *[]byte `json:"value,omitempty"`
	Exists *bool   `json:"exists,omitempty"`
	Error  *Error  `json:"error,omitempty"`
}

type Error struct {
	Code         string `json:"code"`
	Message      string `json:"message"`
	RetryAfterMS int64  `json:"retry_after_ms,omitempty"`
}

type Reader struct {
	reader *bufio.Reader
}

func NewReader(reader io.Reader) *Reader {
	return &Reader{reader: bufio.NewReaderSize(reader, 4096)}
}

func (r *Reader) ReadRequest() (Request, error) {
	frame, err := r.readFrame()
	if err != nil {
		return Request{}, err
	}
	return decodeRequest(frame)
}

func (r *Reader) ReadResponse() (Response, error) {
	frame, err := r.readFrame()
	if err != nil {
		return Response{}, err
	}
	return decodeResponse(frame)
}

func ValidateRequest(request Request) error {
	switch request.Op {
	case "SET", "GET", "DELETE", "EXISTS":
	default:
		return fmt.Errorf("%w: unsupported operation", ErrInvalidFrame)
	}
	if len(request.Key) == 0 || len(request.Key) > MaxKeyBytes {
		return fmt.Errorf("%w: key must contain 1 to %d bytes", ErrInvalidFrame, MaxKeyBytes)
	}
	if len(request.Value) > MaxValueBytes {
		return fmt.Errorf("%w: value exceeds %d bytes", ErrInvalidFrame, MaxValueBytes)
	}
	if request.Op != "SET" && len(request.Value) != 0 {
		return fmt.Errorf("%w: value is only valid for SET", ErrInvalidFrame)
	}
	return nil
}

func WriteRequest(writer io.Writer, request Request) error {
	return writeFrame(writer, request)
}

func WriteResponse(writer io.Writer, response Response) error {
	if err := validateResponse(response); err != nil {
		return err
	}
	return writeFrame(writer, response)
}

func (r *Reader) readFrame() ([]byte, error) {
	var frame []byte
	for {
		part, err := r.reader.ReadSlice('\n')
		if err == nil {
			part = part[:len(part)-1]
		}
		if len(part) > MaxFrameBytes-len(frame) {
			return nil, ErrFrameTooLarge
		}
		needed := len(frame) + len(part)
		if needed > cap(frame) {
			capacity := max(4096, cap(frame)*2, needed)
			capacity = min(capacity, MaxFrameBytes)
			grown := make([]byte, len(frame), capacity)
			copy(grown, frame)
			frame = grown
		}
		frame = append(frame, part...)
		switch err {
		case nil:
			return frame, nil
		case bufio.ErrBufferFull:
			continue
		case io.EOF:
			if len(frame) == 0 {
				return nil, io.EOF
			}
			return nil, fmt.Errorf("%w: missing newline", ErrInvalidFrame)
		default:
			return nil, fmt.Errorf("read frame: %w", err)
		}
	}
}

func writeFrame(writer io.Writer, value any) error {
	frame, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("encode frame: %w", err)
	}
	if len(frame) > MaxFrameBytes {
		return ErrFrameTooLarge
	}
	frame = append(frame, '\n')
	n, err := writer.Write(frame)
	if err != nil {
		return err
	}
	if n != len(frame) {
		return io.ErrShortWrite
	}
	return nil
}
