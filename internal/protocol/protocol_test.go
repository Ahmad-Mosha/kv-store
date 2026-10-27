package protocol

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"
	"testing/iotest"
)

func TestRequestRoundTrip(t *testing.T) {
	for _, request := range []Request{
		{Op: "SET", Key: []byte{0, 0xff, '\n'}, Value: []byte{0xff, 0, '\r'}},
		{Op: "SET", Key: []byte("empty"), Value: []byte{}},
		{Op: "GET", Key: []byte("key")},
		{Op: "DELETE", Key: bytes.Repeat([]byte{'k'}, MaxKeyBytes)},
		{Op: "SET", Key: []byte("maximum"), Value: bytes.Repeat([]byte{'v'}, MaxValueBytes)},
	} {
		var wire bytes.Buffer
		if err := WriteRequest(&wire, request); err != nil {
			t.Fatal(err)
		}
		got, err := NewReader(&wire).ReadRequest()
		if err != nil {
			t.Fatal(err)
		}
		if got.Op != request.Op || !bytes.Equal(got.Key, request.Key) || !bytes.Equal(got.Value, request.Value) {
			t.Fatalf("request changed in round trip: op=%q, key bytes=%d, value bytes=%d", got.Op, len(got.Key), len(got.Value))
		}
		if err := ValidateRequest(got); err != nil {
			t.Fatal(err)
		}
	}
}

func TestResponseRoundTrip(t *testing.T) {
	binaryValue, emptyValue := []byte{0xff, 0, '\n'}, []byte{}
	present, missing := true, false
	for _, response := range []Response{
		{OK: true},
		{OK: true, Value: &binaryValue},
		{OK: true, Value: &emptyValue},
		{OK: true, Exists: &present},
		{OK: true, Exists: &missing},
		{Error: &Error{Code: CodeRateLimited, Message: "try later", RetryAfterMS: 250}},
	} {
		var wire bytes.Buffer
		if err := WriteResponse(&wire, response); err != nil {
			t.Fatal(err)
		}
		if response.Value != nil && len(*response.Value) == 0 && !bytes.Contains(wire.Bytes(), []byte(`"value":""`)) {
			t.Fatalf("empty bytes must be encoded as an empty base64 string: %s", wire.Bytes())
		}
		got, err := NewReader(&wire).ReadResponse()
		if err != nil {
			t.Fatal(err)
		}
		if got.OK != response.OK || (got.Value == nil) != (response.Value == nil) || (got.Exists == nil) != (response.Exists == nil) || (got.Error == nil) != (response.Error == nil) {
			t.Fatalf("response shape changed: got %#v want %#v", got, response)
		}
		if got.Value != nil && !bytes.Equal(*got.Value, *response.Value) {
			t.Fatal("value changed")
		}
		if got.Exists != nil && *got.Exists != *response.Exists {
			t.Fatal("exists changed")
		}
		if got.Error != nil && *got.Error != *response.Error {
			t.Fatalf("error changed: got %+v want %+v", got.Error, response.Error)
		}
	}
}

func TestFragmentedAndCoalescedRequests(t *testing.T) {
	wire := "{\"op\":\"GET\",\"key\":\"YQ==\"}\n{\"op\":\"EXISTS\",\"key\":\"Yg==\"}\r\n"
	for _, input := range []io.Reader{strings.NewReader(wire), iotest.OneByteReader(strings.NewReader(wire))} {
		reader := NewReader(input)
		for i, op := range []string{"GET", "EXISTS"} {
			request, err := reader.ReadRequest()
			if err != nil || request.Op != op || !bytes.Equal(request.Key, []byte{byte('a' + i)}) {
				t.Fatalf("frame %d: request=%+v err=%v", i, request, err)
			}
		}
		if _, err := reader.ReadRequest(); !errors.Is(err, io.EOF) {
			t.Fatalf("clean EOF = %v", err)
		}
	}
}

func TestRequestShapeValidationIsSeparateFromSemantics(t *testing.T) {
	for _, frame := range []string{`{}`, `{"op":"unknown","key":"YQ=="}`, `{"op":"GET","key":null}`, `{"op":"SET","value":"YQ=="}`} {
		request, err := NewReader(strings.NewReader(frame + "\n")).ReadRequest()
		if err != nil {
			t.Fatalf("semantic-invalid request rejected by shape parser: %s: %v", frame, err)
		}
		if err := ValidateRequest(request); err == nil {
			t.Fatalf("semantic-invalid request accepted: %s", frame)
		}
	}
	valid := Request{Op: "GET", Key: []byte("k")}
	cases := []Request{
		{Op: "get", Key: valid.Key},
		{Op: "INCR", Key: valid.Key},
		{Op: "GET"},
		{Op: "GET", Key: make([]byte, MaxKeyBytes+1)},
		{Op: "SET", Key: valid.Key, Value: make([]byte, MaxValueBytes+1)},
		{Op: "GET", Key: valid.Key, Value: []byte("unexpected")},
		{Op: "DELETE", Key: valid.Key, Value: []byte("unexpected")},
		{Op: "EXISTS", Key: valid.Key, Value: []byte("unexpected")},
	}
	for _, request := range cases {
		if err := ValidateRequest(request); err == nil {
			t.Fatalf("invalid request accepted: %q", request.Op)
		}
	}
	for _, op := range []string{"SET", "GET", "DELETE", "EXISTS"} {
		if err := ValidateRequest(Request{Op: op, Key: []byte("k")}); err != nil {
			t.Fatalf("empty value should be accepted for %s: %v", op, err)
		}
	}
}

func TestMalformedRequests(t *testing.T) {
	cases := []string{
		``, ` `, `null`, `[]`, `"request"`, `1`, `{`, `{"op":`,
		`{"op":"GET","unknown":1}`, `{"Op":"GET"}`, `{"op":"GET","op":"SET"}`,
		`{"op":"GET","o\u0070":"SET"}`, `{"key":"YQ==","key":"Yg=="}`,
		`{"op":null}`, `{"op":1}`, `{"key":true}`, `{"key":[65]}`, `{"key":"%%%"}`,
		`{"value":{}}`, `{"value":"A"}`, `{"op":"GET"} {}`, `{}garbage`, `{"op":"GET",}`,
	}
	for _, frame := range cases {
		t.Run(frame, func(t *testing.T) {
			if _, err := NewReader(strings.NewReader(frame + "\n")).ReadRequest(); !errors.Is(err, ErrInvalidFrame) {
				t.Fatalf("got %v, want invalid frame", err)
			}
		})
	}
}

func TestMalformedResponses(t *testing.T) {
	cases := []string{
		`null`, `[]`, `{}`, `{"ok":null}`, `{"ok":"true"}`, `{"ok":true,"ok":true}`,
		`{"ok":true,"unexpected":0}`, `{"ok":true,"error":{"code":"INTERNAL_ERROR"}}`,
		`{"ok":false}`, `{"ok":false,"error":null}`, `{"ok":false,"error":{}}`,
		`{"ok":false,"error":{"code":""}}`, `{"ok":false,"error":{"code":null}}`,
		`{"ok":false,"error":{"code":"ERROR","code":"OTHER"}}`,
		`{"ok":false,"error":{"code":"ERROR","other":1}}`,
		`{"ok":false,"error":{"code":"ERROR","message":false}}`,
		`{"ok":false,"error":{"code":"ERROR","retry_after_ms":-1}}`,
		`{"ok":false,"error":{"code":"ERROR","retry_after_ms":0.5}}`,
		`{"ok":false,"error":{"code":"ERROR","retry_after_ms":null}}`,
		`{"ok":false,"error":{"code":"ERROR","retry_after_ms":9223372036854775808}}`,
		`{"ok":false,"value":"YQ==","error":{"code":"ERROR"}}`,
		`{"ok":true,"exists":1}`, `{"ok":true,"value":[65]}`, `{"ok":true,"value":"%"}`,
		`{"ok":true,"exists":true,"value":""}`, `{"ok":true} {"ok":true}`,
	}
	for _, frame := range cases {
		t.Run(frame, func(t *testing.T) {
			if _, err := NewReader(strings.NewReader(frame + "\n")).ReadResponse(); !errors.Is(err, ErrInvalidFrame) {
				t.Fatalf("got %v, want invalid frame", err)
			}
		})
	}
	response, err := NewReader(strings.NewReader("{\"ok\":true,\"value\":null,\"exists\":null,\"error\":null}\n")).ReadResponse()
	if err != nil || !response.OK || response.Value != nil || response.Exists != nil || response.Error != nil {
		t.Fatalf("optional nulls = %+v, %v", response, err)
	}
}

func TestFrameSizeBoundary(t *testing.T) {
	base := []byte(`{"op":"GET","key":"YQ=="}`)
	for _, extra := range []int{-1, 0, 1} {
		size := MaxFrameBytes + extra
		frame := append(bytes.Clone(base), bytes.Repeat([]byte{' '}, size-len(base))...)
		reader := NewReader(bytes.NewReader(append(frame, '\n')))
		request, err := reader.ReadRequest()
		if extra > 0 {
			if !errors.Is(err, ErrFrameTooLarge) {
				t.Fatalf("size %d: got %v", size, err)
			}
		} else if err != nil || request.Op != "GET" {
			t.Fatalf("size %d: request=%+v err=%v", size, request, err)
		}
	}
	frame := append(bytes.Clone(base), bytes.Repeat([]byte{' '}, MaxFrameBytes-len(base))...)
	got, err := NewReader(bytes.NewReader(append(frame, '\n'))).readFrame()
	if err != nil || len(got) != MaxFrameBytes || cap(got) > MaxFrameBytes {
		t.Fatalf("bounded frame buffer: length=%d capacity=%d err=%v", len(got), cap(got), err)
	}
}

type endlessFrameReader struct{ consumed int }

func (r *endlessFrameReader) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = ' '
	}
	r.consumed += len(p)
	return len(p), nil
}

func TestOversizedFrameStopsReading(t *testing.T) {
	input := &endlessFrameReader{}
	if _, err := NewReader(input).ReadRequest(); !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("oversized frame = %v", err)
	}
	if input.consumed > MaxFrameBytes+4096 {
		t.Fatalf("read beyond bounded framing overhead: %d bytes", input.consumed)
	}
}

func TestEOFAndUnderlyingReadErrors(t *testing.T) {
	for _, frame := range []string{"{", `{"op":"GET"}`, " "} {
		if _, err := NewReader(strings.NewReader(frame)).ReadRequest(); !errors.Is(err, ErrInvalidFrame) {
			t.Fatalf("unterminated %q = %v", frame, err)
		}
	}
	if _, err := NewReader(strings.NewReader("")).ReadRequest(); !errors.Is(err, io.EOF) {
		t.Fatalf("empty stream = %v", err)
	}
	injected := errors.New("injected read error")
	for _, prefix := range []string{"", `{"op":`} {
		input := io.MultiReader(strings.NewReader(prefix), iotest.ErrReader(injected))
		if _, err := NewReader(input).ReadRequest(); !errors.Is(err, injected) || errors.Is(err, ErrInvalidFrame) {
			t.Fatalf("underlying read error = %v", err)
		}
	}
}

type faultyWriter struct {
	err error
}

func (w faultyWriter) Write(p []byte) (int, error) { return len(p) / 2, w.err }

func TestWriteFailures(t *testing.T) {
	injected := errors.New("injected write error")
	request := Request{Op: "GET", Key: []byte("k")}
	for _, encode := range []func(io.Writer) error{
		func(w io.Writer) error { return WriteRequest(w, request) },
		func(w io.Writer) error { return WriteResponse(w, Response{OK: true}) },
	} {
		if err := encode(faultyWriter{err: injected}); !errors.Is(err, injected) {
			t.Fatalf("write error = %v", err)
		}
		if err := encode(faultyWriter{}); !errors.Is(err, io.ErrShortWrite) {
			t.Fatalf("short write = %v", err)
		}
	}
	var wire bytes.Buffer
	if err := WriteRequest(&wire, Request{Op: strings.Repeat("x", MaxFrameBytes)}); !errors.Is(err, ErrFrameTooLarge) || wire.Len() != 0 {
		t.Fatalf("oversize write = %v, emitted %d bytes", err, wire.Len())
	}
	if err := WriteResponse(&wire, Response{}); !errors.Is(err, ErrInvalidFrame) || wire.Len() != 0 {
		t.Fatalf("invalid response write = %v, emitted %d bytes", err, wire.Len())
	}
}

func TestResponseEncodingCanExceedFrameLimit(t *testing.T) {
	message := strings.Repeat("<", MaxFrameBytes/6)
	input := `{"ok":false,"error":{"code":"ERROR","message":"` + message + `"}}` + "\n"
	response, err := NewReader(strings.NewReader(input)).ReadResponse()
	if err != nil {
		t.Fatal(err)
	}
	var wire bytes.Buffer
	if err := WriteResponse(&wire, response); !errors.Is(err, ErrFrameTooLarge) || wire.Len() != 0 {
		t.Fatalf("escaped response size = %v, emitted %d bytes", err, wire.Len())
	}
}

func FuzzReadFrames(f *testing.F) {
	for _, seed := range []string{
		"{\"op\":\"SET\",\"key\":\"AP8=\",\"value\":\"\"}\n",
		"{\"ok\":true,\"value\":\"\"}\n",
		"{\"ok\":false,\"error\":{\"code\":\"NOT_FOUND\"}}\n",
		"null\n", "{}{}\n", "", "partial",
	} {
		f.Add([]byte(seed))
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		request, err := NewReader(bytes.NewReader(data)).ReadRequest()
		if err == nil {
			var wire bytes.Buffer
			if err := WriteRequest(&wire, request); err != nil {
				if !errors.Is(err, ErrFrameTooLarge) {
					t.Fatal(err)
				}
			} else if _, err := NewReader(&wire).ReadRequest(); err != nil {
				t.Fatalf("request round trip failed: %v", err)
			}
		}
		response, err := NewReader(bytes.NewReader(data)).ReadResponse()
		if err == nil {
			var wire bytes.Buffer
			if err := WriteResponse(&wire, response); err != nil {
				if !errors.Is(err, ErrFrameTooLarge) {
					t.Fatal(err)
				}
				return
			}
			decoded, err := NewReader(&wire).ReadResponse()
			if err != nil {
				t.Fatalf("response round trip failed: %v", err)
			}
			before, _ := json.Marshal(response)
			after, _ := json.Marshal(decoded)
			if !bytes.Equal(before, after) {
				t.Fatal("response shape changed")
			}
		}
	})
}
