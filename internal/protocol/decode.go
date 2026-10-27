package protocol

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
)

func decodeRequest(frame []byte) (Request, error) {
	fields, err := readObject(frame, "op", "key", "value")
	if err != nil {
		return Request{}, err
	}
	var request Request
	if raw, ok := fields["op"]; ok {
		if err := decodeString(raw, &request.Op); err != nil {
			return Request{}, err
		}
	}
	if raw, ok := fields["key"]; ok {
		if err := decodeBytes(raw, &request.Key); err != nil {
			return Request{}, err
		}
	}
	if raw, ok := fields["value"]; ok {
		if err := decodeBytes(raw, &request.Value); err != nil {
			return Request{}, err
		}
	}
	return request, nil
}

func decodeResponse(frame []byte) (Response, error) {
	fields, err := readObject(frame, "ok", "value", "exists", "error")
	if err != nil {
		return Response{}, err
	}
	var response Response
	rawOK, ok := fields["ok"]
	if !ok || (!bytes.Equal(rawOK, []byte("true")) && !bytes.Equal(rawOK, []byte("false"))) {
		return Response{}, fmt.Errorf("%w: ok must be a boolean", ErrInvalidFrame)
	}
	response.OK = bytes.Equal(rawOK, []byte("true"))
	if raw, ok := fields["value"]; ok && !bytes.Equal(raw, []byte("null")) {
		var value []byte
		if err := decodeBytes(raw, &value); err != nil {
			return Response{}, err
		}
		response.Value = &value
	}
	if raw, ok := fields["exists"]; ok && !bytes.Equal(raw, []byte("null")) {
		if !bytes.Equal(raw, []byte("true")) && !bytes.Equal(raw, []byte("false")) {
			return Response{}, fmt.Errorf("%w: exists must be a boolean", ErrInvalidFrame)
		}
		exists := bytes.Equal(raw, []byte("true"))
		response.Exists = &exists
	}
	if raw, ok := fields["error"]; ok && !bytes.Equal(raw, []byte("null")) {
		failure, err := decodeError(raw)
		if err != nil {
			return Response{}, err
		}
		response.Error = &failure
	}
	if err := validateResponse(response); err != nil {
		return Response{}, err
	}
	return response, nil
}

func decodeError(raw []byte) (Error, error) {
	fields, err := readObject(raw, "code", "message", "retry_after_ms")
	if err != nil {
		return Error{}, err
	}
	var failure Error
	for name, target := range map[string]*string{"code": &failure.Code, "message": &failure.Message} {
		if value, ok := fields[name]; ok {
			if err := decodeString(value, target); err != nil {
				return Error{}, err
			}
		}
	}
	if value, ok := fields["retry_after_ms"]; ok {
		if bytes.Equal(value, []byte("null")) || json.Unmarshal(value, &failure.RetryAfterMS) != nil {
			return Error{}, fmt.Errorf("%w: retry_after_ms must be an integer", ErrInvalidFrame)
		}
	}
	return failure, nil
}

func validateResponse(response Response) error {
	if response.OK {
		if response.Error != nil || (response.Value != nil && response.Exists != nil) {
			return fmt.Errorf("%w: inconsistent success response", ErrInvalidFrame)
		}
	} else if response.Error == nil || response.Error.Code == "" || response.Value != nil || response.Exists != nil {
		return fmt.Errorf("%w: inconsistent error response", ErrInvalidFrame)
	}
	if response.Error != nil && response.Error.RetryAfterMS < 0 {
		return fmt.Errorf("%w: retry_after_ms cannot be negative", ErrInvalidFrame)
	}
	return nil
}

func readObject(frame []byte, names ...string) (map[string]json.RawMessage, error) {
	decoder := json.NewDecoder(bytes.NewReader(frame))
	token, err := decoder.Token()
	if err != nil || token != json.Delim('{') {
		return nil, fmt.Errorf("%w: expected JSON object", ErrInvalidFrame)
	}
	fields := make(map[string]json.RawMessage, len(names))
	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return nil, fmt.Errorf("%w: %v", ErrInvalidFrame, err)
		}
		name, ok := token.(string)
		if !ok {
			return nil, fmt.Errorf("%w: invalid field name", ErrInvalidFrame)
		}
		known := false
		for _, allowed := range names {
			known = known || name == allowed
		}
		if !known {
			return nil, fmt.Errorf("%w: unknown field %q", ErrInvalidFrame, name)
		}
		if _, duplicate := fields[name]; duplicate {
			return nil, fmt.Errorf("%w: duplicate field %q", ErrInvalidFrame, name)
		}
		var value json.RawMessage
		if err := decoder.Decode(&value); err != nil {
			return nil, fmt.Errorf("%w: %v", ErrInvalidFrame, err)
		}
		fields[name] = value
	}
	if _, err := decoder.Token(); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidFrame, err)
	}
	var extra json.RawMessage
	if err := decoder.Decode(&extra); err != io.EOF {
		return nil, fmt.Errorf("%w: trailing JSON", ErrInvalidFrame)
	}
	return fields, nil
}

func decodeString(raw []byte, target *string) error {
	if len(raw) == 0 || raw[0] != '"' || json.Unmarshal(raw, target) != nil {
		return fmt.Errorf("%w: expected string", ErrInvalidFrame)
	}
	return nil
}

func decodeBytes(raw []byte, target *[]byte) error {
	if bytes.Equal(raw, []byte("null")) {
		*target = nil
		return nil
	}
	if len(raw) == 0 || raw[0] != '"' || json.Unmarshal(raw, target) != nil {
		return fmt.Errorf("%w: expected base64 string", ErrInvalidFrame)
	}
	return nil
}
