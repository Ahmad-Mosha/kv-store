package engine

import "errors"

var (
	ErrNotFound           = errors.New("diskkv: key not found")
	ErrClosed             = errors.New("diskkv: database closed")
	ErrFaulted            = errors.New("diskkv: database faulted; close and reopen")
	ErrLocked             = errors.New("diskkv: database already open")
	ErrCorrupt            = errors.New("diskkv: corrupt data")
	ErrTruncated          = errors.New("diskkv: incomplete data at end of file")
	ErrInvalidKey         = errors.New("diskkv: key must contain 1 to 4096 bytes")
	ErrValueTooLarge      = errors.New("diskkv: value exceeds 1048576 bytes")
	ErrUnsupportedVersion = errors.New("diskkv: unsupported file format version")
)
