// Package diskkv implements a single-process, persistent key-value store.
// Successful mutations are synchronized to the data file before becoming visible.
package diskkv

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

const dataFileName = "data.log"

// dataFile is the narrow I/O boundary used to test short writes and sync failures.
type dataFile interface {
	io.ReaderAt
	io.Writer
	Sync() error
	Stat() (os.FileInfo, error)
	Truncate(int64) error
	Close() error
}

type indexEntry struct {
	offset int64
	size   int64
}

// DB owns the log and an exclusive advisory directory lock. It is safe for
// concurrent use. Callers must not modify input slices while a method uses them.
// A DB must not be copied after its first use.
type DB struct {
	mu     sync.RWMutex
	file   dataFile
	lock   *os.File
	index  map[string]indexEntry
	offset int64
	closed bool
	// A reader must be able to publish a fault before dropping its read lock,
	// so no writer can acknowledge data behind known storage corruption.
	faultMu sync.Mutex
	fault   error
}

// Open opens a database in an existing directory, creating data.log if absent.
// It validates the entire log before exposing any data and never repairs a
// corrupt or incomplete existing file. Use RepairTail explicitly for an
// incomplete final record. Only Linux and macOS local filesystems are supported.
func Open(dir string) (*DB, error) {
	return openWith(dir, func(path string, flags int, mode os.FileMode) (dataFile, error) {
		file, err := os.OpenFile(path, flags, mode)
		if err != nil {
			return nil, err
		}
		return file, nil
	}, func(dir *os.File) error { return dir.Sync() })
}

func openWith(dir string, openData func(string, int, os.FileMode) (dataFile, error), syncDir func(*os.File) error) (_ *DB, err error) {
	directory, lock, err := lockDirectory(dir)
	if err != nil {
		return nil, err
	}
	defer directory.Close()
	var file dataFile
	defer func() {
		if err != nil {
			if file != nil {
				err = errors.Join(err, file.Close())
			}
			err = errors.Join(err, unlockDirectory(lock))
		}
	}()

	path := filepath.Join(dir, dataFileName)
	file, err = openData(path, os.O_RDWR|os.O_APPEND, 0600)
	created := false
	if errors.Is(err, os.ErrNotExist) {
		file, err = openData(path, os.O_RDWR|os.O_APPEND|os.O_CREATE|os.O_EXCL, 0600)
		created = err == nil
	}
	if err != nil {
		return nil, fmt.Errorf("open data file: %w", err)
	}
	if created {
		header := encodeFileHeader()
		n, writeErr := file.Write(header)
		if writeErr != nil {
			return nil, fmt.Errorf("initialize data file: %w", writeErr)
		}
		if n != len(header) {
			return nil, fmt.Errorf("initialize data file: %w", io.ErrShortWrite)
		}
	}

	index, end, err := recoverIndex(file)
	if err != nil {
		return nil, err
	}
	// Also sync recovered records: a previous process may have appended a full
	// record without acknowledging it or completing its own sync.
	if err = file.Sync(); err != nil {
		return nil, fmt.Errorf("sync opened data file: %w", err)
	}
	if err = syncDir(directory); err != nil {
		return nil, fmt.Errorf("sync database directory: %w", err)
	}
	return &DB{file: file, lock: lock, index: index, offset: end}, nil
}

// Set appends a replacement value. It publishes the new index entry only after
// the complete record and its Sync succeed. A failed mutation may nevertheless
// be recovered on reopen; an error is not a guarantee that nothing was written.
func (db *DB) Set(key, value []byte) error {
	encoded, err := encodeRecord(kindSet, key, value)
	if err != nil {
		return err
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	if err := db.checkState(); err != nil {
		return err
	}
	entry := indexEntry{offset: db.offset, size: int64(len(encoded))}
	if err := db.appendRecord(encoded); err != nil {
		return err
	}
	db.index[string(key)] = entry
	return nil
}

// Get returns an independent copy of a value, or ErrNotFound. It verifies the
// record checksum and identity on every read, not only during startup recovery.
func (db *DB) Get(key []byte) ([]byte, error) {
	if err := validateKey(key); err != nil {
		return nil, err
	}
	db.mu.RLock()
	defer db.mu.RUnlock()
	if err := db.checkState(); err != nil {
		return nil, err
	}
	entry, ok := db.index[string(key)]
	if !ok {
		return nil, ErrNotFound
	}
	rec, next, err := readRecordAt(db.file, entry.offset, db.offset)
	if err != nil {
		return nil, db.markFault(fmt.Errorf("read key at offset %d: %w", entry.offset, err))
	}
	if rec.kind != kindSet || next-entry.offset != entry.size || !bytes.Equal(rec.key, key) {
		return nil, db.markFault(fmt.Errorf("%w: index/record mismatch at offset %d", ErrCorrupt, entry.offset))
	}
	return rec.value, nil
}

// Exists reports index membership without reading the stored value.
func (db *DB) Exists(key []byte) (bool, error) {
	if err := validateKey(key); err != nil {
		return false, err
	}
	db.mu.RLock()
	defer db.mu.RUnlock()
	if err := db.checkState(); err != nil {
		return false, err
	}
	_, ok := db.index[string(key)]
	return ok, nil
}

// Delete appends a tombstone for an existing key. A missing key is a no-op.
func (db *DB) Delete(key []byte) error {
	encoded, err := encodeRecord(kindDelete, key, nil)
	if err != nil {
		return err
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	if err := db.checkState(); err != nil {
		return err
	}
	if _, ok := db.index[string(key)]; !ok {
		return nil
	}
	if err := db.appendRecord(encoded); err != nil {
		return err
	}
	delete(db.index, string(key))
	return nil
}

func validateKey(key []byte) error {
	if len(key) == 0 || len(key) > MaxKeySize {
		return ErrInvalidKey
	}
	return nil
}

// appendRecord is called only under the exclusive lock. Never append another
// record behind a failed write: that could turn a repairable tail into interior
// corruption, or create a second history behind an unpublished durable record.
func (db *DB) appendRecord(encoded []byte) error {
	n, err := db.file.Write(encoded)
	if err == nil && n != len(encoded) {
		err = io.ErrShortWrite
	}
	if err == nil {
		err = db.file.Sync()
	}
	if err != nil {
		return db.markFault(fmt.Errorf("mutation outcome uncertain: %w", err))
	}
	db.offset += int64(len(encoded))
	return nil
}

func (db *DB) checkState() error {
	if db.closed {
		return ErrClosed
	}
	db.faultMu.Lock()
	defer db.faultMu.Unlock()
	if db.fault != nil {
		return fmt.Errorf("%w: %w", ErrFaulted, db.fault)
	}
	return nil
}

// The caller holds mu (read or write). Lock ordering is always mu -> faultMu.
func (db *DB) markFault(err error) error {
	db.faultMu.Lock()
	defer db.faultMu.Unlock()
	if db.fault == nil {
		db.fault = err
	}
	return fmt.Errorf("%w: %w", ErrFaulted, err)
}

// Close waits for active operations, closes the log, and releases its lock even
// if closing the log fails. Repeated calls are harmless. It does not turn an
// earlier failed mutation into a successful one.
func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return nil
	}
	db.closed = true
	return errors.Join(db.file.Close(), unlockDirectory(db.lock))
}
