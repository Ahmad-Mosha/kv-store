// Package engine implements an append-only persistent key-value store.
package engine

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

// DB is safe for concurrent use. Do not copy it or mutate inputs during calls.
type DB struct {
	mu     sync.RWMutex
	dir    string
	file   dataFile
	lock   *os.File
	index  map[string]indexEntry
	offset int64
	closed bool
	// Read failures must be published before releasing mu.RLock.
	faultMu sync.Mutex
	fault   error
}

// Open validates or creates data.log in an existing directory. It never repairs data.
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
	dir, err = filepath.Abs(dir)
	if err != nil {
		return nil, fmt.Errorf("resolve database directory: %w", err)
	}
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
	lockInfo, err := lock.Stat()
	if err != nil {
		return nil, fmt.Errorf("read initialization state: %w", err)
	}

	path := filepath.Join(dir, dataFileName)
	file, err = openData(path, os.O_RDWR|os.O_APPEND, 0600)
	created := false
	if errors.Is(err, os.ErrNotExist) {
		artifacts, scanErr := hasCompactionArtifacts(dir)
		if scanErr != nil {
			return nil, scanErr
		}
		if lockInfo.Size() != 0 || artifacts {
			return nil, fmt.Errorf("%w: %s missing from an initialized or interrupted database", ErrCorrupt, dataFileName)
		}
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
	// Recovered records may have been appended without completing Sync.
	if err = file.Sync(); err != nil {
		return nil, fmt.Errorf("sync opened data file: %w", err)
	}
	if err = syncDir(directory); err != nil {
		return nil, fmt.Errorf("sync database directory: %w", err)
	}
	if err = markInitialized(lock); err != nil {
		return nil, err
	}
	return &DB{dir: dir, file: file, lock: lock, index: index, offset: end}, nil
}

// Set synchronizes a value before publishing it. An error does not imply rollback.
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

// Get returns owned bytes and validates the stored record.
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
	rec, err := db.readEntry(string(key), entry)
	if err != nil {
		return nil, db.markFault(err)
	}
	return rec.value, nil
}

func (db *DB) readEntry(key string, entry indexEntry) (record, error) {
	rec, next, err := readRecordAt(db.file, entry.offset, db.offset)
	if err != nil {
		return record{}, fmt.Errorf("read key at offset %d: %w", entry.offset, err)
	}
	if rec.kind != kindSet || next-entry.offset != entry.size || !bytes.Equal(rec.key, []byte(key)) {
		return record{}, fmt.Errorf("%w: index/record mismatch at offset %d", ErrCorrupt, entry.offset)
	}
	return rec, nil
}

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

// Delete is a no-op for a missing key.
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

func (db *DB) appendRecord(encoded []byte) error {
	n, err := db.file.Write(encoded)
	if err == nil && n != len(encoded) {
		err = io.ErrShortWrite
	}
	if err == nil {
		err = db.file.Sync()
	}
	if err != nil {
		// Later appends could turn an incomplete tail into interior corruption.
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

// Lock order: mu (read or write) before faultMu.
func (db *DB) markFault(err error) error {
	db.faultMu.Lock()
	defer db.faultMu.Unlock()
	if db.fault == nil {
		db.fault = err
	}
	return fmt.Errorf("%w: %w", ErrFaulted, err)
}

// Close waits for active calls and releases ownership, even after an I/O failure.
func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return nil
	}
	db.closed = true
	return errors.Join(db.file.Close(), unlockDirectory(db.lock))
}
