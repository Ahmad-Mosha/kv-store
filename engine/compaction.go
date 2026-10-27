package engine

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

const compactionPrefix = ".compact-"

type compactionFile interface {
	dataFile
	Name() string
}

type compactionOps struct {
	createTemp func(string, string) (compactionFile, error)
	rename     func(string, string) error
	remove     func(string) error
	syncDir    func(string) error
}

func defaultCompactionOps() compactionOps {
	return compactionOps{
		createTemp: func(dir, pattern string) (compactionFile, error) {
			file, err := os.CreateTemp(dir, pattern)
			if err != nil {
				return nil, err
			}
			return file, nil
		},
		rename:  os.Rename,
		remove:  os.Remove,
		syncDir: syncDirectory,
	}
}

// Compact pauses operations while replacing the log with its current live records.
func (db *DB) Compact() error {
	return db.compactWith(defaultCompactionOps())
}

func (db *DB) compactWith(ops compactionOps) (err error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	if err := db.checkState(); err != nil {
		return err
	}

	temp, err := ops.createTemp(db.dir, compactionPrefix+"*")
	if err != nil {
		return fmt.Errorf("create compaction file: %w", err)
	}
	published := false
	defer func() {
		if !published {
			err = errors.Join(err, temp.Close())
			if removeErr := ops.remove(temp.Name()); removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
				err = errors.Join(err, fmt.Errorf("remove compaction file: %w", removeErr))
			}
		}
	}()

	if err := writeCompactionRecord(temp, encodeFileHeader()); err != nil {
		return err
	}
	index := make(map[string]indexEntry, len(db.index))
	offset := int64(fileHeaderSize)
	for key, entry := range db.index {
		rec, err := db.readEntry(key, entry)
		if err != nil {
			return db.markFault(err)
		}
		encoded, err := encodeRecord(kindSet, rec.key, rec.value)
		if err != nil {
			return db.markFault(err)
		}
		if err := writeCompactionRecord(temp, encoded); err != nil {
			return err
		}
		index[key] = indexEntry{offset: offset, size: int64(len(encoded))}
		offset += int64(len(encoded))
	}
	if err := temp.Sync(); err != nil {
		return fmt.Errorf("sync compaction file: %w", err)
	}
	if err := ops.rename(temp.Name(), filepath.Join(db.dir, dataFileName)); err != nil {
		// A rename error can have an uncertain namespace outcome.
		return db.markFault(fmt.Errorf("replace data file: %w", err))
	}
	if err := ops.syncDir(db.dir); err != nil {
		return db.markFault(fmt.Errorf("sync compacted directory: %w", err))
	}

	old := db.file
	// ReadAt never moves the replacement's write cursor, which remains at EOF.
	db.file, db.index, db.offset = temp, index, offset
	published = true
	if err := old.Close(); err != nil {
		return fmt.Errorf("compaction committed; close previous data file: %w", err)
	}
	return nil
}

func writeCompactionRecord(file io.Writer, data []byte) error {
	n, err := file.Write(data)
	if err != nil {
		return fmt.Errorf("write compaction file: %w", err)
	}
	if n != len(data) {
		return fmt.Errorf("write compaction file: %w", io.ErrShortWrite)
	}
	return nil
}

func syncDirectory(path string) (err error) {
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, dir.Close()) }()
	return dir.Sync()
}
