package engine

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

func recoverIndex(file dataFile) (map[string]indexEntry, int64, error) {
	index := make(map[string]indexEntry)
	end, err := scanLog(file, func(rec record, offset, next int64) {
		key := string(rec.key)
		switch rec.kind {
		case kindSet:
			index[key] = indexEntry{offset: offset, size: next - offset}
		case kindDelete:
			delete(index, key)
		}
	})
	return index, end, err
}

// On failure, the returned offset is the last complete record boundary.
func scanLog(file dataFile, visit func(record, int64, int64)) (int64, error) {
	info, err := file.Stat()
	if err != nil {
		return 0, fmt.Errorf("stat data file: %w", err)
	}
	if !info.Mode().IsRegular() {
		return 0, fmt.Errorf("data file must be a regular file")
	}
	header := make([]byte, fileHeaderSize)
	if err := readExactAt(file, header, 0); err != nil {
		return 0, fmt.Errorf("read file header: %w", err)
	}
	if err := validateFileHeader(header); err != nil {
		return 0, fmt.Errorf("validate file header: %w", err)
	}
	end := info.Size()
	offset := int64(fileHeaderSize)
	for offset < end {
		rec, next, err := readRecordAt(file, offset, end)
		if err != nil {
			return offset, fmt.Errorf("recover record at offset %d: %w", offset, err)
		}
		if visit != nil {
			visit(rec, offset, next)
		}
		offset = next
	}
	return offset, nil
}

// RepairTail salvages an incomplete final record; it refuses corruption.
// The byte count reports truncation even if the following Sync fails.
func RepairTail(dir string) (discarded int64, err error) {
	directory, lock, err := lockDirectory(dir)
	if err != nil {
		return 0, err
	}
	defer directory.Close()
	defer func() { err = errors.Join(err, unlockDirectory(lock)) }()
	file, err := os.OpenFile(filepath.Join(dir, dataFileName), os.O_RDWR|os.O_APPEND, 0)
	if err != nil {
		return 0, fmt.Errorf("open data file for repair: %w", err)
	}
	defer func() { err = errors.Join(err, file.Close()) }()
	return repairFileTail(file)
}

func repairFileTail(file dataFile) (int64, error) {
	boundary, err := scanLog(file, nil)
	if err == nil {
		return 0, nil
	}
	if !errors.Is(err, ErrTruncated) || boundary < fileHeaderSize {
		return 0, err
	}
	info, statErr := file.Stat()
	if statErr != nil {
		return 0, fmt.Errorf("stat incomplete data file: %w", statErr)
	}
	discarded := info.Size() - boundary
	if err := file.Truncate(boundary); err != nil {
		return 0, fmt.Errorf("truncate incomplete tail: %w", err)
	}
	if err := file.Sync(); err != nil {
		return discarded, fmt.Errorf("sync repaired file (durability uncertain): %w", err)
	}
	return discarded, nil
}
