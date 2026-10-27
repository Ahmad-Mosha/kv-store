package diskkv

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
)

type openingFaultFile struct {
	dataFile
	shortHeader bool
	syncErr     error
}

func (f *openingFaultFile) Write(p []byte) (int, error) {
	if f.shortHeader {
		return f.dataFile.Write(p[:len(p)/2])
	}
	return f.dataFile.Write(p)
}

func (f *openingFaultFile) Sync() error {
	if f.syncErr != nil {
		return f.syncErr
	}
	return f.dataFile.Sync()
}

func TestInitializationShortWriteAndSyncPreserveRecoverableState(t *testing.T) {
	injected := errors.New("injected sync failure")
	for _, stage := range []string{"short header", "file sync", "directory sync"} {
		t.Run(stage, func(t *testing.T) {
			dir := t.TempDir()
			openData := func(path string, flags int, mode os.FileMode) (dataFile, error) {
				file, err := os.OpenFile(path, flags, mode)
				if err != nil {
					return nil, err
				}
				wrapped := &openingFaultFile{dataFile: file, shortHeader: stage == "short header"}
				if stage == "file sync" {
					wrapped.syncErr = injected
				}
				return wrapped, nil
			}
			syncDir := func(file *os.File) error {
				if stage == "directory sync" {
					return injected
				}
				return file.Sync()
			}
			db, err := openWith(dir, openData, syncDir)
			want := injected
			if stage == "short header" {
				want = io.ErrShortWrite
			}
			if db != nil || !errors.Is(err, want) {
				t.Fatalf("Open = (%v, %v), want nil, %v", db, err, want)
			}
			before, err := os.ReadFile(filepath.Join(dir, dataFileName))
			if err != nil {
				t.Fatal(err)
			}
			reopened, err := Open(dir)
			if stage == "short header" {
				if !errors.Is(err, ErrTruncated) || errors.Is(err, ErrLocked) {
					t.Fatalf("partial initialization reopen: %v", err)
				}
				after, readErr := os.ReadFile(filepath.Join(dir, dataFileName))
				if readErr != nil || !bytes.Equal(before, after) {
					t.Fatalf("partial header was modified: %v", readErr)
				}
				if _, err := RepairTail(dir); !errors.Is(err, ErrTruncated) {
					t.Fatalf("file header must not be repaired: %v", err)
				}
			} else {
				if err != nil {
					t.Fatalf("complete initialization should be reopenable, lock released: %v", err)
				}
				if err := reopened.Close(); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

func TestDetectedReadCorruptionPreventsLaterWrites(t *testing.T) {
	dir := t.TempDir()
	db := openTestDB(t, dir)
	if err := db.Set([]byte("key"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, dataFileName)
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	content[len(content)-1] ^= 1
	file, err := os.OpenFile(path, os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteAt(content, 0); err != nil {
		file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Get([]byte("key")); !errors.Is(err, ErrCorrupt) || !errors.Is(err, ErrFaulted) {
		t.Fatalf("Get = %v, want corruption and faulted handle", err)
	}
	assertStateError(t, db, ErrFaulted)
	assertLogUnchanged(t, dir, content)
}

type repairFaultFile struct {
	dataFile
	readErr     error
	readOffset  int64
	truncateErr error
	syncErr     error
}

type concurrentReadFailureFile struct {
	dataFile
	entered chan<- struct{}
	release <-chan struct{}
	cause   error
}

func (f concurrentReadFailureFile) ReadAt([]byte, int64) (int, error) {
	f.entered <- struct{}{}
	<-f.release
	return 0, f.cause
}

func TestConcurrentReadFailuresPublishFaultBeforeUnlock(t *testing.T) {
	dir := t.TempDir()
	db := openTestDB(t, dir)
	if err := db.Set([]byte("key"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}, 2), make(chan struct{})
	injected := errors.New("injected concurrent read failure")
	db.file = concurrentReadFailureFile{dataFile: db.file, entered: entered, release: release, cause: injected}
	results := make(chan error, 2)
	for range 2 {
		go func() { _, err := db.Get([]byte("key")); results <- err }()
	}
	// Both readers hold RLock before either publishes its failure. Race detection
	// therefore exercises the fault state under concurrent shared locks.
	<-entered
	<-entered
	close(release)
	for range 2 {
		if err := <-results; !errors.Is(err, injected) || !errors.Is(err, ErrFaulted) {
			t.Errorf("Get = %v, want original error and ErrFaulted", err)
		}
	}
	assertStateError(t, db, ErrFaulted)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	assertValue(t, openTestDB(t, dir), []byte("key"), []byte("value"))
}

func (f *repairFaultFile) ReadAt(p []byte, offset int64) (int, error) {
	if f.readErr != nil && offset == f.readOffset {
		return 0, f.readErr
	}
	return f.dataFile.ReadAt(p, offset)
}

func (f *repairFaultFile) Truncate(size int64) error {
	if f.truncateErr != nil {
		return f.truncateErr
	}
	return f.dataFile.Truncate(size)
}

func (f *repairFaultFile) Sync() error {
	if f.syncErr != nil {
		return f.syncErr
	}
	return f.dataFile.Sync()
}

func TestRepairDoesNotTreatIOFailureAsTruncation(t *testing.T) {
	for _, stage := range []string{"header read", "record read", "truncate", "sync"} {
		t.Run(stage, func(t *testing.T) {
			dir := t.TempDir()
			rec, err := encodeRecord(kindSet, []byte("key"), []byte("value"))
			if err != nil {
				t.Fatal(err)
			}
			data := append(encodeFileHeader(), rec[:len(rec)-1]...)
			if stage == "record read" {
				data = append(encodeFileHeader(), rec...)
			}
			path := filepath.Join(dir, dataFileName)
			if err := os.WriteFile(path, data, 0600); err != nil {
				t.Fatal(err)
			}
			file, err := os.OpenFile(path, os.O_RDWR, 0)
			if err != nil {
				t.Fatal(err)
			}
			defer file.Close()
			injected := errors.New("injected repair I/O error")
			fault := &repairFaultFile{dataFile: file}
			switch stage {
			case "header read":
				fault.readErr = injected
			case "record read":
				fault.readErr, fault.readOffset = injected, fileHeaderSize
			case "truncate":
				fault.truncateErr = injected
			case "sync":
				fault.syncErr = injected
			}
			discarded, err := repairFileTail(fault)
			if !errors.Is(err, injected) || errors.Is(err, ErrTruncated) {
				t.Fatalf("repair error = %v", err)
			}
			after, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if stage == "sync" {
				if discarded != int64(len(data)-fileHeaderSize) || !bytes.Equal(after, encodeFileHeader()) {
					t.Fatal("sync failure must report the truncation that already occurred")
				}
			} else if discarded != 0 || !bytes.Equal(data, after) {
				t.Fatal("repair modified file after read/truncate failure")
			}
		})
	}
}
