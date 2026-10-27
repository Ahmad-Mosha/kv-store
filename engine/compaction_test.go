package engine

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"maps"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
)

func seedCompactionDB(t *testing.T, dir string) (*DB, map[string][]byte) {
	t.Helper()
	db := openTestDB(t, dir)
	for _, pair := range [][2]string{
		{"kept", "old"}, {"deleted", "must not return"},
		{"other", "another value"}, {"kept", "current"},
	} {
		if err := db.Set([]byte(pair[0]), []byte(pair[1])); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Delete([]byte("deleted")); err != nil {
		t.Fatal(err)
	}
	return db, map[string][]byte{"kept": []byte("current"), "other": []byte("another value")}
}

func assertCompactionModel(t *testing.T, db *DB, model map[string][]byte) {
	t.Helper()
	if len(db.index) != len(model) {
		t.Fatalf("index has %d keys; want %d", len(db.index), len(model))
	}
	for key, value := range model {
		assertValue(t, db, []byte(key), value)
	}
	if _, exists := model["deleted"]; !exists {
		assertMissing(t, db, []byte("deleted"))
	}
}

func compactedSize(model map[string][]byte) int64 {
	size := int64(fileHeaderSize)
	for key, value := range model {
		size += int64(recordHeaderSize + len(key) + len(value))
	}
	return size
}

func TestCompactionPreservesStateAndExactLiveSize(t *testing.T) {
	for _, scenario := range []string{"empty", "all-deleted", "update-heavy", "delete-heavy", "binary-and-maximum"} {
		t.Run(scenario, func(t *testing.T) {
			dir := t.TempDir()
			db := openTestDB(t, dir)
			model := make(map[string][]byte)
			set := func(key, value []byte) {
				t.Helper()
				if err := db.Set(key, value); err != nil {
					t.Fatal(err)
				}
				model[string(key)] = bytes.Clone(value)
			}
			remove := func(key string) {
				t.Helper()
				if err := db.Delete([]byte(key)); err != nil {
					t.Fatal(err)
				}
				delete(model, key)
			}
			switch scenario {
			case "all-deleted", "delete-heavy":
				for n := range 12 {
					set([]byte(fmt.Sprintf("key:%d", n)), []byte("obsolete"))
				}
				for n := range 12 {
					if scenario == "all-deleted" || n > 1 {
						remove(fmt.Sprintf("key:%d", n))
					}
				}
			case "update-heavy":
				for n := range 24 {
					set([]byte("key"), []byte(fmt.Sprintf("revision:%d", n)))
				}
			case "binary-and-maximum":
				set([]byte{0, 0xff, '\n'}, []byte{0xff, 0, '\r'})
				set(bytes.Repeat([]byte{0x80}, MaxKeySize), bytes.Repeat([]byte{0xff}, MaxValueSize))
				set([]byte("empty-value"), nil)
			}
			before := db.offset
			if err := db.Compact(); err != nil {
				t.Fatal(err)
			}
			assertCompactionModel(t, db, model)
			info, err := os.Stat(filepath.Join(dir, dataFileName))
			if err != nil {
				t.Fatal(err)
			}
			if info.Size() != compactedSize(model) || db.offset != info.Size() {
				t.Fatalf("compacted file/index size = %d/%d; want %d", info.Size(), db.offset, compactedSize(model))
			}
			if scenario != "empty" && scenario != "binary-and-maximum" && db.offset >= before {
				t.Fatalf("obsolete records were not reclaimed: before=%d after=%d", before, db.offset)
			}
			// Get uses positional reads; subsequent appends must remain at EOF.
			set([]byte("after-compact"), []byte("append after reads"))
			if err := db.Compact(); err != nil {
				t.Fatal(err)
			}
			remove("after-compact")
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			db = openTestDB(t, dir)
			assertCompactionModel(t, db, model)
			if artifacts, err := filepath.Glob(filepath.Join(dir, compactionPrefix+"*")); err != nil || len(artifacts) != 0 {
				t.Fatalf("successful compaction left temporary artifacts: %v, %v", artifacts, err)
			}
		})
	}
}

type compactionFaultFile struct {
	compactionFile
	writeAt  int
	short    bool
	writeErr error
	syncErr  error
	closeErr error
	writes   int
	onSync   func()
}

func (f *compactionFaultFile) Write(p []byte) (int, error) {
	f.writes++
	if f.writes == f.writeAt {
		if f.short {
			return f.compactionFile.Write(p[:len(p)/2])
		}
		return 0, f.writeErr
	}
	return f.compactionFile.Write(p)
}

func (f *compactionFaultFile) Sync() error {
	if f.syncErr != nil {
		return f.syncErr
	}
	if err := f.compactionFile.Sync(); err != nil {
		return err
	}
	if f.onSync != nil {
		f.onSync()
	}
	return nil
}

func (f *compactionFaultFile) Close() error {
	return errors.Join(f.compactionFile.Close(), f.closeErr)
}

func TestCompactionFailureStagesPreserveRecoverableHistory(t *testing.T) {
	injected := errors.New("injected compaction failure")
	for _, stage := range []string{
		"create", "header-write", "header-short-write", "live-write", "live-short-write", "temp-sync",
		"rename-before-effect", "rename-after-effect", "directory-sync",
	} {
		t.Run(stage, func(t *testing.T) {
			dir := t.TempDir()
			db, model := seedCompactionDB(t, dir)
			before, err := os.ReadFile(filepath.Join(dir, dataFileName))
			if err != nil {
				t.Fatal(err)
			}
			oldFile, oldIndex, oldOffset := db.file, maps.Clone(db.index), db.offset
			ops := defaultCompactionOps()
			create, rename := ops.createTemp, ops.rename
			ops.createTemp = func(dir, pattern string) (compactionFile, error) {
				if stage == "create" {
					return nil, injected
				}
				file, err := create(dir, pattern)
				if err != nil {
					return nil, err
				}
				wrapped := &compactionFaultFile{compactionFile: file, writeErr: injected}
				switch stage {
				case "header-write", "header-short-write":
					wrapped.writeAt = 1
				case "live-write", "live-short-write":
					wrapped.writeAt = 2
				case "temp-sync":
					wrapped.syncErr = injected
				}
				wrapped.short = stage == "header-short-write" || stage == "live-short-write"
				return wrapped, nil
			}
			ops.rename = func(old, new string) error {
				if stage == "rename-before-effect" {
					return injected
				}
				if err := rename(old, new); err != nil {
					return err
				}
				if stage == "rename-after-effect" {
					return injected
				}
				return nil
			}
			if stage == "directory-sync" {
				ops.syncDir = func(string) error { return injected }
			}
			wantErr := injected
			if stage == "header-short-write" || stage == "live-short-write" {
				wantErr = io.ErrShortWrite
			}
			err = db.compactWith(ops)
			if !errors.Is(err, wantErr) {
				t.Fatalf("Compact error = %v; want %v", err, wantErr)
			}
			if db.file != oldFile || db.offset != oldOffset || !reflect.DeepEqual(db.index, oldIndex) {
				t.Fatal("failed compaction published replacement state")
			}
			faulted := stage == "rename-before-effect" || stage == "rename-after-effect" || stage == "directory-sync"
			if faulted {
				if !errors.Is(err, ErrFaulted) {
					t.Fatalf("ambiguous replacement did not fault handle: %v", err)
				}
				assertStateError(t, db, ErrFaulted)
			} else {
				assertCompactionModel(t, db, model)
			}
			if stage != "rename-after-effect" && stage != "directory-sync" {
				assertLogUnchanged(t, dir, before)
			}
			if artifacts, err := filepath.Glob(filepath.Join(dir, compactionPrefix+"*")); err != nil || len(artifacts) != 0 {
				t.Fatalf("failed compaction cleanup left artifacts: %v, %v", artifacts, err)
			}
			if !faulted {
				if err := db.Set([]byte("still-writable"), []byte("yes")); err != nil {
					t.Fatal(err)
				}
				model["still-writable"] = []byte("yes")
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			assertCompactionModel(t, openTestDB(t, dir), model)
		})
	}
}

type closeObservedFile struct {
	dataFile
	onClose func()
	err     error
}

func (f *closeObservedFile) Close() error {
	if f.onClose != nil {
		f.onClose()
	}
	return errors.Join(f.dataFile.Close(), f.err)
}

func TestCompactionReplacementOrdering(t *testing.T) {
	db, model := seedCompactionDB(t, t.TempDir())
	oldIndex, oldOffset := maps.Clone(db.index), db.offset
	var synced, renamed, directorySynced, oldClosed bool
	old := &closeObservedFile{dataFile: db.file}
	old.onClose = func() {
		oldClosed = true
		if !directorySynced || db.file == old || db.offset != compactedSize(model) {
			t.Fatal("old file closed before synchronized replacement was published")
		}
	}
	db.file = old
	ops := defaultCompactionOps()
	create, rename, syncDir := ops.createTemp, ops.rename, ops.syncDir
	ops.createTemp = func(dir, pattern string) (compactionFile, error) {
		file, err := create(dir, pattern)
		if err != nil {
			return nil, err
		}
		return &compactionFaultFile{compactionFile: file, onSync: func() { synced = true }}, nil
	}
	ops.rename = func(oldPath, newPath string) error {
		if !synced {
			t.Fatal("rename occurred before temporary file Sync")
		}
		renamed = true
		return rename(oldPath, newPath)
	}
	ops.syncDir = func(dir string) error {
		if !renamed || db.file != old || db.offset != oldOffset || !reflect.DeepEqual(db.index, oldIndex) {
			t.Fatal("replacement published before directory Sync")
		}
		if err := syncDir(dir); err != nil {
			return err
		}
		directorySynced = true
		return nil
	}
	if err := db.compactWith(ops); err != nil {
		t.Fatal(err)
	}
	if !synced || !renamed || !directorySynced || !oldClosed {
		t.Fatalf("missing replacement stages: %v %v %v %v", synced, renamed, directorySynced, oldClosed)
	}
	assertCompactionModel(t, db, model)
}

func TestCompactionOldCloseFailureKeepsReplacementUsable(t *testing.T) {
	dir := t.TempDir()
	db, model := seedCompactionDB(t, dir)
	injected := errors.New("injected old file Close error")
	db.file = &closeObservedFile{dataFile: db.file, err: injected}
	if err := db.Compact(); !errors.Is(err, injected) || errors.Is(err, ErrFaulted) {
		t.Fatalf("Compact old Close error = %v", err)
	}
	assertCompactionModel(t, db, model)
	if err := db.Set([]byte("after-close-error"), []byte("new inode")); err != nil {
		t.Fatal(err)
	}
	model["after-close-error"] = []byte("new inode")
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	assertCompactionModel(t, openTestDB(t, dir), model)
}

func TestCompactionCleanupErrorsNeverRemoveDataFile(t *testing.T) {
	for _, stage := range []string{"close", "remove", "remove-not-exist"} {
		t.Run(stage, func(t *testing.T) {
			dir := t.TempDir()
			db, model := seedCompactionDB(t, dir)
			primary, cleanup := errors.New("write failure"), errors.New("cleanup failure")
			ops := defaultCompactionOps()
			create, remove := ops.createTemp, ops.remove
			var tempName string
			ops.createTemp = func(dir, pattern string) (compactionFile, error) {
				file, err := create(dir, pattern)
				if err != nil {
					return nil, err
				}
				tempName = file.Name()
				wrapped := &compactionFaultFile{compactionFile: file, writeAt: 1, writeErr: primary}
				if stage == "close" {
					wrapped.closeErr = cleanup
				}
				return wrapped, nil
			}
			ops.remove = func(path string) error {
				if path != tempName || path == filepath.Join(dir, dataFileName) {
					t.Fatalf("cleanup attempted unexpected path %q", path)
				}
				if stage == "remove" {
					return cleanup
				}
				if err := remove(path); err != nil {
					return err
				}
				if stage == "remove-not-exist" {
					return os.ErrNotExist
				}
				return nil
			}
			err := db.compactWith(ops)
			if !errors.Is(err, primary) || (stage != "remove-not-exist" && !errors.Is(err, cleanup)) {
				t.Fatalf("joined cleanup error = %v", err)
			}
			if stage == "remove-not-exist" && errors.Is(err, os.ErrNotExist) {
				t.Fatalf("missing cleanup path was not ignored: %v", err)
			}
			assertCompactionModel(t, db, model)
			if err := db.Set([]byte("after-cleanup-error"), []byte("usable")); err != nil {
				t.Fatal(err)
			}
			model["after-cleanup-error"] = []byte("usable")
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			assertCompactionModel(t, openTestDB(t, dir), model)
		})
	}
}

func TestCompactionRejectsSourceFailuresAndFaultsHandle(t *testing.T) {
	for _, stage := range []string{"checksum", "identity", "read-error"} {
		t.Run(stage, func(t *testing.T) {
			dir := t.TempDir()
			db, _ := seedCompactionDB(t, dir)
			injected := errors.New("injected source ReadAt failure")
			wantErr := error(ErrCorrupt)
			switch stage {
			case "checksum":
				file, err := os.OpenFile(filepath.Join(dir, dataFileName), os.O_WRONLY, 0)
				if err != nil {
					t.Fatal(err)
				}
				entry := db.index["kept"]
				_, err = file.WriteAt([]byte{0xff}, entry.offset+recordHeaderSize+int64(len("kept")))
				closeErr := file.Close()
				if err != nil || closeErr != nil {
					t.Fatalf("corrupt source: %v, %v", err, closeErr)
				}
			case "identity":
				db.index["kept"] = db.index["other"]
			case "read-error":
				db.file = &repairFaultFile{dataFile: db.file, readErr: injected, readOffset: db.index["kept"].offset}
				wantErr = injected
			}
			if err := db.Compact(); !errors.Is(err, wantErr) || !errors.Is(err, ErrFaulted) {
				t.Fatalf("Compact source error = %v; want %v and ErrFaulted", err, wantErr)
			}
			assertStateError(t, db, ErrFaulted)
			if artifacts, err := filepath.Glob(filepath.Join(dir, compactionPrefix+"*")); err != nil || len(artifacts) != 0 {
				t.Fatalf("source failure leaked temporary files: %v, %v", artifacts, err)
			}
		})
	}
}

func TestCompactionRejectsClosedAndFaultedHandles(t *testing.T) {
	db := openTestDB(t, t.TempDir())
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := db.Compact(); !errors.Is(err, ErrClosed) {
		t.Fatalf("Compact closed handle = %v", err)
	}
	db = openTestDB(t, t.TempDir())
	db.file = &faultFile{dataFile: db.file, onSync: func() error { return errors.New("disk failure") }}
	if err := db.Set([]byte("key"), []byte("value")); !errors.Is(err, ErrFaulted) {
		t.Fatal(err)
	}
	if err := db.Compact(); !errors.Is(err, ErrFaulted) {
		t.Fatalf("Compact faulted handle = %v", err)
	}
}

func TestCompactionWithConcurrentReadersAndWriters(t *testing.T) {
	dir := t.TempDir()
	db := openTestDB(t, dir)
	for worker := range 6 {
		if err := db.Set([]byte(fmt.Sprintf("worker:%d", worker)), []byte("initial")); err != nil {
			t.Fatal(err)
		}
	}
	start := make(chan struct{})
	var wg sync.WaitGroup
	for worker := range 6 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			key := []byte(fmt.Sprintf("worker:%d", worker))
			for revision := range 12 {
				value := []byte(fmt.Sprintf("revision:%d", revision))
				if err := db.Set(key, value); err != nil {
					t.Errorf("Set: %v", err)
					return
				}
				got, err := db.Get(key)
				if err != nil || !bytes.Equal(got, value) {
					t.Errorf("Get = %q, %v; want %q", got, err, value)
					return
				}
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		for range 5 {
			if err := db.Compact(); err != nil {
				t.Errorf("Compact: %v", err)
				return
			}
		}
	}()
	close(start)
	wg.Wait()
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db = openTestDB(t, dir)
	for worker := range 6 {
		assertValue(t, db, []byte(fmt.Sprintf("worker:%d", worker)), []byte("revision:11"))
	}
}
