package engine

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func openTestDB(t *testing.T, dir string) *DB {
	t.Helper()
	db, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	return db
}

func assertValue(t *testing.T, db *DB, key, want []byte) {
	t.Helper()
	got, err := db.Get(key)
	if err != nil || !bytes.Equal(got, want) {
		t.Fatalf("Get(%q) = %q, %v; want %q, nil", key, got, err, want)
	}
	if exists, err := db.Exists(key); err != nil || !exists {
		t.Fatalf("Exists(%q) = %v, %v; want true, nil", key, exists, err)
	}
}

func assertMissing(t *testing.T, db *DB, key []byte) {
	t.Helper()
	if _, err := db.Get(key); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get(%q) error = %v; want ErrNotFound", key, err)
	}
	if exists, err := db.Exists(key); err != nil || exists {
		t.Fatalf("Exists(%q) = %v, %v; want false, nil", key, exists, err)
	}
}

func TestDBCRUDAndReopen(t *testing.T) {
	dir := t.TempDir()
	db := openTestDB(t, dir)
	key := []byte("account:42")
	assertMissing(t, db, key)
	for _, value := range []string{"first", "second", "last"} {
		if err := db.Set(key, []byte(value)); err != nil {
			t.Fatal(err)
		}
		assertValue(t, db, key, []byte(value))
	}
	if err := db.Set([]byte("deleted"), []byte("must not return")); err != nil {
		t.Fatal(err)
	}
	if err := db.Delete([]byte("deleted")); err != nil {
		t.Fatal(err)
	}
	before := db.offset
	if err := db.Delete([]byte("deleted")); err != nil {
		t.Fatal(err)
	}
	if db.offset != before {
		t.Fatal("deleting a missing key appended a record")
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db = openTestDB(t, dir)
	assertValue(t, db, key, []byte("last"))
	assertMissing(t, db, []byte("deleted"))
	if err := db.Set([]byte("deleted"), []byte("new generation")); err != nil {
		t.Fatal(err)
	}
	assertValue(t, db, []byte("deleted"), []byte("new generation"))
}

func TestDBBinaryDataAndOwnership(t *testing.T) {
	db := openTestDB(t, t.TempDir())
	key := []byte{0xff, 0, '\n', 0x80}
	value := []byte{0, 0xff, '\n', '\r', 0x80}
	wantKey, wantValue := bytes.Clone(key), bytes.Clone(value)
	if err := db.Set(key, value); err != nil {
		t.Fatal(err)
	}
	key[0], value[0] = 1, 1
	assertValue(t, db, wantKey, wantValue)
	got, err := db.Get(wantKey)
	if err != nil {
		t.Fatal(err)
	}
	got[0] = 99
	assertValue(t, db, wantKey, wantValue)
	if err := db.Set([]byte("empty"), nil); err != nil {
		t.Fatal(err)
	}
	assertValue(t, db, []byte("empty"), []byte{})
}

func TestDBValidationAndLimits(t *testing.T) {
	db := openTestDB(t, t.TempDir())
	for _, key := range [][]byte{nil, {}, bytes.Repeat([]byte("k"), MaxKeySize+1)} {
		if err := db.Set(key, []byte("v")); !errors.Is(err, ErrInvalidKey) {
			t.Errorf("Set invalid key error = %v", err)
		}
		if _, err := db.Get(key); !errors.Is(err, ErrInvalidKey) {
			t.Errorf("Get invalid key error = %v", err)
		}
		if _, err := db.Exists(key); !errors.Is(err, ErrInvalidKey) {
			t.Errorf("Exists invalid key error = %v", err)
		}
		if err := db.Delete(key); !errors.Is(err, ErrInvalidKey) {
			t.Errorf("Delete invalid key error = %v", err)
		}
	}
	if err := db.Set([]byte("large"), make([]byte, MaxValueSize+1)); !errors.Is(err, ErrValueTooLarge) {
		t.Fatalf("oversized value error = %v", err)
	}
	if db.offset != fileHeaderSize {
		t.Fatal("invalid inputs changed the log")
	}
	key, value := bytes.Repeat([]byte("k"), MaxKeySize), bytes.Repeat([]byte{0xff}, MaxValueSize)
	if err := db.Set(key, value); err != nil {
		t.Fatal(err)
	}
	assertValue(t, db, key, value)
}

func TestDBCloseIsIdempotent(t *testing.T) {
	dir := t.TempDir()
	db := openTestDB(t, dir)
	if err := db.Set([]byte("key"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
	}
	assertStateError(t, db, ErrClosed)
	assertValue(t, openTestDB(t, dir), []byte("key"), []byte("value"))
}

func assertStateError(t *testing.T, db *DB, want error) {
	t.Helper()
	checks := []struct {
		name string
		call func() error
	}{
		{"Set", func() error { return db.Set([]byte("key"), []byte("new")) }},
		{"Get", func() error { _, err := db.Get([]byte("key")); return err }},
		{"Exists", func() error { _, err := db.Exists([]byte("key")); return err }},
		{"Delete", func() error { return db.Delete([]byte("key")) }},
	}
	for _, check := range checks {
		if err := check.call(); !errors.Is(err, want) {
			t.Errorf("%s error = %v; want %v", check.name, err, want)
		}
	}
}

func TestDBReferenceModelAcrossRestarts(t *testing.T) {
	dir := t.TempDir()
	db := openTestDB(t, dir)
	model := make(map[string][]byte)
	rng := rand.New(rand.NewSource(20261027))
	for step := range 240 {
		key := fmt.Sprintf("key:%d", rng.Intn(17))
		if rng.Intn(3) == 0 {
			if err := db.Delete([]byte(key)); err != nil {
				t.Fatal(err)
			}
			delete(model, key)
		} else {
			value := make([]byte, rng.Intn(128))
			if _, err := rng.Read(value); err != nil {
				t.Fatal(err)
			}
			if err := db.Set([]byte(key), value); err != nil {
				t.Fatal(err)
			}
			model[key] = bytes.Clone(value)
		}
		if step%19 == 0 || step == 239 {
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			db = openTestDB(t, dir)
			for n := range 17 {
				key := fmt.Sprintf("key:%d", n)
				if want, exists := model[key]; exists {
					assertValue(t, db, []byte(key), want)
				} else {
					assertMissing(t, db, []byte(key))
				}
			}
		}
	}
}

func TestDBConcurrentOperations(t *testing.T) {
	dir := t.TempDir()
	db := openTestDB(t, dir)
	var wg sync.WaitGroup
	for worker := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			key := []byte(fmt.Sprintf("worker:%d", worker))
			for iteration := range 20 {
				value := []byte(fmt.Sprintf("%d:%d", worker, iteration))
				if err := db.Set(key, value); err != nil {
					t.Errorf("Set: %v", err)
					return
				}
				got, err := db.Get(key)
				if err != nil || !bytes.Equal(got, value) {
					t.Errorf("Get own key = %q, %v; want %q", got, err, value)
					return
				}
				if err := db.Set([]byte("shared"), value); err != nil {
					t.Errorf("Set shared: %v", err)
					return
				}
				if _, err := db.Get([]byte("shared")); err != nil {
					t.Errorf("Get shared: %v", err)
					return
				}
			}
		}()
	}
	wg.Wait()
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db = openTestDB(t, dir)
	for worker := range 8 {
		assertValue(t, db, []byte(fmt.Sprintf("worker:%d", worker)), []byte(fmt.Sprintf("%d:19", worker)))
	}
}

type faultFile struct {
	dataFile
	onWrite func([]byte) (int, error)
	onSync  func() error
	writes  int
	syncs   int
}

func (f *faultFile) Write(p []byte) (int, error) {
	f.writes++
	if f.onWrite != nil {
		return f.onWrite(p)
	}
	return f.dataFile.Write(p)
}

func (f *faultFile) Sync() error {
	f.syncs++
	if f.onSync != nil {
		return f.onSync()
	}
	return f.dataFile.Sync()
}

func TestDBPublishesOnlyAfterSync(t *testing.T) {
	db := openTestDB(t, t.TempDir())
	real := db.file
	key := "new-key"
	oldOffset := db.offset
	var stages []string
	f := &faultFile{dataFile: real}
	f.onWrite = func(p []byte) (int, error) {
		stages = append(stages, "write")
		return real.Write(p)
	}
	f.onSync = func() error {
		stages = append(stages, "sync")
		if _, exists := db.index[key]; exists || db.offset != oldOffset {
			t.Fatal("mutation published before Sync completed")
		}
		return real.Sync()
	}
	db.file = f
	if err := db.Set([]byte(key), []byte("value")); err != nil {
		t.Fatal(err)
	}
	db.file = real
	if fmt.Sprint(stages) != "[write sync]" {
		t.Fatalf("I/O stages = %v", stages)
	}
	if _, exists := db.index[key]; !exists || db.offset <= oldOffset {
		t.Fatal("successful mutation was not published")
	}
	assertValue(t, db, []byte(key), []byte("value"))
}

func TestDBMutationFailuresFaultEntireHandle(t *testing.T) {
	injected := errors.New("injected disk error")
	for _, scenario := range []string{"write-error", "short-write", "complete-write-error", "sync-error"} {
		t.Run(scenario, func(t *testing.T) {
			dir := t.TempDir()
			db := openTestDB(t, dir)
			if err := db.Set([]byte("key"), []byte("old")); err != nil {
				t.Fatal(err)
			}
			real := db.file
			oldOffset, oldEntry := db.offset, db.index["key"]
			f := &faultFile{dataFile: real}
			wantErr := injected
			switch scenario {
			case "write-error":
				f.onWrite = func([]byte) (int, error) { return 0, injected }
			case "short-write":
				f.onWrite = func(p []byte) (int, error) { return real.Write(p[:len(p)/2]) }
				wantErr = io.ErrShortWrite
			case "complete-write-error":
				f.onWrite = func(p []byte) (int, error) {
					n, err := real.Write(p)
					if err != nil {
						return n, err
					}
					return n, injected
				}
			case "sync-error":
				f.onSync = func() error { return injected }
			}
			db.file = f
			if err := db.Set([]byte("key"), []byte("new")); !errors.Is(err, wantErr) {
				t.Fatalf("Set error = %v; want %v", err, wantErr)
			}
			if db.offset != oldOffset || db.index["key"] != oldEntry {
				t.Fatal("failed mutation changed published index/offset")
			}
			writes, syncs := f.writes, f.syncs
			assertStateError(t, db, ErrFaulted)
			if f.writes != writes || f.syncs != syncs {
				t.Fatal("faulted handle attempted more mutations")
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			if scenario == "short-write" {
				if _, err := Open(dir); !errors.Is(err, ErrTruncated) {
					t.Fatalf("reopen incomplete write error = %v", err)
				}
				if discarded, err := RepairTail(dir); err != nil || discarded <= 0 {
					t.Fatalf("RepairTail = %d, %v", discarded, err)
				}
			}
			reopened := openTestDB(t, dir)
			want := []byte("new")
			if scenario == "write-error" || scenario == "short-write" {
				want = []byte("old")
			}
			assertValue(t, reopened, []byte("key"), want)
		})
	}
}

func TestDBGetDetectsPostOpenCorruption(t *testing.T) {
	dir := t.TempDir()
	db := openTestDB(t, dir)
	if err := db.Set([]byte("key"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	file, err := os.OpenFile(filepath.Join(dir, dataFileName), os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { file.Close() })
	entry := db.index["key"]
	if _, err := file.WriteAt([]byte{0xff}, entry.offset+recordHeaderSize+3); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Get([]byte("key")); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("Get corrupted record error = %v", err)
	}
}

func TestDBFailedDeleteDoesNotPublishTombstone(t *testing.T) {
	dir := t.TempDir()
	db := openTestDB(t, dir)
	if err := db.Set([]byte("key"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	before, entry := db.offset, db.index["key"]
	injected := errors.New("injected tombstone sync failure")
	db.file = &faultFile{dataFile: db.file, onSync: func() error { return injected }}
	if err := db.Delete([]byte("key")); !errors.Is(err, injected) {
		t.Fatalf("Delete error = %v", err)
	}
	if db.offset != before || db.index["key"] != entry {
		t.Fatal("failed Delete published a tombstone")
	}
	assertStateError(t, db, ErrFaulted)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	// A failed Sync does not roll back a complete tombstone.
	assertMissing(t, openTestDB(t, dir), []byte("key"))
}

type closeErrorFile struct {
	dataFile
	err error
}

func (f closeErrorFile) Close() error {
	return errors.Join(f.dataFile.Close(), f.err)
}

func TestDBCloseErrorStillReleasesLock(t *testing.T) {
	dir := t.TempDir()
	db := openTestDB(t, dir)
	injected := errors.New("injected close failure")
	db.file = closeErrorFile{dataFile: db.file, err: injected}
	if err := db.Close(); !errors.Is(err, injected) {
		t.Fatalf("Close error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("second Close = %v", err)
	}
	openTestDB(t, dir)
}

func TestDBCloseWaitsForConcurrentOperations(t *testing.T) {
	db := openTestDB(t, t.TempDir())
	start := make(chan struct{})
	var wg sync.WaitGroup
	for worker := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			key := []byte(fmt.Sprintf("worker:%d", worker))
			for range 10 {
				if err := db.Set(key, []byte("value")); err != nil && !errors.Is(err, ErrClosed) {
					t.Errorf("concurrent Set: %v", err)
				}
				if _, err := db.Get(key); err != nil && !errors.Is(err, ErrClosed) {
					t.Errorf("concurrent Get: %v", err)
				}
			}
		}()
	}
	close(start)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	wg.Wait()
	assertStateError(t, db, ErrClosed)
}
