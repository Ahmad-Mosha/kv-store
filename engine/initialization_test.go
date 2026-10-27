package engine

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestOpenRefusesMissingInitializedLog(t *testing.T) {
	dir := t.TempDir()
	db := openTestDB(t, dir)
	if err := db.Set([]byte("retained"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(dir, dataFileName)); err != nil {
		t.Fatal(err)
	}
	if other, err := Open(dir); other != nil || !errors.Is(err, ErrCorrupt) {
		if other != nil {
			other.Close()
		}
		t.Fatalf("Open = %v, %v; missing initialized log must not be recreated", other, err)
	}
	if _, err := os.Stat(filepath.Join(dir, dataFileName)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("missing log was recreated: %v", err)
	}
}

func TestOpenUpgradesLegacyLock(t *testing.T) {
	dir := t.TempDir()
	data := append(encodeFileHeader(), mustRecord(t, kindSet, "legacy", "retained")...)
	writeTestLog(t, dir, data)
	if err := os.WriteFile(filepath.Join(dir, "LOCK"), nil, 0600); err != nil {
		t.Fatal(err)
	}
	db := openTestDB(t, dir)
	assertValue(t, db, []byte("legacy"), []byte("retained"))
	marker, err := os.ReadFile(filepath.Join(dir, "LOCK"))
	if err != nil || !bytes.Equal(marker, []byte{1}) {
		t.Fatalf("initialization marker = %v, %v", marker, err)
	}
	assertLogUnchanged(t, dir, data)
}

func TestOpenWithEmptyLockCanInitialize(t *testing.T) {
	dir := t.TempDir()
	if _, err := RepairTail(dir); !errors.Is(err, os.ErrNotExist) {
		t.Fatal(err)
	}
	db := openTestDB(t, dir)
	if err := db.Set([]byte("new"), nil); err != nil {
		t.Fatal(err)
	}
}

func TestOpenNeverPromotesCompactionArtifacts(t *testing.T) {
	for _, state := range []string{"missing", "valid", "corrupt"} {
		t.Run(state, func(t *testing.T) {
			dir := t.TempDir()
			artifact := append(encodeFileHeader(), mustRecord(t, kindSet, "key", "uncommitted")...)
			artifactPath := filepath.Join(dir, compactionPrefix+"orphan")
			if err := os.WriteFile(artifactPath, artifact, 0600); err != nil {
				t.Fatal(err)
			}
			var data []byte
			if state != "missing" {
				data = append(encodeFileHeader(), mustRecord(t, kindSet, "key", "authoritative")...)
				if state == "corrupt" {
					data[len(data)-1] ^= 1
				}
				writeTestLog(t, dir, data)
			}
			db, err := Open(dir)
			if state == "valid" {
				if err != nil {
					t.Fatal(err)
				}
				assertValue(t, db, []byte("key"), []byte("authoritative"))
				if err := db.Close(); err != nil {
					t.Fatal(err)
				}
			} else if db != nil || !errors.Is(err, ErrCorrupt) {
				if db != nil {
					db.Close()
				}
				t.Fatalf("Open = %v, %v", db, err)
			}
			if state == "missing" {
				if _, err := os.Stat(filepath.Join(dir, dataFileName)); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("created a log despite compaction artifacts: %v", err)
				}
			} else {
				assertLogUnchanged(t, dir, data)
			}
			after, err := os.ReadFile(artifactPath)
			if err != nil || !bytes.Equal(after, artifact) {
				t.Fatalf("startup modified a compaction artifact: %v", err)
			}
		})
	}
}

func TestInitializationMarkerFailures(t *testing.T) {
	for _, stage := range []string{"write", "short-write", "sync"} {
		t.Run(stage, func(t *testing.T) {
			file, err := os.CreateTemp(t.TempDir(), "lock")
			if err != nil {
				t.Fatal(err)
			}
			defer file.Close()
			injected := errors.New("injected marker failure")
			fault := &faultFile{dataFile: file}
			want := injected
			switch stage {
			case "write":
				fault.onWrite = func([]byte) (int, error) { return 0, injected }
			case "short-write":
				fault.onWrite = func([]byte) (int, error) { return 0, nil }
				want = io.ErrShortWrite
			case "sync":
				fault.onSync = func() error { return injected }
			}
			if err := markInitialized(fault); !errors.Is(err, want) {
				t.Fatalf("markInitialized = %v, want %v", err, want)
			}
			if stage != "sync" && fault.syncs != 0 {
				t.Fatal("sync attempted after an incomplete marker write")
			}
		})
	}
}

func TestConcurrentFirstOpenHasOneOwner(t *testing.T) {
	dir := t.TempDir()
	start := make(chan struct{})
	results := make(chan *DB, 2)
	failures := make(chan error, 2)
	var wg sync.WaitGroup
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			db, err := Open(dir)
			results <- db
			failures <- err
		}()
	}
	close(start)
	wg.Wait()
	close(results)
	close(failures)
	owners := 0
	for db := range results {
		if db != nil {
			owners++
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
		}
	}
	if owners != 1 {
		t.Fatalf("successful first openers = %d, want one", owners)
	}
	for err := range failures {
		if err != nil && !errors.Is(err, ErrLocked) {
			t.Fatalf("unexpected initialization failure: %v", err)
		}
	}
	openTestDB(t, dir)
}

func TestCompactionUsesAbsoluteDirectory(t *testing.T) {
	dir := t.TempDir()
	cwd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	relative, err := filepath.Rel(cwd, dir)
	if err != nil {
		t.Fatal(err)
	}
	db := openTestDB(t, relative)
	if err := db.Set([]byte("key"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	t.Chdir(t.TempDir())
	if err := db.Compact(); err != nil {
		t.Fatal(err)
	}
	assertValue(t, db, []byte("key"), []byte("value"))
	if err := db.Set([]byte("after"), []byte("retained")); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	assertValue(t, openTestDB(t, dir), []byte("after"), []byte("retained"))
}
