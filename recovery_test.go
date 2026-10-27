package diskkv

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func writeTestLog(t *testing.T, dir string, content []byte) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, dataFileName), content, 0600); err != nil {
		t.Fatal(err)
	}
}

func assertLogUnchanged(t *testing.T, dir string, want []byte) {
	t.Helper()
	got, err := os.ReadFile(filepath.Join(dir, dataFileName))
	if err != nil || !bytes.Equal(got, want) {
		t.Fatalf("log changed unexpectedly (read error %v)", err)
	}
}

func mustRecord(t *testing.T, kind recordKind, key, value string) []byte {
	t.Helper()
	record, err := encodeRecord(kind, []byte(key), []byte(value))
	if err != nil {
		t.Fatal(err)
	}
	return record
}

func TestOpenStrictlyRejectsTruncatedRecordAtEveryByte(t *testing.T) {
	prefix := append(encodeFileHeader(), mustRecord(t, kindSet, "safe", "retained")...)
	tail := mustRecord(t, kindSet, "interrupted", "incomplete mutation")
	for cut := 1; cut < len(tail); cut++ {
		t.Run(fmt.Sprintf("cut-%d", cut), func(t *testing.T) {
			dir := t.TempDir()
			content := append(bytes.Clone(prefix), tail[:cut]...)
			writeTestLog(t, dir, content)
			if db, err := Open(dir); !errors.Is(err, ErrTruncated) {
				if db != nil {
					db.Close()
				}
				t.Fatalf("Open error = %v; want ErrTruncated", err)
			}
			assertLogUnchanged(t, dir, content)
			discarded, err := RepairTail(dir)
			if err != nil || discarded != int64(cut) {
				t.Fatalf("RepairTail = %d, %v; want %d, nil", discarded, err, cut)
			}
			assertLogUnchanged(t, dir, prefix)
			db := openTestDB(t, dir)
			assertValue(t, db, []byte("safe"), []byte("retained"))
			assertMissing(t, db, []byte("interrupted"))
			if err := db.Set([]byte("after-repair"), []byte("writable")); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestRepairTailRefusesCorruptionAndIncompleteFileHeader(t *testing.T) {
	valid := append(encodeFileHeader(), mustRecord(t, kindSet, "safe", "value")...)
	corruptPayload := bytes.Clone(valid)
	corruptPayload[len(corruptPayload)-1] ^= 0xff
	corruptRecordHeader := bytes.Clone(valid)
	corruptRecordHeader[fileHeaderSize+8] ^= 0xff
	corruptFileHeader := bytes.Clone(valid)
	corruptFileHeader[0] ^= 0xff
	scenarios := []struct {
		name string
		data []byte
		want error
	}{
		{"payload-checksum", corruptPayload, ErrCorrupt},
		{"record-header-checksum", corruptRecordHeader, ErrCorrupt},
		{"file-header-checksum", corruptFileHeader, ErrCorrupt},
		{"empty-file", nil, ErrTruncated},
		{"incomplete-file-header", valid[:fileHeaderSize-1], ErrTruncated},
	}
	for _, scenario := range scenarios {
		t.Run(scenario.name, func(t *testing.T) {
			dir := t.TempDir()
			writeTestLog(t, dir, scenario.data)
			if db, err := Open(dir); !errors.Is(err, scenario.want) {
				if db != nil {
					db.Close()
				}
				t.Fatalf("Open error = %v; want %v", err, scenario.want)
			}
			if discarded, err := RepairTail(dir); discarded != 0 || !errors.Is(err, scenario.want) {
				t.Fatalf("RepairTail = %d, %v; want 0, %v", discarded, err, scenario.want)
			}
			assertLogUnchanged(t, dir, scenario.data)
		})
	}
}

func TestRepairTailLeavesCompleteLogUnchanged(t *testing.T) {
	dir := t.TempDir()
	content := append(encodeFileHeader(), mustRecord(t, kindSet, "key", "value")...)
	writeTestLog(t, dir, content)
	if discarded, err := RepairTail(dir); err != nil || discarded != 0 {
		t.Fatalf("RepairTail = %d, %v", discarded, err)
	}
	assertLogUnchanged(t, dir, content)
}

func TestRepairTailDoesNotCreateDatabase(t *testing.T) {
	dir := t.TempDir()
	if _, err := RepairTail(dir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("RepairTail missing database error = %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, dataFileName)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("repair created a data file: %v", err)
	}
}

func TestRepairTailSyncFailureReportsDiscardedBytes(t *testing.T) {
	dir := t.TempDir()
	prefix := append(encodeFileHeader(), mustRecord(t, kindSet, "key", "safe")...)
	content := append(bytes.Clone(prefix), []byte("partial")...)
	writeTestLog(t, dir, content)
	file, err := os.OpenFile(filepath.Join(dir, dataFileName), os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	injected := errors.New("injected repair sync failure")
	wrapped := &faultFile{dataFile: file, onSync: func() error { return injected }}
	discarded, err := repairFileTail(wrapped)
	if discarded != 7 || !errors.Is(err, injected) || wrapped.syncs != 1 {
		t.Fatalf("repairFileTail = %d, %v; sync calls %d", discarded, err, wrapped.syncs)
	}
	assertLogUnchanged(t, dir, prefix)
}

func TestOpenLockAndRepairExclusivity(t *testing.T) {
	dir := t.TempDir()
	db := openTestDB(t, dir)
	if other, err := Open(dir); !errors.Is(err, ErrLocked) {
		if other != nil {
			other.Close()
		}
		t.Fatalf("second Open error = %v; want ErrLocked", err)
	}
	if _, err := RepairTail(dir); !errors.Is(err, ErrLocked) {
		t.Fatalf("repair while open error = %v; want ErrLocked", err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := RepairTail(dir); err != nil {
		t.Fatal(err)
	}
	openTestDB(t, dir)
}

func TestOpenInitializationFailuresReleaseLock(t *testing.T) {
	injected := errors.New("injected initialization failure")
	for _, stage := range []string{"write", "sync", "directory-sync"} {
		t.Run(stage, func(t *testing.T) {
			dir := t.TempDir()
			openData := func(path string, flags int, mode os.FileMode) (dataFile, error) {
				file, err := os.OpenFile(path, flags, mode)
				if err != nil {
					return nil, err
				}
				wrapped := &faultFile{dataFile: file}
				if stage == "write" {
					wrapped.onWrite = func([]byte) (int, error) { return 0, injected }
				}
				if stage == "sync" {
					wrapped.onSync = func() error { return injected }
				}
				return wrapped, nil
			}
			syncDir := func(dir *os.File) error {
				if stage == "directory-sync" {
					return injected
				}
				return dir.Sync()
			}
			if db, err := openWith(dir, openData, syncDir); !errors.Is(err, injected) {
				if db != nil {
					db.Close()
				}
				t.Fatalf("openWith error = %v", err)
			}
			if stage == "write" {
				// Explicitly remove the failed new file; Open must not silently
				// initialize a file left empty by a failed prior initialization.
				if err := os.Remove(filepath.Join(dir, dataFileName)); err != nil {
					t.Fatal(err)
				}
			}
			openTestDB(t, dir)
		})
	}
}

func startProcessHelper(t *testing.T, mode, dir string) *exec.Cmd {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestStorageProcessHelper$")
	cmd.Env = append(os.Environ(), "DISKKV_TEST_HELPER="+mode, "DISKKV_TEST_DIR="+dir)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	stdin, err := cmd.StdinPipe()
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		cancel()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		stdin.Close()
		cmd.Process.Kill()
		cmd.Wait()
		cancel()
	})
	scanner := bufio.NewScanner(stdout)
	if !scanner.Scan() || strings.TrimSpace(scanner.Text()) != "ready" {
		cmd.Process.Kill()
		cmd.Wait()
		t.Fatalf("helper did not acknowledge readiness: %v; stderr=%s", scanner.Err(), stderr.String())
	}
	return cmd
}

func TestDirectoryLockAcrossProcesses(t *testing.T) {
	dir := t.TempDir()
	child := startProcessHelper(t, "lock", dir)
	if db, err := Open(dir); !errors.Is(err, ErrLocked) {
		if db != nil {
			db.Close()
		}
		t.Fatalf("Open while another process holds lock error = %v", err)
	}
	if err := child.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	if err := child.Wait(); err == nil {
		t.Fatal("killed helper unexpectedly exited successfully")
	}
	openTestDB(t, dir)
}

func TestAcknowledgedMutationsSurviveProcessKill(t *testing.T) {
	// Killing this child does not clear the kernel page cache or simulate power
	// loss. The test establishes process-crash recovery only.
	dir := t.TempDir()
	child := startProcessHelper(t, "write", dir)
	if err := child.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	if err := child.Wait(); err == nil {
		t.Fatal("killed helper unexpectedly exited successfully")
	}
	db := openTestDB(t, dir)
	assertValue(t, db, []byte("acknowledged"), []byte("last committed value"))
	assertMissing(t, db, []byte("deleted"))
}

func TestStorageProcessHelper(t *testing.T) {
	mode := os.Getenv("DISKKV_TEST_HELPER")
	if mode == "" {
		t.Skip("subprocess helper")
	}
	db, err := Open(os.Getenv("DISKKV_TEST_DIR"))
	if err != nil {
		t.Fatal(err)
	}
	if mode == "write" {
		for _, mutation := range []struct{ key, value string }{
			{"acknowledged", "old"},
			{"deleted", "must stay deleted"},
			{"acknowledged", "last committed value"},
		} {
			if err := db.Set([]byte(mutation.key), []byte(mutation.value)); err != nil {
				t.Fatal(err)
			}
		}
		if err := db.Delete([]byte("deleted")); err != nil {
			t.Fatal(err)
		}
	}
	fmt.Println("ready")
	// The parent keeps stdin open until it kills this process. No Close runs.
	if _, err := io.Copy(io.Discard, os.Stdin); err != nil {
		t.Fatal(err)
	}
}
