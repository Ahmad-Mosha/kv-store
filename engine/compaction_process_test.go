package engine

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

func pauseCompactionProcess() {
	fmt.Println("ready")
	io.Copy(io.Discard, os.Stdin)
	os.Exit(0)
}

type processCompactionFile struct {
	compactionFile
	stage  string
	writes int
}

func (f *processCompactionFile) Write(p []byte) (int, error) {
	f.writes++
	if f.stage == "partial-write" && f.writes == 2 {
		n, err := f.compactionFile.Write(p[:len(p)/2])
		if err == nil {
			pauseCompactionProcess()
		}
		return n, err
	}
	return f.compactionFile.Write(p)
}

func (f *processCompactionFile) Sync() error {
	if err := f.compactionFile.Sync(); err != nil {
		return err
	}
	if f.stage == "temp-sync" {
		pauseCompactionProcess()
	}
	return nil
}

func TestCompactionProcessHelper(t *testing.T) {
	stage := os.Getenv("DISKKV_COMPACTION_STAGE")
	if stage == "" {
		t.Skip("subprocess helper")
	}
	db, err := Open(os.Getenv("DISKKV_COMPACTION_DIR"))
	if err != nil {
		t.Fatal(err)
	}
	if stage == "after-publish" {
		db.file = &closeObservedFile{dataFile: db.file, onClose: pauseCompactionProcess}
	}
	ops := defaultCompactionOps()
	create, rename, syncDir := ops.createTemp, ops.rename, ops.syncDir
	ops.createTemp = func(dir, pattern string) (compactionFile, error) {
		file, err := create(dir, pattern)
		if err != nil {
			return nil, err
		}
		if stage == "temp-created" {
			pauseCompactionProcess()
		}
		return &processCompactionFile{compactionFile: file, stage: stage}, nil
	}
	ops.rename = func(old, new string) error {
		if stage == "before-rename" {
			pauseCompactionProcess()
		}
		if err := rename(old, new); err != nil {
			return err
		}
		if stage == "after-rename" {
			pauseCompactionProcess()
		}
		return nil
	}
	ops.syncDir = func(dir string) error {
		if err := syncDir(dir); err != nil {
			return err
		}
		if stage == "after-directory-sync" {
			pauseCompactionProcess()
		}
		return nil
	}
	if err := db.compactWith(ops); err != nil {
		t.Fatal(err)
	}
	t.Fatal("compaction returned without reaching selected process barrier")
}

func startCompactionProcess(t *testing.T, stage, dir string) *exec.Cmd {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestCompactionProcessHelper$")
	cmd.Env = append(os.Environ(), "DISKKV_COMPACTION_STAGE="+stage, "DISKKV_COMPACTION_DIR="+dir)
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
		t.Fatalf("compaction process did not reach %s: %v; stderr=%s", stage, scanner.Err(), stderr.String())
	}
	return cmd
}

func TestCompactionRecoversAfterProcessKillAtEveryReplacementStage(t *testing.T) {
	// These subprocess deaths preserve the kernel cache. They test process
	// recovery at real filesystem boundaries, not OS crash or power loss.
	for _, stage := range []string{
		"temp-created", "partial-write", "temp-sync", "before-rename",
		"after-rename", "after-directory-sync", "after-publish",
	} {
		t.Run(stage, func(t *testing.T) {
			dir := t.TempDir()
			db, model := seedCompactionDB(t, dir)
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			before, err := os.ReadFile(filepath.Join(dir, dataFileName))
			if err != nil {
				t.Fatal(err)
			}
			child := startCompactionProcess(t, stage, dir)
			if other, err := Open(dir); !errors.Is(err, ErrLocked) {
				if other != nil {
					other.Close()
				}
				t.Fatalf("child did not retain directory lock at %s: %v", stage, err)
			}
			if _, err := RepairTail(dir); !errors.Is(err, ErrLocked) {
				t.Fatalf("repair bypassed compaction ownership: %v", err)
			}
			if err := child.Process.Kill(); err != nil {
				t.Fatal(err)
			}
			if err := child.Wait(); err == nil {
				t.Fatal("killed child unexpectedly returned success")
			}
			replaced := stage == "after-rename" || stage == "after-directory-sync" || stage == "after-publish"
			if !replaced {
				assertLogUnchanged(t, dir, before)
			}
			artifacts, err := filepath.Glob(filepath.Join(dir, compactionPrefix+"*"))
			if err != nil {
				t.Fatal(err)
			}
			if !replaced && len(artifacts) != 1 {
				t.Fatalf("expected one ignored orphan at %s, found %v", stage, artifacts)
			}
			db = openTestDB(t, dir)
			assertCompactionModel(t, db, model)
			if replaced && db.offset != compactedSize(model) {
				t.Fatalf("reopened replacement size = %d; want %d", db.offset, compactedSize(model))
			}
			if err := db.Set([]byte("after-recovery"), []byte("new append")); err != nil {
				t.Fatal(err)
			}
			model["after-recovery"] = []byte("new append")
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			assertCompactionModel(t, openTestDB(t, dir), model)
		})
	}
}
