package engine

import (
	"fmt"
	"io"
	"os"
	"strings"
)

func hasCompactionArtifacts(dir string) (bool, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return false, fmt.Errorf("check compaction artifacts: %w", err)
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), compactionPrefix) {
			return true, nil
		}
	}
	return false, nil
}

func markInitialized(lock dataFile) error {
	n, err := lock.Write([]byte{1})
	if err != nil {
		return fmt.Errorf("write initialization marker: %w", err)
	}
	if n != 1 {
		return fmt.Errorf("write initialization marker: %w", io.ErrShortWrite)
	}
	if err := lock.Sync(); err != nil {
		return fmt.Errorf("sync initialization marker: %w", err)
	}
	return nil
}
