//go:build !linux && !darwin

package diskkv

import (
	"fmt"
	"os"
)

func lockDirectory(string) (*os.File, *os.File, error) {
	return nil, nil, fmt.Errorf("diskkv: directory locking is supported only on Linux and macOS")
}

func unlockDirectory(*os.File) error { return nil }
