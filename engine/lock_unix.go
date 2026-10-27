//go:build linux || darwin

package engine

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"golang.org/x/sys/unix"
)

func lockDirectory(dir string) (*os.File, *os.File, error) {
	directory, err := os.Open(dir)
	if err != nil {
		return nil, nil, fmt.Errorf("open database directory: %w", err)
	}
	info, err := directory.Stat()
	if err != nil || !info.IsDir() {
		directory.Close()
		if err == nil {
			err = fmt.Errorf("path is not a directory")
		}
		return nil, nil, fmt.Errorf("database directory: %w", err)
	}
	// Never unlink LOCK: another opener could then lock a different inode.
	lock, err := os.OpenFile(filepath.Join(dir, "LOCK"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		directory.Close()
		return nil, nil, fmt.Errorf("open directory lock: %w", err)
	}
	if err := unix.Flock(int(lock.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		lock.Close()
		directory.Close()
		if errors.Is(err, unix.EWOULDBLOCK) || errors.Is(err, unix.EAGAIN) {
			return nil, nil, ErrLocked
		}
		return nil, nil, fmt.Errorf("lock database directory: %w", err)
	}
	return directory, lock, nil
}

func unlockDirectory(lock *os.File) error {
	return errors.Join(unix.Flock(int(lock.Fd()), unix.LOCK_UN), lock.Close())
}
