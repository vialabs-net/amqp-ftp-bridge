// Package tempfiles owns the service's temporary directory for its lifetime.
package tempfiles

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// Acquire locks dir before deleting abandoned transfer files. Sharing a directory
// with a live service fails closed, so its in-flight files are never removed.
func Acquire(dir string) (*os.File, error) {
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(filepath.Join(dir, ".afb.lock"), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	if err := lock(f); err != nil {
		f.Close()
		return nil, fmt.Errorf("directory in use: %w", err)
	}
	entries, err := os.ReadDir(dir)
	if err == nil {
		for _, entry := range entries {
			name := entry.Name()
			if entry.Type().IsRegular() && (strings.HasPrefix(name, "download-") || strings.HasPrefix(name, "field-")) && strings.HasSuffix(name, ".tmp") {
				if err = os.Remove(filepath.Join(dir, name)); err != nil {
					break
				}
			}
		}
	}
	if err != nil {
		f.Close()
		return nil, err
	}
	return f, nil // Closing releases the OS lock, including on process death.
}
