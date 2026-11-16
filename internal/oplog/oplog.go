// Package oplog writes the operational log, service-YYYY-MM-DD.log, one file
// per local day, and optionally mirrors it to another writer (stderr).
package oplog

import (
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// DailyFile is an io.Writer that appends to <dir>/<prefix>YYYY-MM-DD<suffix>,
// switching files when the local day changes.
type DailyFile struct {
	dir, prefix, suffix string
	now                 func() time.Time

	mu   sync.Mutex
	day  string
	file *os.File
}

// NewDailyFile creates dir if needed.
func NewDailyFile(dir, prefix, suffix string, now func() time.Time) (*DailyFile, error) {
	if now == nil {
		now = time.Now
	}
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, fmt.Errorf("create log dir: %w", err)
	}
	return &DailyFile{dir: dir, prefix: prefix, suffix: suffix, now: now}, nil
}

// Write implements io.Writer.
func (d *DailyFile) Write(p []byte) (int, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	day := d.now().Format("2006-01-02")
	if d.file == nil || day != d.day {
		if d.file != nil {
			_ = d.file.Close()
			d.file = nil
		}
		f, err := os.OpenFile(filepath.Join(d.dir, d.prefix+day+d.suffix), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o640)
		if err != nil {
			return 0, err
		}
		d.file, d.day = f, day
	}
	return d.file.Write(p)
}

// Close closes the current file.
func (d *DailyFile) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.file == nil {
		return nil
	}
	err := d.file.Close()
	d.file = nil
	return err
}

// New returns a logger that writes text lines to service-YYYY-MM-DD.log in
// dir, and to mirror when it is not nil.
func New(dir string, mirror io.Writer) (*slog.Logger, io.Closer, error) {
	f, err := NewDailyFile(dir, "service-", ".log", nil)
	if err != nil {
		return nil, nil, err
	}
	var w io.Writer = f
	if mirror != nil {
		w = io.MultiWriter(f, mirror)
	}
	h := slog.NewTextHandler(w, &slog.HandlerOptions{
		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			if a.Key == slog.TimeKey && len(groups) == 0 {
				return slog.String(slog.TimeKey, a.Value.Time().Format("2006-01-02T15:04:05.000Z07:00"))
			}
			return a
		},
	})
	return slog.New(h), f, nil
}
