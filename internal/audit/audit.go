// Package audit writes the JSON Lines audit log, one file per local day.
package audit

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

// Steps.
const (
	StepReceived     = "received"
	StepDownloaded   = "downloaded"
	StepUploaded     = "uploaded"
	StepAcked        = "acked"
	StepDeadLettered = "dead_lettered"
	StepDailySummary = "daily_summary"
)

// TimeLayout is RFC 3339 with milliseconds and the local offset.
const TimeLayout = "2006-01-02T15:04:05.000Z07:00"

const dayLayout = "2006-01-02"

// Record is one audit line. Fields not used by a step are omitted.
type Record struct {
	Time     string `json:"ts"`
	Step     string `json:"step"`
	MsgID    string `json:"msg_id,omitempty"`
	Queue    string `json:"queue,omitempty"`
	Version  string `json:"version"`
	Hostname string `json:"hostname"`

	// received
	Event       *string         `json:"event,omitempty"`
	Matched     *bool           `json:"matched,omitempty"`
	Body        json.RawMessage `json:"body,omitempty"`
	BodyText    *string         `json:"body_text,omitempty"`
	Redelivered *bool           `json:"redelivered,omitempty"`

	// downloaded
	Source     string `json:"source,omitempty"`
	SourceID   string `json:"source_id,omitempty"`
	Host       string `json:"host,omitempty"`
	HTTPStatus int    `json:"http_status,omitempty"`
	SHA256     string `json:"sha256,omitempty"`

	// uploaded
	Folder   string `json:"folder,omitempty"`
	FileName string `json:"file_name,omitempty"`
	FTPCode  int    `json:"ftp_code,omitempty"`

	// downloaded, uploaded
	Bytes *int64 `json:"bytes,omitempty"`
	Ms    *int64 `json:"ms,omitempty"`

	// acked
	TotalMs *int64 `json:"total_ms,omitempty"`

	// dead_lettered
	FailedStep string `json:"failed_step,omitempty"`
	Error      string `json:"error,omitempty"`

	// daily_summary
	Date   string             `json:"date,omitempty"`
	Queues map[string]*Totals `json:"queues,omitempty"`
}

// Totals are the per-queue counters of a daily summary.
type Totals struct {
	Received     int64 `json:"received"`
	Uploaded     int64 `json:"uploaded"`
	DeadLettered int64 `json:"dead_lettered"`
	Bytes        int64 `json:"bytes"`
}

// Ptr returns a pointer to v.
func Ptr[T any](v T) *T { return &v }

// FileName returns the audit file name for the local day of t.
func FileName(t time.Time) string {
	return "audit-" + t.Format(dayLayout) + ".jsonl"
}

// Writer appends records to the file of the current local day and keeps the
// per-queue totals of that day.
type Writer struct {
	dir      string
	version  string
	hostname string
	now      func() time.Time

	mu        sync.Mutex
	file      *os.File
	fileDay   string
	totals    map[string]*Totals
	totalsDay string
}

// Option configures a Writer.
type Option func(*Writer)

// WithClock replaces time.Now, for tests.
func WithClock(now func() time.Time) Option { return func(w *Writer) { w.now = now } }

// NewWriter creates dir if needed, restores today's totals from today's file
// and writes a missing summary for yesterday.
func NewWriter(dir, version, hostname string, opts ...Option) (*Writer, error) {
	w := &Writer{dir: dir, version: version, hostname: hostname, now: time.Now}
	for _, o := range opts {
		o(w)
	}
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, fmt.Errorf("create audit dir: %w", err)
	}
	now := w.now()
	today := now.Format(dayLayout)
	t, _, err := summarize(filepath.Join(dir, FileName(now)))
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return nil, err
	}
	w.totals, w.totalsDay = t, today

	yesterday := now.AddDate(0, 0, -1)
	yt, hasSummary, err := summarize(filepath.Join(dir, FileName(yesterday)))
	if err == nil && !hasSummary {
		if err := w.appendTo(yesterday.Format(dayLayout), w.summaryRecord(yesterday.Format(dayLayout), yt)); err != nil {
			return nil, err
		}
	}
	return w, nil
}

// Write stamps r with the time, version and hostname, and appends it.
func (w *Writer) Write(r Record) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	now := w.now()
	day := now.Format(dayLayout)
	if day != w.totalsDay {
		if err := w.rolloverLocked(day); err != nil {
			return err
		}
	}
	r.Time = now.Format(TimeLayout)
	r.Version = w.version
	r.Hostname = w.hostname
	w.count(r)
	return w.appendLocked(day, r)
}

// Rollover writes the daily summary when the local day has changed since the
// last record. Call it at local midnight.
func (w *Writer) Rollover() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	day := w.now().Format(dayLayout)
	if day == w.totalsDay {
		return nil
	}
	return w.rolloverLocked(day)
}

func (w *Writer) rolloverLocked(newDay string) error {
	err := w.appendLocked(w.totalsDay, w.summaryRecord(w.totalsDay, w.totals))
	w.totals, w.totalsDay = map[string]*Totals{}, newDay
	return err
}

func (w *Writer) summaryRecord(day string, totals map[string]*Totals) Record {
	if totals == nil {
		totals = map[string]*Totals{}
	}
	return Record{
		Time:     w.now().Format(TimeLayout),
		Step:     StepDailySummary,
		Version:  w.version,
		Hostname: w.hostname,
		Date:     day,
		Queues:   totals,
	}
}

func (w *Writer) count(r Record) {
	if r.Queue == "" {
		return
	}
	add(w.totals, r)
}

func add(totals map[string]*Totals, r Record) {
	t := totals[r.Queue]
	if t == nil {
		t = &Totals{}
		totals[r.Queue] = t
	}
	switch r.Step {
	case StepReceived:
		t.Received++
	case StepUploaded:
		t.Uploaded++
		if r.Bytes != nil {
			t.Bytes += *r.Bytes
		}
	case StepDeadLettered:
		t.DeadLettered++
	}
}

func (w *Writer) appendTo(day string, r Record) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.appendLocked(day, r)
}

func (w *Writer) appendLocked(day string, r Record) error {
	line, err := json.Marshal(r)
	if err != nil {
		return fmt.Errorf("encode audit record: %w", err)
	}
	if w.fileDay != day || w.file == nil {
		if w.file != nil {
			_ = w.file.Close()
			w.file = nil
		}
		f, err := os.OpenFile(filepath.Join(w.dir, "audit-"+day+".jsonl"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o640)
		if err != nil {
			return fmt.Errorf("open audit file: %w", err)
		}
		w.file, w.fileDay = f, day
	}
	if _, err := w.file.Write(append(line, '\n')); err != nil {
		return fmt.Errorf("write audit file: %w", err)
	}
	return nil
}

// Totals returns a copy of today's totals.
func (w *Writer) Totals() map[string]Totals {
	w.mu.Lock()
	defer w.mu.Unlock()
	out := make(map[string]Totals, len(w.totals))
	for k, v := range w.totals {
		out[k] = *v
	}
	return out
}

// Close closes the current file.
func (w *Writer) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.file == nil {
		return nil
	}
	err := w.file.Close()
	w.file = nil
	return err
}

// summarize computes the totals of an audit file and reports whether it
// already holds a daily summary.
func summarize(path string) (map[string]*Totals, bool, error) {
	totals := map[string]*Totals{}
	f, err := os.Open(path)
	if err != nil {
		return totals, false, err
	}
	defer f.Close()
	hasSummary := false
	err = scan(f, func(r Record) bool {
		if r.Step == StepDailySummary {
			hasSummary = true
		} else if r.Queue != "" {
			add(totals, r)
		}
		return true
	})
	return totals, hasSummary, err
}

// Cleanup deletes audit-*.jsonl and service-*.log files in dir whose date is
// more than retentionDays days before now. It returns the deleted names.
func Cleanup(dir string, retentionDays int, now time.Time) ([]string, error) {
	if retentionDays <= 0 {
		return nil, nil
	}
	y, m, d := now.Date()
	cutoff := time.Date(y, m, d, 0, 0, 0, 0, now.Location()).AddDate(0, 0, -retentionDays)
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var deleted []string
	var errs []error
	for _, e := range entries {
		day, ok := fileDay(e.Name())
		if !ok || e.IsDir() {
			continue
		}
		t, err := time.ParseInLocation(dayLayout, day, now.Location())
		if err != nil || !t.Before(cutoff) {
			continue
		}
		if err := os.Remove(filepath.Join(dir, e.Name())); err != nil {
			errs = append(errs, err)
			continue
		}
		deleted = append(deleted, e.Name())
	}
	sort.Strings(deleted)
	return deleted, errors.Join(errs...)
}

func fileDay(name string) (string, bool) {
	for _, p := range [][2]string{{"audit-", ".jsonl"}, {"service-", ".log"}} {
		if len(name) == len(p[0])+len(dayLayout)+len(p[1]) &&
			name[:len(p[0])] == p[0] && name[len(name)-len(p[1]):] == p[1] {
			return name[len(p[0]) : len(p[0])+len(dayLayout)], true
		}
	}
	return "", false
}

// NextMidnight returns the next local midnight after t.
func NextMidnight(t time.Time) time.Time {
	y, m, d := t.Date()
	next := time.Date(y, m, d+1, 0, 0, 0, 0, t.Location())
	for !next.After(t) || next.Format(dayLayout) == t.Format(dayLayout) {
		next = next.Add(time.Hour)
	}
	return next
}

func scan(f *os.File, fn func(Record) bool) error {
	reader := bufio.NewReader(f)
	for {
		line, err := reader.ReadBytes('\n')
		if err != nil && err != io.EOF {
			return err
		}
		if len(line) == 0 {
			break
		}
		var r Record
		if err := json.Unmarshal(line, &r); err != nil {
			continue
		}
		if !fn(r) {
			break
		}
	}
	return nil
}
