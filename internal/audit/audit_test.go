package audit

import (
	"bufio"
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"
)

type clock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *clock) now() time.Time  { c.mu.Lock(); defer c.mu.Unlock(); return c.t }
func (c *clock) set(t time.Time) { c.mu.Lock(); c.t = t; c.mu.Unlock() }

var loc = time.FixedZone("test", -3*3600)

func readRecords(t *testing.T, path string) []Record {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var out []Record
	s := bufio.NewScanner(f)
	for s.Scan() {
		var r Record
		if err := json.Unmarshal(s.Bytes(), &r); err != nil {
			t.Fatalf("bad line %q: %v", s.Text(), err)
		}
		out = append(out, r)
	}
	return out
}

func TestWriterAndDailySummary(t *testing.T) {
	dir := t.TempDir()
	c := &clock{t: time.Date(2026, 3, 1, 23, 59, 0, 123e6, loc)}
	w, err := NewWriter(dir, "1.0.0", "host-a", WithClock(c.now))
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	write := func(r Record) {
		if err := w.Write(r); err != nil {
			t.Fatal(err)
		}
	}
	write(Record{Step: StepReceived, MsgID: "m1", Queue: "files.upload", Body: json.RawMessage(`{"a":1}`), Redelivered: Ptr(false)})
	write(Record{Step: StepUploaded, MsgID: "m1", Queue: "files.upload", Bytes: Ptr[int64](10), FTPCode: 226})
	write(Record{Step: StepAcked, MsgID: "m1", Queue: "files.upload", TotalMs: Ptr[int64](5)})
	write(Record{Step: StepReceived, MsgID: "m2", Queue: "files.other"})
	write(Record{Step: StepDeadLettered, MsgID: "m2", Queue: "files.other", FailedStep: "download"})
	if got := w.Totals()["files.upload"]; got != (Totals{Received: 1, Uploaded: 1, Bytes: 10}) {
		t.Errorf("totals = %+v", got)
	}

	// Rollover before midnight does nothing.
	if err := w.Rollover(); err != nil {
		t.Fatal(err)
	}
	c.set(time.Date(2026, 3, 2, 0, 0, 0, 5e6, loc))
	if err := w.Rollover(); err != nil {
		t.Fatal(err)
	}
	write(Record{Step: StepReceived, MsgID: "m3", Queue: "files.upload"})

	day1 := readRecords(t, filepath.Join(dir, "audit-2026-03-01.jsonl"))
	if len(day1) != 6 {
		t.Fatalf("day1 has %d records", len(day1))
	}
	first := day1[0]
	if first.Time != "2026-03-01T23:59:00.123-03:00" || first.Version != "1.0.0" || first.Hostname != "host-a" {
		t.Errorf("stamp = %+v", first)
	}
	if !regexp.MustCompile(`^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{3}[+-]\d\d:\d\d$`).MatchString(first.Time) {
		t.Errorf("time format %q", first.Time)
	}
	sum := day1[5]
	if sum.Step != StepDailySummary || sum.Date != "2026-03-01" {
		t.Fatalf("summary = %+v", sum)
	}
	if *sum.Queues["files.upload"] != (Totals{Received: 1, Uploaded: 1, Bytes: 10}) ||
		*sum.Queues["files.other"] != (Totals{Received: 1, DeadLettered: 1}) {
		t.Errorf("summary queues = %+v %+v", sum.Queues["files.upload"], sum.Queues["files.other"])
	}
	day2 := readRecords(t, filepath.Join(dir, "audit-2026-03-02.jsonl"))
	if len(day2) != 1 || day2[0].MsgID != "m3" {
		t.Errorf("day2 = %+v", day2)
	}
}

func TestWriteAfterMidnightWithoutRollover(t *testing.T) {
	dir := t.TempDir()
	c := &clock{t: time.Date(2026, 3, 1, 12, 0, 0, 0, loc)}
	w, _ := NewWriter(dir, "v", "h", WithClock(c.now))
	defer w.Close()
	_ = w.Write(Record{Step: StepReceived, MsgID: "m1", Queue: "q"})
	c.set(time.Date(2026, 3, 2, 1, 0, 0, 0, loc))
	_ = w.Write(Record{Step: StepReceived, MsgID: "m2", Queue: "q"})
	day1 := readRecords(t, filepath.Join(dir, "audit-2026-03-01.jsonl"))
	if len(day1) != 2 || day1[1].Step != StepDailySummary || day1[1].Queues["q"].Received != 1 {
		t.Errorf("day1 = %+v", day1)
	}
}

func TestRestoreAndMissingSummary(t *testing.T) {
	dir := t.TempDir()
	c := &clock{t: time.Date(2026, 3, 1, 10, 0, 0, 0, loc)}
	w, _ := NewWriter(dir, "v", "h", WithClock(c.now))
	_ = w.Write(Record{Step: StepReceived, MsgID: "m1", Queue: "q"})
	_ = w.Write(Record{Step: StepUploaded, MsgID: "m1", Queue: "q", Bytes: Ptr[int64](7)})
	_ = w.Close()

	// Restart the same day: totals come back from the file.
	w, _ = NewWriter(dir, "v", "h", WithClock(c.now))
	if got := w.Totals()["q"]; got != (Totals{Received: 1, Uploaded: 1, Bytes: 7}) {
		t.Errorf("restored totals = %+v", got)
	}
	_ = w.Close()

	// Restart the next day: yesterday gets its missing summary, only once.
	c.set(time.Date(2026, 3, 2, 9, 0, 0, 0, loc))
	for i := 0; i < 2; i++ {
		w, err := NewWriter(dir, "v", "h", WithClock(c.now))
		if err != nil {
			t.Fatal(err)
		}
		_ = w.Close()
	}
	day1 := readRecords(t, filepath.Join(dir, "audit-2026-03-01.jsonl"))
	if len(day1) != 3 || day1[2].Step != StepDailySummary || day1[2].Queues["q"].Bytes != 7 {
		t.Errorf("day1 = %+v", day1)
	}
}

func TestNewWriterBadDir(t *testing.T) {
	f := filepath.Join(t.TempDir(), "file")
	_ = os.WriteFile(f, nil, 0o600)
	if _, err := NewWriter(filepath.Join(f, "sub"), "v", "h"); err == nil {
		t.Error("want error")
	}
}

func TestCleanup(t *testing.T) {
	dir := t.TempDir()
	names := []string{
		"audit-2025-01-01.jsonl", "audit-2026-02-28.jsonl", "audit-2026-03-01.jsonl",
		"service-2025-12-31.log", "service-2026-03-01.log", "other-2020-01-01.txt", "audit-bad.jsonl",
	}
	for _, n := range names {
		_ = os.WriteFile(filepath.Join(dir, n), nil, 0o600)
	}
	now := time.Date(2026, 3, 1, 15, 0, 0, 0, loc)
	deleted, err := Cleanup(dir, 30, now)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(deleted, ",") != "audit-2025-01-01.jsonl,service-2025-12-31.log" {
		t.Errorf("deleted = %v", deleted)
	}
	if d, _ := Cleanup(dir, 0, now); d != nil {
		t.Error("retention 0 must keep everything")
	}
	if _, err := Cleanup(filepath.Join(dir, "missing"), 1, now); err == nil {
		t.Error("want error for missing dir")
	}
}

func TestNextMidnight(t *testing.T) {
	got := NextMidnight(time.Date(2026, 12, 31, 18, 30, 0, 0, loc))
	if !got.Equal(time.Date(2027, 1, 1, 0, 0, 0, 0, loc)) {
		t.Errorf("NextMidnight = %v", got)
	}
	if FileName(got) != "audit-2027-01-01.jsonl" {
		t.Error(FileName(got))
	}
}

func TestNoSecretsInRecord(t *testing.T) {
	b, _ := json.Marshal(Record{Step: StepDownloaded, Host: "files.example.com"})
	if bytes.Contains(b, []byte("Authorization")) || bytes.Contains(b, []byte("password")) {
		t.Error(string(b))
	}
}

func TestNextMidnightDST(t *testing.T) {
	zone, err := time.LoadLocation("America/Santiago")
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 9, 5, 23, 30, 0, 0, zone)
	if next := NextMidnight(now); !next.After(now) || next.Day() != 6 || next.Hour() != 1 {
		t.Errorf("next day starts at %v", next)
	}
}

func TestRestartAfterPartialAuditLine(t *testing.T) {
	for _, tail := range []string{`{"step":"received"`, `{"step":"received","msg_id":"old","queue":"q"}`} {
		t.Run(tail, func(t *testing.T) {
			dir := t.TempDir()
			now := time.Date(2026, 3, 1, 12, 0, 0, 0, loc)
			if err := os.WriteFile(filepath.Join(dir, FileName(now)), []byte(tail), 0o600); err != nil {
				t.Fatal(err)
			}
			w, err := NewWriter(dir, "v", "host-a", WithClock(func() time.Time { return now }))
			if err != nil {
				t.Fatal(err)
			}
			if err := w.Write(Record{Step: StepReceived, MsgID: "new", Queue: "q"}); err != nil {
				t.Fatal(err)
			}
			w.Close()
			var out bytes.Buffer
			if _, err := Query(dir, Filter{Date: "2026-03-01"}, &out); err != nil || !strings.Contains(out.String(), `"msg_id":"new"`) {
				t.Fatalf("new record lost: %s %v", out.String(), err)
			}
			w, err = NewWriter(dir, "v", "host-a", WithClock(func() time.Time { return now }))
			if err != nil {
				t.Fatal(err)
			}
			defer w.Close()
			want := int64(1)
			if json.Valid([]byte(tail)) {
				want = 2
			}
			if w.Totals()["q"].Received != want {
				t.Fatal("restart totals lost valid records")
			}
		})
	}
}
