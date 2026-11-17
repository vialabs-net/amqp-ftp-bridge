package audit

import (
	"bytes"
	"io"
	"strings"
	"testing"
	"time"
)

func TestQuery(t *testing.T) {
	dir := t.TempDir()
	c := &clock{t: time.Date(2026, 3, 1, 10, 0, 0, 0, loc)}
	w, _ := NewWriter(dir, "v", "h", WithClock(c.now))
	for _, r := range []Record{
		{Step: StepReceived, MsgID: "m1", Queue: "a"},
		{Step: StepUploaded, MsgID: "m1", Queue: "a", FileName: "one.pdf"},
		{Step: StepAcked, MsgID: "m1", Queue: "a"},
		{Step: StepReceived, MsgID: "m2", Queue: "b"},
		{Step: StepDeadLettered, MsgID: "m2", Queue: "b", FileName: "two.txt"},
		{Step: StepReceived, MsgID: "m3", Queue: "a"},
	} {
		_ = w.Write(r)
	}
	c.set(time.Date(2026, 3, 2, 0, 0, 1, 0, loc))
	_ = w.Rollover()
	_ = w.Close()

	run := func(f Filter) (int, string) {
		t.Helper()
		f.Date = "2026-03-01"
		var buf bytes.Buffer
		n, err := Query(dir, f, &buf)
		if err != nil {
			t.Fatal(err)
		}
		return n, buf.String()
	}
	if n, out := run(Filter{}); n != 3 || strings.Count(out, "\n") != 7 || !strings.Contains(out, StepDailySummary) {
		t.Errorf("all: %d\n%s", n, out)
	}
	if n, out := run(Filter{Queue: "a"}); n != 2 || strings.Contains(out, StepDailySummary) {
		t.Errorf("queue a: %d\n%s", n, out)
	}
	if n, out := run(Filter{File: "*.pdf"}); n != 1 || strings.Count(out, `"msg_id":"m1"`) != 3 {
		t.Errorf("file: %d\n%s", n, out)
	}
	if n, _ := run(Filter{Result: ResultDeadLettered}); n != 1 {
		t.Errorf("dead_lettered: %d", n)
	}
	if n, _ := run(Filter{Result: ResultIncomplete}); n != 1 {
		t.Errorf("incomplete: %d", n)
	}
	if n, _ := run(Filter{Result: ResultAcked, Queue: "b"}); n != 0 {
		t.Errorf("acked in b: %d", n)
	}

	for _, f := range []Filter{{Date: "2026/03/01"}, {Date: "2026-03-01", Result: "x"}, {Date: "2026-03-01", File: "["}, {Date: "2026-01-01"}} {
		if _, err := Query(dir, f, &bytes.Buffer{}); err == nil {
			t.Errorf("%+v: want error", f)
		}
	}
	// Default date is today: no file today in this dir.
	if _, err := Query(dir, Filter{}, &bytes.Buffer{}); err == nil {
		t.Error("want error for missing file today")
	}
}

func TestQuerySharedMessageID(t *testing.T) {
	dir := t.TempDir()
	date := time.Date(2026, 3, 1, 10, 0, 0, 0, loc)
	w, err := NewWriter(dir, "v", "h", WithClock(func() time.Time { return date }))
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range []Record{
		{Step: StepReceived, MsgID: "shared", Queue: "a"},
		{Step: StepAcked, MsgID: "shared", Queue: "a"},
		{Step: StepReceived, MsgID: "shared", Queue: "b"},
		{Step: StepDeadLettered, MsgID: "shared", Queue: "b"},
	} {
		if err := w.Write(r); err != nil {
			t.Fatal(err)
		}
	}
	w.Close()
	var out bytes.Buffer
	n, err := Query(dir, Filter{Date: "2026-03-01", Queue: "a", Result: ResultAcked}, &out)
	if err != nil || n != 1 || strings.Count(out.String(), "\n") != 2 || strings.Contains(out.String(), `"queue":"b"`) {
		t.Errorf("cross-queue records or result: %d, %v, %s", n, err, out.String())
	}
}

func TestLargeEscapedRecordCanBeReopenedAndQueried(t *testing.T) {
	dir := t.TempDir()
	date := time.Date(2026, 3, 1, 10, 0, 0, 0, loc)
	opts := WithClock(func() time.Time { return date })
	w, err := NewWriter(dir, "v", "h", opts)
	if err != nil {
		t.Fatal(err)
	}
	body := strings.Repeat("\x00", 12<<20)
	if err := w.Write(Record{Step: StepReceived, MsgID: "large", Queue: "q", BodyText: &body}); err != nil {
		t.Fatal(err)
	}
	w.Close()
	w, err = NewWriter(dir, "v", "h", opts)
	if err != nil {
		t.Fatal(err)
	}
	w.Close()
	if n, err := Query(dir, Filter{Date: "2026-03-01"}, io.Discard); err != nil || n != 1 {
		t.Errorf("query writer-produced record: %d, %v", n, err)
	}
}

func TestQueryHTTP(t *testing.T) {
	dir := t.TempDir()
	c := &clock{t: time.Date(2026, 3, 1, 10, 0, 0, 0, loc)}
	w, _ := NewWriter(dir, "v", "h", WithClock(c.now))
	for _, r := range []Record{
		{Step: StepReceived, MsgID: "h1", Route: "upload", Method: "POST", ClientIP: "192.0.2.1", BytesDeclared: Ptr(int64(4))},
		{Step: StepUploaded, MsgID: "h1", Route: "upload", FileName: "a.pdf", Bytes: Ptr(int64(4))},
		{Step: StepResponded, MsgID: "h1", Route: "upload", HTTPStatus: 200},
		{Step: StepReceived, MsgID: "h2", Route: "download"},
		{Step: StepFailed, MsgID: "h2", Route: "download", FailedStep: "download", FTPCode: 550},
		{Step: StepResponded, MsgID: "h2", Route: "download", HTTPStatus: 404},
		{Step: StepReceived, MsgID: "h3", Route: "download"},
		{Step: StepDownloaded, MsgID: "h3", Route: "download", Bytes: Ptr(int64(9))},
		{Step: StepResponded, MsgID: "h3", Route: "download", HTTPStatus: 200},
	} {
		_ = w.Write(r)
	}
	got := w.RouteTotals()
	if got["upload"] != (RouteTotals{Received: 1, Uploaded: 1, Bytes: 4}) || got["download"] != (RouteTotals{Received: 2, Downloaded: 1, Failed: 1, Bytes: 9}) {
		t.Errorf("route totals = %+v", got)
	}
	if len(w.Totals()) != 0 {
		t.Errorf("queue totals = %+v", w.Totals())
	}
	c.set(time.Date(2026, 3, 2, 0, 0, 1, 0, loc))
	_ = w.Rollover()
	_ = w.Close()

	// A restart on the same day rebuilds route totals from the file.
	c.set(time.Date(2026, 3, 1, 12, 0, 0, 0, loc))
	w2, _ := NewWriter(dir, "v", "h", WithClock(c.now))
	if w2.RouteTotals()["download"].Received != 2 {
		t.Errorf("rebuilt = %+v", w2.RouteTotals())
	}
	_ = w2.Close()

	run := func(f Filter) int {
		t.Helper()
		f.Date = "2026-03-01"
		var buf bytes.Buffer
		n, err := Query(dir, f, &buf)
		if err != nil {
			t.Fatal(err)
		}
		return n
	}
	if n := run(Filter{Route: "download"}); n != 2 {
		t.Errorf("route: %d", n)
	}
	if n := run(Filter{Result: ResultFailed}); n != 1 {
		t.Errorf("failed: %d", n)
	}
	if n := run(Filter{Result: ResultResponded}); n != 2 {
		t.Errorf("responded: %d", n)
	}
	var buf bytes.Buffer
	_, _ = Query(dir, Filter{Date: "2026-03-01"}, &buf)
	if !strings.Contains(buf.String(), `"routes":{"download":{"received":2,"uploaded":0,"downloaded":1,"failed":1,"bytes":9}`) {
		t.Errorf("summary:\n%s", buf.String())
	}
}
