package audit

import (
	"bytes"
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
