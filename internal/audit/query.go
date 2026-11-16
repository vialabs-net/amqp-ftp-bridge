package audit

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"time"
)

// Message results used by Filter.Result.
const (
	ResultAcked        = "acked"
	ResultDeadLettered = "dead_lettered"
	ResultIncomplete   = "incomplete"
)

// Filter selects messages in an audit file. Empty fields match everything.
type Filter struct {
	// Date is the day (YYYY-MM-DD) whose file is read.
	Date string
	// Queue matches the queue name exactly.
	Queue string
	// File is a pattern (path.Match syntax) for the uploaded file name.
	File string
	// Result is acked, dead_lettered or incomplete.
	Result string
}

// Query writes, as JSON Lines, every record of the messages that match f.
// Records of one message stay together, in the order they were written.
// Daily summaries are written only when no filter other than Date is set.
// It returns the number of messages written.
func Query(dir string, f Filter, out io.Writer) (int, error) {
	if f.Date == "" {
		f.Date = time.Now().Format(dayLayout)
	}
	if _, err := time.Parse(dayLayout, f.Date); err != nil {
		return 0, fmt.Errorf("date must be YYYY-MM-DD: %q", f.Date)
	}
	switch f.Result {
	case "", ResultAcked, ResultDeadLettered, ResultIncomplete:
	default:
		return 0, fmt.Errorf("result must be acked, dead_lettered or incomplete: %q", f.Result)
	}
	if f.File != "" {
		if _, err := path.Match(f.File, ""); err != nil {
			return 0, fmt.Errorf("file pattern: %w", err)
		}
	}
	file, err := os.Open(filepath.Join(dir, "audit-"+f.Date+".jsonl"))
	if err != nil {
		return 0, err
	}
	defer file.Close()

	type message struct {
		records []Record
		lines   [][]byte
	}
	type messageKey struct{ queue, id string }
	var order []messageKey
	msgs := map[messageKey]*message{}
	var summaries [][]byte
	s := bufio.NewScanner(file)
	s.Buffer(make([]byte, 0, 64*1024), 64*1024*1024)
	for s.Scan() {
		var r Record
		if err := json.Unmarshal(s.Bytes(), &r); err != nil {
			continue
		}
		line := append([]byte(nil), s.Bytes()...)
		if r.Step == StepDailySummary {
			summaries = append(summaries, line)
			continue
		}
		key := messageKey{r.Queue, r.MsgID}
		m := msgs[key]
		if m == nil {
			m = &message{}
			msgs[key] = m
			order = append(order, key)
		}
		m.records = append(m.records, r)
		m.lines = append(m.lines, line)
	}
	if err := s.Err(); err != nil {
		return 0, err
	}

	n := 0
	for _, id := range order {
		m := msgs[id]
		if !f.matches(m.records) {
			continue
		}
		n++
		for _, l := range m.lines {
			if _, err := out.Write(append(l, '\n')); err != nil {
				return n, err
			}
		}
	}
	if f.Queue == "" && f.File == "" && f.Result == "" {
		for _, l := range summaries {
			if _, err := out.Write(append(l, '\n')); err != nil {
				return n, err
			}
		}
	}
	return n, nil
}

func (f Filter) matches(records []Record) bool {
	result := ResultIncomplete
	queueOK, fileOK := f.Queue == "", f.File == ""
	for _, r := range records {
		switch r.Step {
		case StepAcked:
			result = ResultAcked
		case StepDeadLettered:
			result = ResultDeadLettered
		}
		if r.Queue == f.Queue {
			queueOK = true
		}
		if f.File != "" && r.FileName != "" {
			if ok, _ := path.Match(f.File, r.FileName); ok {
				fileOK = true
			}
		}
	}
	return queueOK && fileOK && (f.Result == "" || f.Result == result)
}
