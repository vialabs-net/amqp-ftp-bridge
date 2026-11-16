package oplog

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestDailyFile(t *testing.T) {
	dir := t.TempDir()
	now := time.Date(2026, 3, 1, 23, 59, 0, 0, time.Local)
	d, err := NewDailyFile(dir, "service-", ".log", func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	_, _ = d.Write([]byte("one\n"))
	now = now.Add(2 * time.Minute)
	_, _ = d.Write([]byte("two\n"))
	_ = d.Close()
	_ = d.Close()
	for name, want := range map[string]string{"service-2026-03-01.log": "one\n", "service-2026-03-02.log": "two\n"} {
		b, _ := os.ReadFile(filepath.Join(dir, name))
		if string(b) != want {
			t.Errorf("%s = %q", name, b)
		}
	}
	if _, err := NewDailyFile(filepath.Join(dir, "service-2026-03-01.log", "x"), "a", "b", nil); err == nil {
		t.Error("want error")
	}
}

func TestNew(t *testing.T) {
	dir := t.TempDir()
	var mirror bytes.Buffer
	log, c, err := New(dir, &mirror)
	if err != nil {
		t.Fatal(err)
	}
	log.Info("amqp connected", "url", "amqps://rabbitmq.example.internal")
	_ = c.Close()
	if !strings.Contains(mirror.String(), "amqp connected") {
		t.Errorf("mirror = %q", mirror.String())
	}
	b, _ := os.ReadFile(filepath.Join(dir, "service-"+time.Now().Format("2006-01-02")+".log"))
	if !strings.Contains(string(b), "msg=\"amqp connected\"") {
		t.Errorf("file = %q", b)
	}
	if _, _, err := New(filepath.Join(dir, "service-"+time.Now().Format("2006-01-02")+".log", "x"), nil); err == nil {
		t.Error("want error")
	}
}
