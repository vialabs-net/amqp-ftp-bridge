package ftpup

import (
	"context"
	"errors"
	"net"
	"net/textproto"
	"strings"
	"testing"
	"time"
)

func TestJoin(t *testing.T) {
	ok := map[[2]string]string{
		{"inbox/2026", "a.pdf"}: "inbox/2026/a.pdf",
		{"/inbox/", "a.pdf"}:    "/inbox/a.pdf",
		{`inbox\sub`, "a.pdf"}:  "inbox/sub/a.pdf",
		{"", "a.pdf"}:           "a.pdf",
		{"./inbox//x", "b"}:     "inbox/x/b",
		{"/", "root.txt"}:       "/root.txt",
	}
	for in, want := range ok {
		got, err := Join(in[0], in[1])
		if err != nil || got != want {
			t.Errorf("Join(%q, %q) = %q, %v; want %q", in[0], in[1], got, err, want)
		}
	}
	for _, in := range [][2]string{{"a", ""}, {"a", ".."}, {"a", "x/y"}, {"a", `x\y`}, {"../a", "f"}, {`a\..\b`, "f"}, {"a\x00", "f"}, {"a", "f\x00"}} {
		if _, err := Join(in[0], in[1]); err == nil {
			t.Errorf("Join(%q, %q): want error", in[0], in[1])
		}
	}
}

func TestError(t *testing.T) {
	e := wrap("stor x", &textproto.Error{Code: 553, Msg: "Could not create file."})
	if e.Error() != "ftp stor x: 553 Could not create file." || codeOf(e) != 553 {
		t.Errorf("%q %d", e.Error(), codeOf(e))
	}
	e = wrap("connect", errors.New("refused"))
	if e.Error() != "ftp connect: refused" || codeOf(e) != 0 || !errors.Is(e, errors.Unwrap(e)) {
		t.Error(e)
	}
	if wrap("x", nil) != nil {
		t.Error("wrap nil")
	}
	if replyText(errors.New("x")) != "x" {
		t.Error("replyText")
	}
}

func TestConnectFails(t *testing.T) {
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	addr := ln.Addr().String()
	ln.Close()
	c := &Client{Address: addr, Timeout: time.Second}
	if _, err := c.Upload(context.Background(), "a", "b", strings.NewReader("x")); err == nil || !strings.Contains(err.Error(), "ftp connect") {
		t.Errorf("err = %v", err)
	}
	if _, err := c.Upload(context.Background(), "a", "..", strings.NewReader("x")); err == nil {
		t.Error("want invalid name")
	}
	if err := c.Check(context.Background()); err == nil {
		t.Error("check: want error")
	}
}
