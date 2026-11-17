package ftpup

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/textproto"
	"strings"
	"sync"
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

func TestStalledServer(t *testing.T) {
	for _, greeting := range []string{"", "220 ready\r\n"} {
		for _, cancelOnly := range []bool{false, true} {
			ln, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			done := make(chan struct{})
			go func() {
				defer close(done)
				conn, err := ln.Accept()
				if err != nil {
					return
				}
				defer conn.Close()
				_, _ = conn.Write([]byte(greeting))
				_, _ = io.Copy(io.Discard, conn)
			}()
			ctx, cancel := context.WithCancel(context.Background())
			timeout := 50 * time.Millisecond
			if cancelOnly {
				timeout = time.Minute
			}
			timer := time.AfterFunc(150*time.Millisecond, cancel)
			c := &Client{Address: ln.Addr().String(), Timeout: timeout}
			start := time.Now()
			err = c.Check(ctx)
			timer.Stop()
			cancel()
			ln.Close()
			<-done
			if err == nil || time.Since(start) > time.Second {
				t.Errorf("stalled server: greeting %q, cancel %v: %v after %v", greeting, cancelOnly, err, time.Since(start))
			}
		}
	}
}

func TestQuotaFailureDoesNotDeleteDestination(t *testing.T) {
	control, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer control.Close()
	data, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer data.Close()
	done := make(chan struct{})
	var commands []string
	go func() {
		defer close(done)
		conn, err := control.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		fmt.Fprint(conn, "220 ready\r\n")
		r := bufio.NewReader(conn)
		for {
			line, err := r.ReadString('\n')
			if err != nil {
				return
			}
			command := strings.Fields(line)[0]
			commands = append(commands, strings.TrimSpace(line))
			reply := "200 ok\r\n"
			switch command {
			case "USER":
				reply = "230 logged in\r\n"
			case "FEAT":
				reply = "211 no features\r\n"
			case "EPSV":
				reply = fmt.Sprintf("229 (|||%d|)\r\n", data.Addr().(*net.TCPAddr).Port)
			case "STOR":
				reply = "552 quota exceeded\r\n"
			case "QUIT":
				return
			}
			fmt.Fprint(conn, reply)
		}
	}()
	c := &Client{Address: control.Addr().String(), User: "u", Password: "p", Timeout: time.Second}
	var mu sync.Mutex
	open, peak := 0, 0
	c.Dial = func(ctx context.Context, network, address string) (net.Conn, error) {
		conn, err := (&net.Dialer{}).DialContext(ctx, network, address)
		if err != nil || address != c.Address {
			return conn, err
		}
		mu.Lock()
		open++
		peak = max(peak, open)
		mu.Unlock()
		return &trackedConn{Conn: conn, closed: func() { mu.Lock(); open--; mu.Unlock() }}, nil
	}
	code, err := c.Upload(context.Background(), "", "existing.txt", strings.NewReader("replacement"))
	<-done
	mu.Lock()
	if peak != 1 || open != 0 {
		t.Errorf("cleanup connections: peak %d, open %d", peak, open)
	}
	mu.Unlock()
	if err == nil || code != 552 || strings.Contains(strings.Join(commands, ","), "DELE existing.txt") {
		t.Errorf("quota failure must preserve destination: %d, %v, commands %v", code, err, commands)
	}
}

type trackedConn struct {
	net.Conn
	once   sync.Once
	closed func()
}

func (c *trackedConn) Close() error {
	err := c.Conn.Close()
	c.once.Do(c.closed)
	return err
}
