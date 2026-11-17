package ftpup

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"strings"
	"testing"
	"time"
)

// fakeRetr serves one control session with SIZE and RETR over EPSV.
func fakeRetr(t *testing.T, files map[string]string, sizeOK bool) string {
	t.Helper()
	control, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	data, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { control.Close(); data.Close() })
	go func() {
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
			fields := strings.Fields(line)
			arg := strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(line), fields[0]))
			reply := "200 ok\r\n"
			switch fields[0] {
			case "USER":
				reply = "230 logged in\r\n"
			case "FEAT":
				reply = "211 no features\r\n"
			case "EPSV":
				reply = fmt.Sprintf("229 (|||%d|)\r\n", data.Addr().(*net.TCPAddr).Port)
			case "SIZE":
				body, ok := files[arg]
				if !ok || !sizeOK {
					reply = "550 no size\r\n"
				} else {
					reply = fmt.Sprintf("213 %d\r\n", len(body))
				}
			case "RETR":
				body, ok := files[arg]
				if !ok {
					reply = "550 not found\r\n"
					break
				}
				d, err := data.Accept()
				if err != nil {
					return
				}
				fmt.Fprint(conn, "150 sending\r\n")
				_, _ = io.WriteString(d, body)
				d.Close()
				reply = "226 done\r\n"
			case "QUIT":
				fmt.Fprint(conn, "221 bye\r\n")
				return
			}
			fmt.Fprint(conn, reply)
		}
	}()
	return control.Addr().String()
}

func TestDownload(t *testing.T) {
	for _, sizeOK := range []bool{true, false} {
		addr := fakeRetr(t, map[string]string{"/a/b/c.pdf": "contents"}, sizeOK)
		c := &Client{Address: addr, User: "u", Password: "p", Timeout: time.Second}
		d, err := c.Download(context.Background(), "/a/b", "c.pdf")
		if err != nil {
			t.Fatal(err)
		}
		if want := map[bool]int64{true: 8, false: -1}[sizeOK]; d.Size != want {
			t.Errorf("size = %d, want %d", d.Size, want)
		}
		b, err := io.ReadAll(d)
		if err != nil || string(b) != "contents" {
			t.Errorf("read %q %v", b, err)
		}
		if code, err := d.Close(); err != nil || code != 226 {
			t.Errorf("close = %d %v", code, err)
		}
	}
}

func TestDownloadMissing(t *testing.T) {
	addr := fakeRetr(t, map[string]string{}, true)
	c := &Client{Address: addr, User: "u", Password: "p", Timeout: time.Second}
	_, err := c.Download(context.Background(), "a", "missing.pdf")
	if Code(err) != StatusFileUnavailable || Op(err) != "retr" {
		t.Errorf("err = %v (code %d, op %q)", err, Code(err), Op(err))
	}
	if _, err := c.Download(context.Background(), "../a", "x"); err == nil || Op(err) != "" {
		t.Errorf("traversal: %v", err)
	}
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	closed := ln.Addr().String()
	ln.Close()
	_, err = (&Client{Address: closed, Timeout: time.Second}).Download(context.Background(), "a", "b")
	if Op(err) != "connect" || Code(err) != 0 {
		t.Errorf("connect: %v", err)
	}
}
