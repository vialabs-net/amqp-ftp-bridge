// Package ftpup uploads files to an FTP server: passive mode, binary type,
// missing folders created, existing files overwritten.
package ftpup

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/textproto"
	"path"
	"strings"
	"time"

	"github.com/jlaffaye/ftp"
)

// StatusTransferOK is the reply that closes a successful STOR.
const StatusTransferOK = ftp.StatusClosingDataConnection

// Client uploads to one FTP server.
type Client struct {
	Address  string
	User     string
	Password string
	Timeout  time.Duration
}

// Error is a failed FTP operation with the server reply code, if any.
type Error struct {
	Op   string
	Code int
	Err  error
}

func (e *Error) Error() string {
	if e.Code != 0 {
		return fmt.Sprintf("ftp %s: %d %s", e.Op, e.Code, replyText(e.Err))
	}
	return fmt.Sprintf("ftp %s: %v", e.Op, e.Err)
}

func (e *Error) Unwrap() error { return e.Err }

func replyText(err error) string {
	var te *textproto.Error
	if errors.As(err, &te) {
		return strings.TrimSpace(te.Msg)
	}
	return err.Error()
}

func wrap(op string, err error) error {
	if err == nil {
		return nil
	}
	e := &Error{Op: op, Err: err}
	var te *textproto.Error
	if errors.As(err, &te) {
		e.Code = te.Code
	}
	return e
}

// Join validates folder and name and returns the remote path. Backslashes in
// folder are read as separators. ".." is refused in both.
func Join(folder, name string) (string, error) {
	if name == "" || name == "." || name == ".." || strings.ContainsAny(name, `/\`) || strings.ContainsRune(name, 0) {
		return "", fmt.Errorf("invalid file name %q", name)
	}
	f := strings.ReplaceAll(folder, `\`, "/")
	for _, part := range strings.Split(f, "/") {
		if part == ".." {
			return "", fmt.Errorf("invalid folder %q", folder)
		}
	}
	if strings.ContainsRune(f, 0) {
		return "", fmt.Errorf("invalid folder %q", folder)
	}
	if f == "" {
		return name, nil
	}
	return path.Join(path.Clean(f), name), nil
}

func (c *Client) connect(ctx context.Context) (*ftp.ServerConn, error) {
	// The library's DialWithTimeout only bounds the TCP connection. Apply
	// deadlines and cancellation to both control and passive data sockets.
	dial := func(network, address string) (net.Conn, error) {
		d := net.Dialer{Timeout: c.Timeout}
		raw, err := d.DialContext(ctx, network, address)
		if err != nil {
			return nil, err
		}
		stop := context.AfterFunc(ctx, func() { _ = raw.Close() })
		return &timedConn{Conn: raw, timeout: c.Timeout, stop: stop}, nil
	}
	conn, err := ftp.Dial(c.Address, ftp.DialWithDialFunc(dial))
	if err != nil {
		return nil, wrap("connect", err)
	}
	if err := conn.Login(c.User, c.Password); err != nil {
		_ = conn.Quit()
		return nil, wrap("login", err)
	}
	return conn, nil
}

type timedConn struct {
	net.Conn
	timeout time.Duration
	stop    func() bool
}

func (c *timedConn) Read(b []byte) (int, error) {
	if c.timeout > 0 {
		if err := c.SetReadDeadline(time.Now().Add(c.timeout)); err != nil {
			return 0, err
		}
	}
	return c.Conn.Read(b)
}

func (c *timedConn) Write(b []byte) (int, error) {
	if c.timeout > 0 {
		if err := c.SetWriteDeadline(time.Now().Add(c.timeout)); err != nil {
			return 0, err
		}
	}
	return c.Conn.Write(b)
}

func (c *timedConn) Close() error {
	c.stop()
	return c.Conn.Close()
}

// Upload stores r at <folder>/<name> and returns the final reply code.
func (c *Client) Upload(ctx context.Context, folder, name string, r io.Reader) (int, error) {
	remote, err := Join(folder, name)
	if err != nil {
		return 0, err
	}
	conn, err := c.connect(ctx)
	if err != nil {
		return codeOf(err), err
	}
	defer func() { _ = conn.Quit() }()

	if dir := path.Dir(remote); dir != "." && dir != "/" {
		if err := mkdirAll(conn, dir); err != nil {
			return codeOf(err), err
		}
	}
	err = conn.Stor(remote, r)
	if err != nil {
		e := wrap("stor "+remote, err)
		return codeOf(e), e
	}
	return StatusTransferOK, nil
}

// mkdirAll creates every missing folder of dir. Relative paths stay relative
// to the login directory: the working directory is restored after each probe.
func mkdirAll(conn *ftp.ServerConn, dir string) error {
	home, err := conn.CurrentDir()
	if err != nil {
		return wrap("pwd", err)
	}
	cur := ""
	if strings.HasPrefix(dir, "/") {
		cur = "/"
	}
	for _, part := range strings.Split(strings.Trim(dir, "/"), "/") {
		if part == "" || part == "." {
			continue
		}
		cur = path.Join(cur, part)
		if conn.ChangeDir(cur) == nil {
			if err := conn.ChangeDir(home); err != nil {
				return wrap("cwd "+home, err)
			}
			continue
		}
		if err := conn.MakeDir(cur); err != nil {
			// Another client may have created it meanwhile.
			if conn.ChangeDir(cur) != nil {
				return wrap("mkdir "+cur, err)
			}
			if err := conn.ChangeDir(home); err != nil {
				return wrap("cwd "+home, err)
			}
		}
	}
	return nil
}

func codeOf(err error) int {
	var e *Error
	if errors.As(err, &e) {
		return e.Code
	}
	return 0
}

// Check connects, logs in and reads the working directory.
func (c *Client) Check(ctx context.Context) error {
	conn, err := c.connect(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Quit() }()
	if _, err := conn.CurrentDir(); err != nil {
		return wrap("pwd", err)
	}
	return nil
}
