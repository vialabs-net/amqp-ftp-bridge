// Package ftpup uploads files to an FTP server: passive mode, binary type,
// missing folders created, existing files overwritten.
package ftpup

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"net"
	"net/textproto"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
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
	// RecoveryDir is private to one route, under the service's locked temp dir.
	RecoveryDir string
	// Dial, when set, opens the control and data connections instead of
	// net.Dialer. Tests use it to count sessions.
	Dial func(ctx context.Context, network, address string) (net.Conn, error)
	mu   sync.Mutex
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
		dialCtx := ctx
		if c.Timeout > 0 {
			var cancel context.CancelFunc
			dialCtx, cancel = context.WithTimeout(ctx, c.Timeout)
			defer cancel()
		}
		dialer := c.Dial
		if dialer == nil {
			dialer = (&net.Dialer{}).DialContext
		}
		raw, err := dialer(dialCtx, network, address)
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
func (c *Client) Upload(ctx context.Context, folder, name string, r io.Reader) (code int, resultErr error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	remote, err := Join(folder, name)
	if err != nil {
		return 0, err
	}
	conn, err := c.connect(ctx)
	if err != nil {
		return codeOf(err), err
	}
	defer func() {
		if conn != nil {
			_ = conn.Quit()
		}
	}()
	if err := c.recover(conn); err != nil {
		return 0, err
	}

	if dir := path.Dir(remote); dir != "." && dir != "/" {
		if err := mkdirAll(conn, dir); err != nil {
			return codeOf(err), err
		}
	}
	temporary := path.Join(path.Dir(remote), ".afb-"+rand.Text()+".tmp")
	journal := ""
	if c.RecoveryDir != "" {
		journal = filepath.Join(c.RecoveryDir, path.Base(temporary)+".journal")
		if err := os.WriteFile(journal, []byte(temporary), 0o600); err != nil {
			return 0, err
		}
	}
	committed := false
	defer func() {
		if committed {
			if journal != "" {
				_ = os.Remove(journal)
			}
			return
		}
		// The transfer connection can already be closed by cancellation or EOF.
		// Close it before reconnecting so cleanup still consumes just one session.
		_ = conn.Quit()
		conn = nil
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		cleanup, err := c.connect(cleanupCtx)
		if err == nil {
			err = cleanup.Delete(temporary)
			_ = cleanup.Quit()
		}
		if err == nil {
			if journal != "" {
				_ = os.Remove(journal)
			}
		} else {
			resultErr = errors.Join(resultErr, fmt.Errorf("remote temporary cleanup pending: %w", err))
		}
	}()
	err = conn.Stor(temporary, r)
	if err != nil {
		e := wrap("stor "+remote, err)
		return codeOf(e), e
	}
	if err := conn.Rename(temporary, remote); err != nil {
		e := wrap("rename "+remote, err)
		return codeOf(e), e
	}
	committed = true
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

// A journal is written before STOR. The next upload reclaims only this route's
// recorded names, including after process death or a failed cleanup connection.
func (c *Client) recover(conn *ftp.ServerConn) error {
	if c.RecoveryDir == "" {
		return nil
	}
	if err := os.MkdirAll(c.RecoveryDir, 0o700); err != nil {
		return err
	}
	entries, err := os.ReadDir(c.RecoveryDir)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if !entry.Type().IsRegular() || !strings.HasPrefix(entry.Name(), ".afb-") || !strings.HasSuffix(entry.Name(), ".journal") {
			continue
		}
		journal := filepath.Join(c.RecoveryDir, entry.Name())
		remote, err := os.ReadFile(journal)
		if err != nil {
			return err
		}
		if path.Base(string(remote))+".journal" != entry.Name() {
			return errors.New("invalid FTP recovery journal")
		}
		if err := conn.Delete(string(remote)); err != nil {
			var te *textproto.Error
			if !errors.As(err, &te) || te.Code != 550 {
				return wrap("recover temporary", err)
			}
		}
		if err := os.Remove(journal); err != nil {
			return err
		}
	}
	return nil
}

// StatusFileUnavailable is the reply to RETR or SIZE for a missing file.
const StatusFileUnavailable = ftp.StatusFileUnavailable

// Code returns the FTP reply code carried by err, or 0.
func Code(err error) int { return codeOf(err) }

// Op returns the failed operation of err ("connect", "login", "retr", ...),
// without its path, or "" when err is not an FTP error.
func Op(err error) string {
	var e *Error
	if !errors.As(err, &e) {
		return ""
	}
	op, _, _ := strings.Cut(e.Op, " ")
	return op
}

// Download is an open RETR transfer. Read the file from it, then Close.
type Download struct {
	// Size is the file size reported by SIZE, or -1 when unknown.
	Size int64
	conn *ftp.ServerConn
	resp *ftp.Response
}

// Read reads the file contents.
func (d *Download) Read(p []byte) (int, error) { return d.resp.Read(p) }

// Close ends the transfer and the session. It returns the final reply code
// (226) when the server confirmed a complete transfer.
func (d *Download) Close() (int, error) {
	err := wrap("retr", d.resp.Close())
	_ = d.conn.Quit()
	if err != nil {
		return codeOf(err), err
	}
	return StatusTransferOK, nil
}

// Download opens <folder>/<name> on the server. The session stays open until
// the returned Download is closed. Cancelling ctx aborts the transfer.
func (c *Client) Download(ctx context.Context, folder, name string) (*Download, error) {
	remote, err := Join(folder, name)
	if err != nil {
		return nil, err
	}
	conn, err := c.connect(ctx)
	if err != nil {
		return nil, err
	}
	size, err := conn.FileSize(remote)
	if err != nil {
		size = -1
	}
	resp, err := conn.Retr(remote)
	if err != nil {
		_ = conn.Quit()
		return nil, wrap("retr "+remote, err)
	}
	return &Download{Size: size, conn: conn, resp: resp}, nil
}
