// Package download fetches a file over HTTP into a temporary file on disk.
package download

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

// Client downloads from one source.
type Client struct {
	Prefix   string
	User     string
	Password string
	HTTP     *http.Client
	TempDir  string
}

// Result describes a finished download. The caller removes Path.
type Result struct {
	Path   string
	Host   string
	Status int
	Bytes  int64
	SHA256 string
}

// StatusError is a non-2xx answer.
type StatusError struct {
	Status int
	Host   string
}

func (e *StatusError) Error() string {
	return fmt.Sprintf("GET from %s: HTTP %d %s", e.Host, e.Status, http.StatusText(e.Status))
}

// New returns a client with the given timeout for the whole download.
func New(prefix, user, password, tempDir string, timeout time.Duration) *Client {
	return &Client{
		Prefix: prefix, User: user, Password: password, TempDir: tempDir,
		HTTP: &http.Client{Timeout: timeout},
	}
}

// URL returns the download URL for id. The id is path-escaped.
func (c *Client) URL(id string) (string, error) {
	if id == "" || id == "." || id == ".." {
		return "", fmt.Errorf("invalid source id %q", id)
	}
	u, err := url.Parse(c.Prefix)
	if err != nil || u.Host == "" {
		return "", fmt.Errorf("invalid source URL prefix")
	}
	if u.Path == "" {
		u.Path = "/"
	}
	escaped := u.EscapedPath() + url.PathEscape(id)
	u.Path += id
	u.RawPath = escaped
	return u.String(), nil
}

// Host returns the host of the prefix, without user information.
func (c *Client) Host() string {
	u, err := url.Parse(c.Prefix)
	if err != nil {
		return ""
	}
	return u.Host
}

// Fetch streams GET <prefix><id> into a new temp file. On error the temp file
// is removed and Result.Status holds the HTTP status if one was received.
func (c *Client) Fetch(ctx context.Context, id string) (Result, error) {
	res := Result{Host: c.Host()}
	u, err := c.URL(id)
	if err != nil {
		return res, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return res, err
	}
	if c.User != "" || c.Password != "" {
		req.SetBasicAuth(c.User, c.Password)
	}
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return res, fmt.Errorf("GET from %s: %w", res.Host, stripURL(err))
	}
	defer resp.Body.Close()
	res.Status = resp.StatusCode
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64*1024))
		return res, &StatusError{Status: resp.StatusCode, Host: res.Host}
	}
	if err := os.MkdirAll(c.TempDir, 0o750); err != nil {
		return res, fmt.Errorf("create temp dir: %w", err)
	}
	f, err := os.CreateTemp(c.TempDir, "download-*.tmp")
	if err != nil {
		return res, fmt.Errorf("create temp file: %w", err)
	}
	h := sha256.New()
	n, err := io.Copy(io.MultiWriter(f, h), resp.Body)
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		_ = os.Remove(f.Name())
		return res, fmt.Errorf("read body from %s: %w", res.Host, stripURL(err))
	}
	if resp.ContentLength >= 0 && n != resp.ContentLength {
		_ = os.Remove(f.Name())
		return res, fmt.Errorf("read body from %s: got %d of %d bytes", res.Host, n, resp.ContentLength)
	}
	res.Path, res.Bytes, res.SHA256 = f.Name(), n, hex.EncodeToString(h.Sum(nil))
	return res, nil
}

// Check sends HEAD <prefix>, or GET when HEAD is not allowed, and returns the
// status. It is used by the check command.
func (c *Client) Check(ctx context.Context) (int, error) {
	status := 0
	for _, m := range []string{http.MethodHead, http.MethodGet} {
		req, err := http.NewRequestWithContext(ctx, m, c.Prefix, nil)
		if err != nil {
			return 0, err
		}
		if c.User != "" || c.Password != "" {
			req.SetBasicAuth(c.User, c.Password)
		}
		resp, err := c.HTTP.Do(req)
		if err != nil {
			return 0, fmt.Errorf("%s %s: %w", m, c.Host(), stripURL(err))
		}
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64*1024))
		resp.Body.Close()
		status = resp.StatusCode
		if status != http.StatusMethodNotAllowed && status != http.StatusNotImplemented {
			break
		}
	}
	return status, nil
}

// stripURL drops the URL from *url.Error, so that error messages and audit
// records hold only the host.
func stripURL(err error) error {
	if ue, ok := err.(*url.Error); ok {
		msg := ue.Err.Error()
		if strings.Contains(msg, "@") {
			return fmt.Errorf("request failed")
		}
		return ue.Err
	}
	return err
}
