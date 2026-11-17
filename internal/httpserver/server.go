// Package httpserver serves synchronous HTTP routes that upload a request
// body to an FTP server, or stream a file from it, and answer with the result.
package httpserver

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/netip"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/vialabs-net/amqp-ftp-bridge/internal/audit"
	"github.com/vialabs-net/amqp-ftp-bridge/internal/config"
	"github.com/vialabs-net/amqp-ftp-bridge/internal/creds"
	"github.com/vialabs-net/amqp-ftp-bridge/internal/ftpup"
	"github.com/vialabs-net/amqp-ftp-bridge/internal/pathpattern"
)

// Steps reported in failed audit records.
const (
	StepAuth     = "auth"
	StepMapping  = "mapping"
	StepBody     = "body"
	StepFTPWait  = "ftp_wait"
	StepUpload   = "upload"
	StepDownload = "download"
	StepTransfer = "transfer"
)

// unmatchedRoute labels the metrics of requests that match no route.
const unmatchedRoute = "unmatched"

// Observer receives the HTTP and FTP session events that metrics are built
// from. metrics.Prometheus implements it.
type Observer interface {
	HTTPRequest(route string, code int, d time.Duration)
	HTTPInFlight(route string, delta int)
	FTPSessions(delta int)
	FTPSessionWait(d time.Duration)
	FTPSessionRejected()
	FTPResult(ok bool)
}

// NopObserver ignores every event.
type NopObserver struct{}

func (NopObserver) HTTPRequest(string, int, time.Duration) {}
func (NopObserver) HTTPInFlight(string, int)               {}
func (NopObserver) FTPSessions(int)                        {}
func (NopObserver) FTPSessionWait(time.Duration)           {}
func (NopObserver) FTPSessionRejected()                    {}
func (NopObserver) FTPResult(bool)                         {}

// AuditWriter appends audit records.
type AuditWriter interface {
	Write(audit.Record) error
}

// Transfer is an open FTP download.
type Transfer interface {
	io.Reader
	// Size is the file size, or -1 when unknown.
	Size() int64
	// Close ends the transfer and returns the final FTP reply code.
	Close() (int, error)
}

// FTPClient is one FTP session for one request.
type FTPClient interface {
	Upload(ctx context.Context, folder, name string, r io.Reader) (int, error)
	Download(ctx context.Context, folder, name string) (Transfer, error)
}

// Target is an FTP server with the login used by a request.
type Target struct {
	Name     string
	Address  string
	User     string
	Password string
	Timeout  time.Duration
}

// Route is one compiled HTTP route.
type Route struct {
	Name    string
	Action  string
	Method  string
	Pattern *pathpattern.Pattern
	// Folder and FileName are selectors: header:, path:, query: or literal:.
	Folder   string
	FileName string
	// Target holds the FTP server and its default login.
	Target Target
	// CredentialBy, when set, selects the FTP login from Credentials by its
	// value. An unknown value answers 404.
	CredentialBy string
	Credentials  map[string]creds.Credential
}

// Server handles the HTTP routes. Create it with its fields set, then call
// Serve.
type Server struct {
	Routes         []Route
	Users          []creds.Credential
	TrustedProxies []netip.Prefix
	MaxUploadBytes int64
	TempDir        string
	Sessions       *Sessions
	FTPWait        time.Duration
	Audit          AuditWriter
	Log            *slog.Logger
	Obs            Observer
	// NewFTP returns the client for one request. slot identifies the FTP
	// session, which no other request uses meanwhile. Default: ftpup.
	NewFTP func(t Target, slot int) FTPClient
	// DialFTP, when set, opens the FTP connections of the default NewFTP.
	DialFTP func(ctx context.Context, network, address string) (net.Conn, error)

	ReadHeaderTimeout time.Duration
	ReadTimeout       time.Duration
	WriteTimeout      time.Duration
	IdleTimeout       time.Duration

	// hard is cancelled when the shutdown grace period ends. Uploads use it
	// instead of the request context, so that a client that goes away after
	// sending its body does not interrupt the FTP transfer.
	hard     context.Context
	handlers sync.WaitGroup
}

func (s *Server) obs() Observer {
	if s.Obs == nil {
		return NopObserver{}
	}
	return s.Obs
}

// DefaultFTP returns an ftpup client whose recovery journal is private to
// the session slot, under tempDir.
func DefaultFTP(tempDir string, dial func(context.Context, string, string) (net.Conn, error)) func(Target, int) FTPClient {
	return func(t Target, slot int) FTPClient {
		key := sha256.Sum256([]byte(fmt.Sprintf("http\x00%s\x00%s\x00%d", t.Address, t.User, slot)))
		return ftpAdapter{&ftpup.Client{
			Address: t.Address, User: t.User, Password: t.Password, Timeout: t.Timeout, Dial: dial,
			RecoveryDir: filepath.Join(tempDir, "ftp-http-"+hex.EncodeToString(key[:8])),
		}}
	}
}

type ftpAdapter struct{ c *ftpup.Client }

func (a ftpAdapter) Upload(ctx context.Context, folder, name string, r io.Reader) (int, error) {
	return a.c.Upload(ctx, folder, name, r)
}

func (a ftpAdapter) Download(ctx context.Context, folder, name string) (Transfer, error) {
	d, err := a.c.Download(ctx, folder, name)
	if err != nil {
		return nil, err
	}
	return ftpTransfer{d}, nil
}

type ftpTransfer struct{ *ftpup.Download }

func (t ftpTransfer) Size() int64 { return t.Download.Size }

// Serve answers requests on ln until ctx is cancelled. It then stops
// accepting connections and waits up to grace for the requests in flight.
// When grace ends first, it cancels them and closes their connections. It
// returns nil after a shutdown, or the error that stopped the server.
func (s *Server) Serve(ctx context.Context, ln net.Listener, grace time.Duration) error {
	hard, cancelHard := context.WithCancel(context.WithoutCancel(ctx))
	defer cancelHard()
	s.hard = hard
	if s.NewFTP == nil {
		s.NewFTP = DefaultFTP(s.TempDir, s.DialFTP)
	}
	if s.Sessions == nil {
		s.Sessions = NewSessions(1, s.Obs)
	}
	srv := &http.Server{
		Handler:           s,
		ReadHeaderTimeout: s.ReadHeaderTimeout,
		ReadTimeout:       s.ReadTimeout,
		WriteTimeout:      s.WriteTimeout,
		IdleTimeout:       s.IdleTimeout,
		BaseContext:       func(net.Listener) context.Context { return hard },
		ErrorLog:          slog.NewLogLogger(s.log().Handler(), slog.LevelWarn),
	}
	errc := make(chan error, 1)
	go func() { errc <- srv.Serve(ln) }()
	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
	}
	sctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), grace)
	defer cancel()
	if err := srv.Shutdown(sctx); err != nil {
		s.log().Warn("http shutdown timeout: cancelling requests in flight")
		cancelHard()
		_ = srv.Close()
	}
	// Handlers still write their last audit records after a forced close.
	done := make(chan struct{})
	go func() { s.handlers.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		s.log().Warn("http requests did not stop after cancellation")
	}
	<-errc
	return nil
}

// exchange is one request in progress.
type exchange struct {
	route    *Route
	w        http.ResponseWriter
	r        *http.Request
	values   map[string]string
	msgID    string
	start    time.Time
	status   int
	folder   string
	fileName string
	target   Target
}

func (s *Server) audit(x *exchange, rec audit.Record) {
	rec.MsgID, rec.Route = x.msgID, x.route.Name
	if s.Audit == nil {
		return
	}
	if err := s.Audit.Write(rec); err != nil {
		s.log().Error("audit write failed", "route", x.route.Name, "step", rec.Step, "err", err)
	}
}

// ServeHTTP implements http.Handler.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.handlers.Add(1)
	defer s.handlers.Done()
	start := time.Now()
	route, values, allowed := s.match(r)
	if route == nil {
		code := http.StatusNotFound
		if len(allowed) > 0 {
			code = http.StatusMethodNotAllowed
			w.Header().Set("Allow", strings.Join(allowed, ", "))
		}
		plain(w, code, http.StatusText(code))
		s.obs().HTTPRequest(unmatchedRoute, code, time.Since(start))
		return
	}
	x := &exchange{route: route, w: w, r: r, values: values, msgID: requestID(r), start: start}
	w.Header().Set("X-Request-Id", x.msgID)
	o := s.obs()
	o.HTTPInFlight(route.Name, 1)
	rec := audit.Record{Step: audit.StepReceived, Method: r.Method, ClientIP: clientIP(r, s.TrustedProxies)}
	if route.Action == config.ActionUpload && r.ContentLength >= 0 {
		rec.BytesDeclared = audit.Ptr(r.ContentLength)
	}
	s.audit(x, rec)

	// A failed download stream aborts the connection with a panic, after
	// the records below are written.
	defer func() {
		total := time.Since(x.start)
		s.audit(x, audit.Record{Step: audit.StepResponded, HTTPStatus: x.status, TotalMs: audit.Ptr(total.Milliseconds())})
		o.HTTPRequest(route.Name, x.status, total)
		o.HTTPInFlight(route.Name, -1)
	}()
	if !s.prepare(x) {
		return
	}
	if route.Action == config.ActionUpload {
		s.upload(x)
	} else {
		s.download(x)
	}
}

// match returns the first route whose pattern and method match, with its
// path values. With no match, allowed lists the methods of the routes whose
// pattern matches.
func (s *Server) match(r *http.Request) (*Route, map[string]string, []string) {
	var allowed []string
	path := r.URL.EscapedPath()
	for i := range s.Routes {
		route := &s.Routes[i]
		values, ok := route.Pattern.Match(path)
		if !ok {
			continue
		}
		if route.Method == r.Method || (route.Method == http.MethodGet && r.Method == http.MethodHead) {
			return route, values, nil
		}
		allowed = append(allowed, route.Method)
	}
	return nil, nil, allowed
}

// prepare checks the credentials and resolves the folder, file name and FTP
// login. It answers the request and returns false on failure.
func (s *Server) prepare(x *exchange) bool {
	if !authenticate(x.r, s.Users) {
		x.w.Header().Set("WWW-Authenticate", `Basic realm="amqp-ftp-bridge", charset="UTF-8"`)
		s.fail(x, http.StatusUnauthorized, StepAuth, errors.New("missing or wrong credentials"), 0, "authentication required")
		return false
	}
	var err error
	if x.folder, err = x.value(x.route.Folder); err != nil {
		s.fail(x, http.StatusBadRequest, StepMapping, err, 0, "folder: "+err.Error())
		return false
	}
	if x.fileName, err = x.value(x.route.FileName); err != nil {
		s.fail(x, http.StatusBadRequest, StepMapping, err, 0, "file name: "+err.Error())
		return false
	}
	if _, err := ftpup.Join(x.folder, x.fileName); err != nil {
		s.fail(x, http.StatusBadRequest, StepMapping, err, 0, "invalid folder or file name")
		return false
	}
	x.target = x.route.Target
	if x.route.CredentialBy != "" {
		key, err := x.value(x.route.CredentialBy)
		if err != nil {
			s.fail(x, http.StatusBadRequest, StepMapping, err, 0, err.Error())
			return false
		}
		c, ok := lookup(x.route.Credentials, key)
		if !ok {
			s.fail(x, http.StatusNotFound, StepMapping, fmt.Errorf("no FTP credential for %q", key), 0, "not found")
			return false
		}
		x.target.User, x.target.Password = c.User, c.Password
	}
	return true
}

// lookup finds key exactly, then case-insensitively.
func lookup(m map[string]creds.Credential, key string) (creds.Credential, bool) {
	if c, ok := m[key]; ok {
		return c, true
	}
	for k, c := range m {
		if strings.EqualFold(k, key) {
			return c, true
		}
	}
	return creds.Credential{}, false
}

// value resolves a selector against the request.
func (x *exchange) value(selector string) (string, error) {
	kind, name, _ := strings.Cut(selector, ":")
	switch kind + ":" {
	case config.SelectLiteral:
		return name, nil
	case config.SelectHeader:
		v := x.r.Header.Values(name)
		if len(v) == 0 {
			return "", fmt.Errorf("header %s is missing", name)
		}
		return v[0], nil
	case config.SelectQuery:
		q := x.r.URL.Query()
		if !q.Has(name) {
			return "", fmt.Errorf("query parameter %s is missing", name)
		}
		return q.Get(name), nil
	case config.SelectPath:
		if v, ok := x.values[name]; ok {
			return v, nil
		}
		return "", fmt.Errorf("path value %s is missing", name)
	}
	return "", fmt.Errorf("invalid selector %q", selector)
}

// fail writes the failed record and answers with a short plain-text message.
func (s *Server) fail(x *exchange, status int, step string, err error, ftpCode int, public string) {
	s.audit(x, audit.Record{Step: audit.StepFailed, FailedStep: step, Error: err.Error(), FTPCode: ftpCode, Folder: x.folder, FileName: x.fileName})
	s.log().Info("http request failed", "route", x.route.Name, "msg_id", x.msgID, "status", status, "step", step, "err", err)
	x.status = status
	plain(x.w, status, public)
}

func plain(w http.ResponseWriter, status int, msg string) {
	h := w.Header()
	h.Set("Content-Type", "text/plain; charset=utf-8")
	h.Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(status)
	_, _ = io.WriteString(w, msg+"\n")
}

// ftpFailure answers an FTP error: 404 for a missing file, 502 otherwise.
func (s *Server) ftpFailure(x *exchange, step string, err error) {
	code := ftpup.Code(err)
	if step == StepDownload && code == ftpup.StatusFileUnavailable {
		s.obs().FTPResult(true)
		s.fail(x, http.StatusNotFound, step, err, code, "not found")
		return
	}
	s.obs().FTPResult(false)
	msg := "FTP " + step + " failed"
	if op := ftpup.Op(err); op != "" {
		msg = "FTP " + op + " failed"
	}
	if code != 0 {
		msg += " (" + strconv.Itoa(code) + ")"
	}
	s.fail(x, http.StatusBadGateway, step, err, code, msg)
}

// acquire takes an FTP session or answers 503.
func (s *Server) acquire(x *exchange) (int, func(), bool) {
	slot, release, err := s.Sessions.Acquire(x.r.Context(), s.FTPWait)
	if err != nil {
		if errors.Is(err, ErrBusy) {
			x.w.Header().Set("Retry-After", strconv.Itoa(max(1, int(s.FTPWait.Seconds()))))
		}
		s.fail(x, http.StatusServiceUnavailable, StepFTPWait, err, 0, "no FTP session available, retry later")
		return 0, nil, false
	}
	return slot, release, true
}

func (s *Server) upload(x *exchange) {
	if x.r.ContentLength > s.MaxUploadBytes {
		s.fail(x, http.StatusRequestEntityTooLarge, StepBody, fmt.Errorf("declared %d bytes, limit %d", x.r.ContentLength, s.MaxUploadBytes), 0, "upload too large")
		return
	}
	// Buffer to disk first, so that a slow client never holds an FTP session.
	f, sum, n, err := s.buffer(x)
	if f != nil {
		defer func() { f.Close(); _ = os.Remove(f.Name()) }()
	}
	if err != nil {
		var tooLarge *http.MaxBytesError
		switch {
		case errors.As(err, &tooLarge):
			s.fail(x, http.StatusRequestEntityTooLarge, StepBody, err, 0, "upload too large")
		case f == nil:
			s.fail(x, http.StatusInternalServerError, StepBody, err, 0, "internal error")
		default:
			s.fail(x, http.StatusBadRequest, StepBody, err, 0, "request body could not be read")
		}
		return
	}
	slot, release, ok := s.acquire(x)
	if !ok {
		return
	}
	defer release()
	start := time.Now()
	code, err := s.NewFTP(x.target, slot).Upload(s.hardCtx(), x.folder, x.fileName, f)
	if err != nil {
		s.ftpFailure(x, StepUpload, err)
		return
	}
	s.obs().FTPResult(true)
	s.audit(x, audit.Record{
		Step: audit.StepUploaded, Folder: x.folder, FileName: x.fileName, FTPCode: code,
		Bytes: audit.Ptr(n), SHA256: sum, Ms: audit.Ptr(time.Since(start).Milliseconds()),
	})
	x.status = http.StatusOK
	x.w.Header().Set("Content-Length", "0")
	x.w.WriteHeader(http.StatusOK)
}

// buffer copies the request body to a temporary file, rewound for reading.
// The file is returned whenever it was created.
func (s *Server) buffer(x *exchange) (*os.File, string, int64, error) {
	if err := os.MkdirAll(s.TempDir, 0o750); err != nil {
		return nil, "", 0, err
	}
	f, err := os.CreateTemp(s.TempDir, "http-*.tmp")
	if err != nil {
		return nil, "", 0, err
	}
	h := sha256.New()
	body := http.MaxBytesReader(x.w, x.r.Body, s.MaxUploadBytes)
	n, err := io.Copy(io.MultiWriter(f, h), body)
	if err == nil {
		_, err = f.Seek(0, io.SeekStart)
	}
	return f, hex.EncodeToString(h.Sum(nil)), n, err
}

func (s *Server) download(x *exchange) {
	slot, release, ok := s.acquire(x)
	if !ok {
		return
	}
	defer release()
	start := time.Now()
	t, err := s.NewFTP(x.target, slot).Download(x.r.Context(), x.folder, x.fileName)
	if err != nil {
		s.ftpFailure(x, StepDownload, err)
		return
	}
	h := x.w.Header()
	h.Set("Content-Type", "application/octet-stream")
	h.Set("X-Content-Type-Options", "nosniff")
	if size := t.Size(); size >= 0 {
		h.Set("Content-Length", strconv.FormatInt(size, 10))
	}
	x.status = http.StatusOK
	x.w.WriteHeader(http.StatusOK)
	var n int64
	var copyErr error
	if x.r.Method != http.MethodHead {
		n, copyErr = io.Copy(x.w, t)
	}
	code, closeErr := t.Close()
	if copyErr == nil {
		copyErr = closeErr
	}
	if copyErr != nil && x.r.Method != http.MethodHead {
		s.obs().FTPResult(x.r.Context().Err() != nil)
		s.audit(x, audit.Record{Step: audit.StepFailed, FailedStep: StepTransfer, Error: copyErr.Error(), FTPCode: code, Folder: x.folder, FileName: x.fileName, Bytes: audit.Ptr(n)})
		s.log().Info("http download interrupted", "route", x.route.Name, "msg_id", x.msgID, "bytes", n, "err", copyErr)
		// Abort the connection so that the client cannot take a truncated
		// body for a complete one. The deferred records are still written.
		defer panic(http.ErrAbortHandler)
		return
	}
	s.obs().FTPResult(true)
	s.audit(x, audit.Record{
		Step: audit.StepDownloaded, Folder: x.folder, FileName: x.fileName, FTPCode: code,
		Bytes: audit.Ptr(n), Ms: audit.Ptr(time.Since(start).Milliseconds()),
	})
}

// requestID returns X-Request-Id when it is short printable ASCII, else a
// new UUID.
func requestID(r *http.Request) string {
	id := r.Header.Get("X-Request-Id")
	ok := id != "" && len(id) <= 128
	for i := 0; ok && i < len(id); i++ {
		ok = id[i] > ' ' && id[i] < 0x7f
	}
	if ok {
		return id
	}
	var b [16]byte
	_, _ = rand.Read(b[:])
	b[6] = b[6]&0x0f | 0x40
	b[8] = b[8]&0x3f | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

func (s *Server) log() *slog.Logger {
	if s.Log == nil {
		return slog.New(slog.DiscardHandler)
	}
	return s.Log
}

func (s *Server) hardCtx() context.Context {
	if s.hard == nil {
		return context.Background()
	}
	return s.hard
}
