package httpserver

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/vialabs-net/amqp-ftp-bridge/internal/audit"
	"github.com/vialabs-net/amqp-ftp-bridge/internal/creds"
	"github.com/vialabs-net/amqp-ftp-bridge/internal/ftpup"
	"github.com/vialabs-net/amqp-ftp-bridge/internal/pathpattern"
)

// fakeFTP stores files in memory, keyed by user and remote path.
type fakeFTP struct {
	mu    sync.Mutex
	files map[string]string
	// err, when set, is returned by every operation.
	err error
	// block, when set, makes uploads wait until it is closed.
	block   chan struct{}
	started chan struct{}
	// truncate makes downloads fail after this many bytes.
	truncate      int
	sizeless      bool
	sizeOffset    int64
	downloadBlock bool
}

func (f *fakeFTP) setErr(err error) { f.mu.Lock(); f.err = err; f.mu.Unlock() }

func (f *fakeFTP) getErr() error { f.mu.Lock(); defer f.mu.Unlock(); return f.err }

func (f *fakeFTP) get(key string) string { f.mu.Lock(); defer f.mu.Unlock(); return f.files[key] }

func (f *fakeFTP) count() int { f.mu.Lock(); defer f.mu.Unlock(); return len(f.files) }

func (o *countObs) snapshot() (peak, inUse, rejected int, requests map[string]int, ftpOK *bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	requests = map[string]int{}
	for k, v := range o.requests {
		requests[k] = v
	}
	return o.peak, o.inUse, o.rejected, requests, o.ftpOK
}

func (f *fakeFTP) client(t Target, slot int) FTPClient { return &fakeSession{f: f, user: t.User} }

type fakeSession struct {
	f    *fakeFTP
	user string
}

func (s *fakeSession) Upload(ctx context.Context, folder, name string, r io.Reader) (int, error) {
	if err := s.f.getErr(); err != nil {
		return ftpup.Code(err), err
	}
	if s.f.started != nil {
		s.f.started <- struct{}{}
	}
	if s.f.block != nil {
		select {
		case <-s.f.block:
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}
	b, err := io.ReadAll(r)
	if err != nil {
		return 0, err
	}
	p, _ := ftpup.Join(folder, name)
	s.f.mu.Lock()
	s.f.files[s.user+":"+p] = string(b)
	s.f.mu.Unlock()
	return 226, nil
}

func (s *fakeSession) Download(ctx context.Context, folder, name string) (Transfer, error) {
	if s.f.downloadBlock {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	if err := s.f.getErr(); err != nil {
		return nil, err
	}
	p, _ := ftpup.Join(folder, name)
	s.f.mu.Lock()
	body, ok := s.f.files[s.user+":"+p]
	sizeless, truncate := s.f.sizeless, s.f.truncate
	s.f.mu.Unlock()
	if !ok {
		return nil, &ftpup.Error{Op: "retr " + p, Code: 550, Err: errors.New("550 not found")}
	}
	size := int64(len(body)) + s.f.sizeOffset
	if sizeless {
		size = -1
	}
	var r io.Reader = strings.NewReader(body)
	if truncate > 0 {
		r = io.MultiReader(strings.NewReader(body[:truncate]), errReader{})
	}
	return &fakeTransfer{Reader: r, size: size}, nil
}

type errReader struct{}

func (errReader) Read([]byte) (int, error) { return 0, errors.New("data connection reset") }

type cancelEOFReader struct {
	io.Reader
	cancel context.CancelFunc
}

func (r cancelEOFReader) Read(p []byte) (int, error) {
	n, err := r.Reader.Read(p)
	if err == io.EOF {
		r.cancel()
	}
	return n, err
}

type fakeTransfer struct {
	io.Reader
	size int64
}

func (t *fakeTransfer) Size() int64         { return t.size }
func (t *fakeTransfer) Close() (int, error) { return 226, nil }

type memAudit struct {
	mu   sync.Mutex
	recs []audit.Record
}

func (m *memAudit) Write(r audit.Record) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.recs = append(m.recs, r)
	return nil
}

func (m *memAudit) steps(id string) string {
	m.mu.Lock()
	defer m.mu.Unlock()
	var s []string
	for _, r := range m.recs {
		if r.MsgID == id {
			s = append(s, r.Step)
		}
	}
	return strings.Join(s, ",")
}

func (m *memAudit) find(id, step string) audit.Record {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, r := range m.recs {
		if r.MsgID == id && r.Step == step {
			return r
		}
	}
	return audit.Record{}
}

type countObs struct {
	NopObserver
	mu       sync.Mutex
	inUse    int
	peak     int
	rejected int
	requests map[string]int
	ftpOK    *bool
}

func (o *countObs) FTPSessions(d int) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.inUse += d
	o.peak = max(o.peak, o.inUse)
}

func (o *countObs) FTPSessionRejected() { o.mu.Lock(); o.rejected++; o.mu.Unlock() }

func (o *countObs) HTTPRequest(route string, code int, _ time.Duration) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.requests == nil {
		o.requests = map[string]int{}
	}
	o.requests[route+" "+http.StatusText(code)]++
}

func (o *countObs) FTPResult(ok bool) { o.mu.Lock(); o.ftpOK = &ok; o.mu.Unlock() }

type fixture struct {
	srv   *Server
	ftp   *fakeFTP
	audit *memAudit
	obs   *countObs
	ts    *httptest.Server
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	f := &fixture{ftp: &fakeFTP{files: map[string]string{}}, audit: &memAudit{}, obs: &countObs{}}
	target := Target{Name: "main", Address: "ftp.example.internal:21", User: "default", Password: "x"}
	f.srv = &Server{
		Routes: []Route{
			{Name: "upload", Action: "upload", Method: "POST", Pattern: pathpattern.MustCompile("/api/upload/"),
				Folder: "header:X-Folder", FileName: "header:X-File-Name", Target: target},
			{Name: "download", Action: "download", Method: "GET", Pattern: pathpattern.MustCompile("/api/file/{disk}:{dir...}/{name}"),
				Folder: "path:dir", FileName: "path:name", Target: target, CredentialBy: "path:disk",
				Credentials: map[string]creds.Credential{"L": {User: "disk-l", Password: "y"}}},
			{Name: "query", Action: "upload", Method: "PUT", Pattern: pathpattern.MustCompile("/api/q"),
				Folder: "literal:fixed", FileName: "query:name", Target: target},
		},
		Users:          []creds.Credential{{User: "client", Password: "secret"}, {User: "other", Password: "pw2"}},
		TrustedProxies: []netip.Prefix{netip.MustParsePrefix("127.0.0.1/32")},
		MaxUploadBytes: 1024,
		TempDir:        t.TempDir(),
		Sessions:       NewSessions(2, f.obs),
		FTPWait:        50 * time.Millisecond,
		Audit:          f.audit,
		Log:            slog.New(slog.DiscardHandler),
		Obs:            f.obs,
		NewFTP:         f.ftp.client,
	}
	f.ts = httptest.NewServer(f.srv)
	t.Cleanup(f.ts.Close)
	return f
}

func (f *fixture) do(t *testing.T, method, path string, body io.Reader, hdr map[string]string) (*http.Response, string) {
	t.Helper()
	req, err := http.NewRequest(method, f.ts.URL+path, body)
	if err != nil {
		t.Fatal(err)
	}
	req.SetBasicAuth("client", "secret")
	for k, v := range hdr {
		if k == "Authorization" && v == "" {
			req.Header.Del(k)
			continue
		}
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp, string(b)
}

func TestUpload(t *testing.T) {
	f := newFixture(t)
	resp, body := f.do(t, "POST", "/api/upload/", strings.NewReader("hello"), map[string]string{
		"X-Folder": `in\2026`, "X-File-Name": "a.pdf", "X-Request-Id": "req-1", "X-Forwarded-For": "192.0.2.50, 127.0.0.1",
	})
	if resp.StatusCode != 200 || body != "" || resp.Header.Get("X-Request-Id") != "req-1" {
		t.Fatalf("upload = %d %q %v", resp.StatusCode, body, resp.Header)
	}
	if got := f.ftp.get("default:in/2026/a.pdf"); got != "hello" {
		t.Errorf("stored %q", got)
	}
	if s := f.audit.steps("req-1"); s != "received,uploaded,responded" {
		t.Errorf("steps = %s", s)
	}
	rec := f.audit.find("req-1", audit.StepReceived)
	if rec.Route != "upload" || rec.Method != "POST" || rec.ClientIP != "192.0.2.50" || *rec.BytesDeclared != 5 || rec.Queue != "" {
		t.Errorf("received = %+v", rec)
	}
	sum := sha256.Sum256([]byte("hello"))
	up := f.audit.find("req-1", audit.StepUploaded)
	if up.SHA256 != hex.EncodeToString(sum[:]) || *up.Bytes != 5 || up.FTPCode != 226 || up.Folder != `in\2026` || up.FileName != "a.pdf" {
		t.Errorf("uploaded = %+v", up)
	}
	if r := f.audit.find("req-1", audit.StepResponded); r.HTTPStatus != 200 || r.TotalMs == nil {
		t.Errorf("responded = %+v", r)
	}
	// Overwrite, with the other user, and a query value.
	resp, _ = f.do(t, "PUT", "/api/q?name=b.txt", strings.NewReader("v2"), map[string]string{"Authorization": ""})
	if resp.StatusCode != 401 {
		t.Errorf("no auth = %d", resp.StatusCode)
	}
	req, _ := http.NewRequest("PUT", f.ts.URL+"/api/q?name=b.txt", strings.NewReader("v2"))
	req.SetBasicAuth("other", "pw2")
	r2, err := http.DefaultClient.Do(req)
	if err != nil || r2.StatusCode != 200 {
		t.Fatalf("other user: %v %v", r2, err)
	}
	r2.Body.Close()
	if f.ftp.get("default:fixed/b.txt") != "v2" {
		t.Errorf("files = %d", f.ftp.count())
	}
	if peak, inUse, _, _, _ := f.obs.snapshot(); peak != 1 || inUse != 0 {
		t.Errorf("sessions peak %d in use %d", peak, inUse)
	}
}

func TestUploadErrors(t *testing.T) {
	f := newFixture(t)
	cases := []struct {
		name string
		hdr  map[string]string
		body io.Reader
		code int
		msg  string
	}{
		{"wrong password", map[string]string{"X-Folder": "a", "X-File-Name": "b", "Authorization": "Basic Y2xpZW50Om5v"}, nil, 401, "authentication required"},
		{"no auth", map[string]string{"Authorization": ""}, nil, 401, "authentication required"},
		{"missing header", map[string]string{"X-Folder": "a"}, nil, 400, "file name: header X-File-Name is missing"},
		{"missing folder", map[string]string{"X-File-Name": "b"}, nil, 400, "folder: header X-Folder is missing"},
		{"traversal", map[string]string{"X-Folder": "a/../../etc", "X-File-Name": "b"}, nil, 400, "invalid folder or file name"},
		{"backslash traversal", map[string]string{"X-Folder": `a\..\b`, "X-File-Name": "b"}, nil, 400, "invalid folder or file name"},
		{"name traversal", map[string]string{"X-Folder": "a", "X-File-Name": ".."}, nil, 400, "invalid folder or file name"},
		{"slash in name", map[string]string{"X-Folder": "a", "X-File-Name": "x/y"}, nil, 400, "invalid folder or file name"},
		{"too large", map[string]string{"X-Folder": "a", "X-File-Name": "b"}, bytes.NewReader(make([]byte, 2000)), 413, "upload too large"},
		{"too large chunked", map[string]string{"X-Folder": "a", "X-File-Name": "b"}, io.MultiReader(bytes.NewReader(make([]byte, 2000))), 413, "upload too large"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			body := c.body
			if body == nil {
				body = strings.NewReader("x")
			}
			resp, msg := f.do(t, "POST", "/api/upload/", body, c.hdr)
			if resp.StatusCode != c.code || strings.TrimSpace(msg) != c.msg || resp.Header.Get("Content-Type") != "text/plain; charset=utf-8" {
				t.Errorf("= %d %q", resp.StatusCode, msg)
			}
			if c.code == 401 && resp.Header.Get("WWW-Authenticate") == "" {
				t.Error("no WWW-Authenticate")
			}
			id := resp.Header.Get("X-Request-Id")
			if len(id) != 36 || f.audit.steps(id) != "received,failed,responded" {
				t.Errorf("id %q steps %s", id, f.audit.steps(id))
			}
		})
	}
	if f.ftp.count() != 0 {
		t.Errorf("files = %d", f.ftp.count())
	}
	f.srv.TempDir = "/proc/afb-not-writable"
	resp, _ := f.do(t, "POST", "/api/upload/", strings.NewReader("x"), map[string]string{"X-Folder": "a", "X-File-Name": "b"})
	if resp.StatusCode != 500 {
		t.Errorf("temp dir failure = %d", resp.StatusCode)
	}
}

func TestFTPErrors(t *testing.T) {
	f := newFixture(t)
	for _, c := range []struct {
		err  error
		msg  string
		code int
	}{
		{&ftpup.Error{Op: "connect", Err: errors.New("connection refused")}, "FTP connect failed", 502},
		{&ftpup.Error{Op: "login", Code: 530, Err: errors.New("530 Login incorrect")}, "FTP login failed (530)", 502},
		{&ftpup.Error{Op: "stor in/a.pdf", Code: 552, Err: errors.New("552 quota")}, "FTP stor failed (552)", 502},
		{errors.New("other"), "FTP upload failed", 502},
	} {
		f.ftp.setErr(c.err)
		resp, msg := f.do(t, "POST", "/api/upload/", strings.NewReader("x"), map[string]string{"X-Folder": "in", "X-File-Name": "a.pdf"})
		if resp.StatusCode != c.code || strings.TrimSpace(msg) != c.msg {
			t.Errorf("%v: %d %q", c.err, resp.StatusCode, msg)
		}
		if strings.Contains(msg, "in/a.pdf") || strings.Contains(msg, "incorrect") {
			t.Errorf("message leaks details: %q", msg)
		}
		rec := f.audit.find(resp.Header.Get("X-Request-Id"), audit.StepFailed)
		if rec.FailedStep != "upload" || rec.Error == "" || rec.FTPCode != ftpup.Code(c.err) {
			t.Errorf("failed = %+v", rec)
		}
		if _, _, _, _, ok := f.obs.snapshot(); ok == nil || *ok {
			t.Error("FTP failure not reported")
		}
	}
	f.ftp.setErr(&ftpup.Error{Op: "login", Code: 530, Err: errors.New("530")})
	resp, msg := f.do(t, "GET", "/api/file/L:/a/b.pdf", nil, nil)
	if resp.StatusCode != 502 || strings.TrimSpace(msg) != "FTP login failed (530)" {
		t.Errorf("download: %d %q", resp.StatusCode, msg)
	}
}

func TestDownload(t *testing.T) {
	f := newFixture(t)
	f.ftp.files["disk-l:/a/b/c.pdf"] = "%PDF-content"
	f.ftp.files["disk-l:a/c.pdf"] = "relative"
	resp, body := f.do(t, "GET", "/api/file/L:/a/b/c.pdf", nil, map[string]string{"X-Request-Id": "dl-1"})
	if resp.StatusCode != 200 || body != "%PDF-content" || resp.Header.Get("Content-Type") != "application/octet-stream" || resp.ContentLength != 12 {
		t.Fatalf("download = %d %q %v", resp.StatusCode, body, resp.Header)
	}
	if s := f.audit.steps("dl-1"); s != "received,downloaded,responded" {
		t.Errorf("steps = %s", s)
	}
	d := f.audit.find("dl-1", audit.StepDownloaded)
	if d.Folder != "/a/b" || d.FileName != "c.pdf" || *d.Bytes != 12 || d.FTPCode != 226 || d.SHA256 != "" {
		t.Errorf("downloaded = %+v", d)
	}
	if rec := f.audit.find("dl-1", audit.StepReceived); rec.BytesDeclared != nil {
		t.Errorf("received = %+v", rec)
	}
	f.ftp.mu.Lock()
	f.ftp.sizeless = true
	f.ftp.mu.Unlock()
	resp, body = f.do(t, "GET", "/api/file/l:a/c.pdf", nil, nil)
	if resp.StatusCode != 200 || body != "relative" {
		t.Errorf("relative, lower-case disk = %d %q", resp.StatusCode, body)
	}
	for path, want := range map[string]int{
		"/api/file/L:/a/missing.pdf": 404,
		"/api/file/Q:/a/b/c.pdf":     404,
		"/api/file/L:/a/../c.pdf":    400,
		"/api/file/L:/a/x%00y":       400,
		"/api/nothing":               404,
	} {
		resp, msg := f.do(t, "GET", path, nil, nil)
		if resp.StatusCode != want {
			t.Errorf("%s = %d %q", path, resp.StatusCode, msg)
		}
	}
	resp, _ = f.do(t, "DELETE", "/api/file/L:/a/b/c.pdf", nil, nil)
	if resp.StatusCode != 405 || resp.Header.Get("Allow") != "GET" {
		t.Errorf("method = %d %v", resp.StatusCode, resp.Header)
	}
	resp, body = f.do(t, "HEAD", "/api/file/L:/a/b/c.pdf", nil, nil)
	if resp.StatusCode != 200 || body != "" {
		t.Errorf("head = %d %q", resp.StatusCode, body)
	}
	if _, _, _, req, _ := f.obs.snapshot(); req["unmatched Not Found"] != 1 || req["download Not Found"] != 2 {
		t.Errorf("requests = %v", req)
	}
}

func TestDownloadInterrupted(t *testing.T) {
	f := newFixture(t)
	f.ftp.files["disk-l:/a/big.bin"] = strings.Repeat("x", 100)
	f.ftp.truncate = 10
	req, _ := http.NewRequest("GET", f.ts.URL+"/api/file/L:/a/big.bin", nil)
	req.SetBasicAuth("client", "secret")
	req.Header.Set("X-Request-Id", "cut-1")
	resp, err := http.DefaultClient.Do(req)
	if err == nil {
		_, err = io.ReadAll(resp.Body)
		resp.Body.Close()
	}
	if err == nil {
		t.Error("truncated download read without error")
	}
	deadline := time.Now().Add(5 * time.Second)
	for f.audit.steps("cut-1") != "received,failed,responded" && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if s := f.audit.steps("cut-1"); s != "received,failed,responded" {
		t.Errorf("steps = %s", s)
	}
	if r := f.audit.find("cut-1", audit.StepFailed); r.FailedStep != StepTransfer || *r.Bytes != 10 {
		t.Errorf("failed = %+v", r)
	}
}

func TestDownloadChangedSize(t *testing.T) {
	for _, offset := range []int64{-3072, 3072} {
		f := newFixture(t)
		body := strings.Repeat("x", 4096)
		f.ftp.files["disk-l:/a/file.bin"] = body
		f.ftp.sizeOffset = offset
		req, _ := http.NewRequest("GET", f.ts.URL+"/api/file/L:/a/file.bin", nil)
		req.SetBasicAuth("client", "secret")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		got, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil || string(got) != body {
			t.Errorf("stale SIZE offset %d: received %d bytes, err %v", offset, len(got), err)
		}
	}
}

func TestBufferedUploadWaitSurvivesDisconnect(t *testing.T) {
	f := newFixture(t)
	f.srv.Sessions = NewSessions(1, f.obs)
	_, release, err := f.srv.Sessions.Acquire(context.Background(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	f.srv.FTPWait = time.Second
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req := httptest.NewRequest("POST", "/api/upload/", cancelEOFReader{strings.NewReader("received body"), cancel}).WithContext(ctx)
	req.SetBasicAuth("client", "secret")
	req.Header.Set("X-Folder", "a")
	req.Header.Set("X-File-Name", "b")
	timer := time.AfterFunc(50*time.Millisecond, release)
	defer timer.Stop()
	w := httptest.NewRecorder()
	f.srv.ServeHTTP(w, req)
	if w.Code != 200 || f.ftp.get("default:a/b") != "received body" {
		t.Errorf("buffered upload = %d %q", w.Code, w.Body.String())
	}
}

func TestDownloadDeadlineReleasesSession(t *testing.T) {
	f := newFixture(t)
	f.ftp.downloadBlock = true
	f.srv.WriteTimeout = 30 * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	req := httptest.NewRequest("GET", "/api/file/L:/a/b", nil).WithContext(ctx)
	req.SetBasicAuth("client", "secret")
	w := httptest.NewRecorder()
	start := time.Now()
	f.srv.ServeHTTP(w, req)
	if w.Code != 502 || time.Since(start) > 500*time.Millisecond {
		t.Errorf("blocked download = %d after %v", w.Code, time.Since(start))
	}
	if _, inUse, _, _, _ := f.obs.snapshot(); inUse != 0 {
		t.Errorf("sessions still in use: %d", inUse)
	}
}

func TestUploadCapacityBeforeBuffering(t *testing.T) {
	f := newFixture(t)
	f.srv.uploadsOnce.Do(func() { f.srv.uploads = NewSessions(1, nil) })
	_, release, err := f.srv.uploads.Acquire(context.Background(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	req := httptest.NewRequest("POST", "/api/upload/", errReader{})
	req.SetBasicAuth("client", "secret")
	req.Header.Set("X-Folder", "a")
	req.Header.Set("X-File-Name", "b")
	w := httptest.NewRecorder()
	f.srv.ServeHTTP(w, req)
	if w.Code != 503 || w.Header().Get("Retry-After") == "" {
		t.Errorf("upload capacity = %d", w.Code)
	}
	release()
	resp, _ := f.do(t, "POST", "/api/upload/", strings.NewReader("accepted"), map[string]string{"X-Folder": "a", "X-File-Name": "b"})
	if resp.StatusCode != 200 {
		t.Errorf("released upload capacity = %d", resp.StatusCode)
	}
}

func TestSessionsBusy(t *testing.T) {
	f := newFixture(t)
	f.srv.Sessions = NewSessions(1, f.obs)
	f.ftp.block = make(chan struct{})
	f.ftp.started = make(chan struct{}, 1)
	done := make(chan int)
	go func() {
		req, _ := http.NewRequest("POST", f.ts.URL+"/api/upload/", strings.NewReader("slow"))
		req.SetBasicAuth("client", "secret")
		req.Header.Set("X-Folder", "a")
		req.Header.Set("X-File-Name", "b")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			done <- 0
			return
		}
		resp.Body.Close()
		done <- resp.StatusCode
	}()
	<-f.ftp.started
	resp, msg := f.do(t, "POST", "/api/upload/", strings.NewReader("x"), map[string]string{"X-Folder": "a", "X-File-Name": "c"})
	if resp.StatusCode != 503 || resp.Header.Get("Retry-After") != "1" || !strings.Contains(msg, "retry later") {
		t.Errorf("busy = %d %v %q", resp.StatusCode, resp.Header, msg)
	}
	if r := f.audit.find(resp.Header.Get("X-Request-Id"), audit.StepFailed); r.FailedStep != StepFTPWait {
		t.Errorf("failed = %+v", r)
	}
	close(f.ftp.block)
	if code := <-done; code != 200 {
		t.Errorf("first upload = %d", code)
	}
	if peak, _, rejected, _, _ := f.obs.snapshot(); rejected != 1 || peak != 1 {
		t.Errorf("rejected %d peak %d", rejected, peak)
	}
}

func TestSessions(t *testing.T) {
	s := NewSessions(0, nil)
	if s.Size() != 1 {
		t.Errorf("size = %d", s.Size())
	}
	slot, release, err := s.Acquire(context.Background(), time.Second)
	if err != nil || slot != 0 {
		t.Fatal(slot, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, err := s.Acquire(ctx, time.Second); !errors.Is(err, context.Canceled) {
		t.Errorf("cancelled: %v", err)
	}
	if _, _, err := s.Acquire(context.Background(), 0); !errors.Is(err, ErrBusy) {
		t.Errorf("busy: %v", err)
	}
	release()
	release() // idempotent
	if s.Size() != 1 || len(s.slots) != 1 {
		t.Errorf("slots = %d", len(s.slots))
	}
}

func TestClientIP(t *testing.T) {
	trusted := []netip.Prefix{netip.MustParsePrefix("192.0.2.10/32"), netip.MustParsePrefix("192.0.2.128/25")}
	for _, c := range []struct{ remote, xff, want string }{
		{"192.0.2.1:5000", "198.51.100.1", "192.0.2.1"},
		{"192.0.2.10:5000", "198.51.100.1", "198.51.100.1"},
		{"192.0.2.10:5000", "203.0.113.9, 198.51.100.1, 192.0.2.130", "198.51.100.1"},
		{"192.0.2.10:5000", "192.0.2.130", "192.0.2.130"},
		{"192.0.2.10:5000", "", "192.0.2.10"},
		{"192.0.2.10:5000", "garbage, 198.51.100.7", "198.51.100.7"},
		{"192.0.2.10:5000", "198.51.100.7, garbage", "192.0.2.10"},
		{"[::ffff:192.0.2.10]:5000", "198.51.100.2", "198.51.100.2"},
		{"pipe", "", "pipe"},
	} {
		r := httptest.NewRequest("GET", "/", nil)
		r.RemoteAddr = c.remote
		if c.xff != "" {
			r.Header.Set("X-Forwarded-For", c.xff)
		}
		if got := clientIP(r, trusted); got != c.want {
			t.Errorf("%s %q = %s, want %s", c.remote, c.xff, got, c.want)
		}
	}
}

func TestAuthenticate(t *testing.T) {
	users := []creds.Credential{{User: "a", Password: "1"}, {User: "b", Password: "2"}}
	for _, c := range []struct {
		user, pass string
		want       bool
	}{{"a", "1", true}, {"b", "2", true}, {"a", "2", false}, {"c", "1", false}, {"", "", false}} {
		r := httptest.NewRequest("GET", "/", nil)
		r.SetBasicAuth(c.user, c.pass)
		if authenticate(r, users) != c.want {
			t.Errorf("%s:%s", c.user, c.pass)
		}
	}
	if authenticate(httptest.NewRequest("GET", "/", nil), users) {
		t.Error("no header accepted")
	}
}

func TestRequestID(t *testing.T) {
	for in, keep := range map[string]bool{"abc-123": true, "": false, "has space": false, strings.Repeat("x", 129): false, "tab\t": false} {
		r := httptest.NewRequest("GET", "/", nil)
		r.Header.Set("X-Request-Id", in)
		if got := requestID(r); (got == in) != keep || len(got) == 0 {
			t.Errorf("%q -> %q", in, got)
		}
	}
}

func TestServeGracefulShutdown(t *testing.T) {
	f := newFixture(t)
	f.ftp.block = make(chan struct{})
	f.ftp.started = make(chan struct{}, 1)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	served := make(chan error, 1)
	go func() { served <- f.srv.Serve(ctx, ln, 5*time.Second) }()
	status := make(chan int, 1)
	go func() {
		req, _ := http.NewRequest("POST", "http://"+ln.Addr().String()+"/api/upload/", strings.NewReader("in flight"))
		req.SetBasicAuth("client", "secret")
		req.Header.Set("X-Folder", "g")
		req.Header.Set("X-File-Name", "f.txt")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			status <- 0
			return
		}
		resp.Body.Close()
		status <- resp.StatusCode
	}()
	<-f.ftp.started
	cancel()
	time.Sleep(100 * time.Millisecond)
	if _, err := net.DialTimeout("tcp", ln.Addr().String(), time.Second); err == nil {
		t.Error("listener still accepts during shutdown")
	}
	close(f.ftp.block)
	if code := <-status; code != 200 {
		t.Errorf("in-flight upload = %d", code)
	}
	if err := <-served; err != nil {
		t.Error(err)
	}
	if f.ftp.get("default:g/f.txt") != "in flight" {
		t.Errorf("files = %d", f.ftp.count())
	}
}

func TestServeShutdownTimeout(t *testing.T) {
	f := newFixture(t)
	f.ftp.block = make(chan struct{})
	f.ftp.started = make(chan struct{}, 1)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	served := make(chan error, 1)
	go func() { served <- f.srv.Serve(ctx, ln, 100*time.Millisecond) }()
	go func() {
		req, _ := http.NewRequest("POST", "http://"+ln.Addr().String()+"/api/upload/", strings.NewReader("stuck"))
		req.SetBasicAuth("client", "secret")
		req.Header.Set("X-Folder", "g")
		req.Header.Set("X-File-Name", "f.txt")
		req.Header.Set("X-Request-Id", "stuck-1")
		if resp, err := http.DefaultClient.Do(req); err == nil {
			resp.Body.Close()
		}
	}()
	<-f.ftp.started
	cancel()
	select {
	case err := <-served:
		if err != nil {
			t.Error(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Serve did not return after the grace period")
	}
	if r := f.audit.find("stuck-1", audit.StepFailed); r.FailedStep != StepUpload {
		t.Errorf("failed = %+v (steps %s)", r, f.audit.steps("stuck-1"))
	}
}

func TestServeListenerError(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ln.Close()
	s := &Server{}
	if err := s.Serve(context.Background(), ln, time.Second); err == nil {
		t.Error("closed listener: no error")
	}
}

func TestDefaultFTP(t *testing.T) {
	dir := t.TempDir()
	newFTP := DefaultFTP(dir, nil)
	a := newFTP(Target{Address: "127.0.0.1:1", User: "u"}, 0).(ftpAdapter)
	b := newFTP(Target{Address: "127.0.0.1:1", User: "u"}, 1).(ftpAdapter)
	if a.c.RecoveryDir == b.c.RecoveryDir || !strings.HasPrefix(a.c.RecoveryDir, dir) {
		t.Errorf("recovery dirs %q %q", a.c.RecoveryDir, b.c.RecoveryDir)
	}
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	addr := ln.Addr().String()
	ln.Close()
	c := newFTP(Target{Address: addr, Timeout: time.Second}, 0)
	if _, err := c.Download(context.Background(), "a", "b"); ftpup.Op(err) != "connect" {
		t.Errorf("download: %v", err)
	}
	if _, err := c.Upload(context.Background(), "a", "b", strings.NewReader("x")); ftpup.Op(err) != "connect" {
		t.Errorf("upload: %v", err)
	}
}
