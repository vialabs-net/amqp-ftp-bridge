//go:build integration

package cli

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jlaffaye/ftp"

	"github.com/vialabs-net/amqp-ftp-bridge/internal/audit"
)

const (
	clientUser = "http-client"
	clientPass = "http-secret"
)

type httpSpec struct {
	ftpAddr     string
	metricsAddr string
	sessions    int
	ftpWait     string
	maxUpload   string
	shutdown    string
	// amqp, when set, is prepended: a configuration with queue routes too.
	amqp string
}

// writeHTTPTestConfig writes an HTTP-only configuration (no amqp section)
// and returns its path, log directory and HTTP base URL.
func writeHTTPTestConfig(t *testing.T, s httpSpec) (cfgPath, logDir, base string) {
	t.Helper()
	t.Setenv("AFB_CRED_HTTP_CLIENT", clientUser+":"+clientPass)
	dir := t.TempDir()
	listen := fmt.Sprintf("127.0.0.1:%d", freePort(t))
	def := func(v, d string) string {
		if v == "" {
			return d
		}
		return v
	}
	var b strings.Builder
	if s.amqp != "" {
		b.WriteString(s.amqp)
	} else {
		fmt.Fprintf(&b, "data_dir: %s\nshutdown_timeout: %s\n", dir, def(s.shutdown, "30s"))
		fmt.Fprintf(&b, "ftp:\n  - name: main\n    address: %s\n    credential: ftp\n    timeout: 10s\n", s.ftpAddr)
	}
	if s.metricsAddr != "" {
		fmt.Fprintf(&b, "metrics:\n  listen: %s\n", s.metricsAddr)
	}
	fmt.Fprintf(&b, `http:
  listen: %s
  credentials: [http-client]
  max_ftp_sessions: %d
  ftp_wait: %s
  max_upload_bytes: %s
  routes:
    - name: upload
      action: upload
      path: /api/upload/
      ftp: main
      folder: header:X-Folder
      file_name: header:X-File-Name
    - name: download
      action: download
      path: "/api/file/{disk}:{dir...}/{name}"
      ftp: main
      folder: path:dir
      file_name: path:name
      ftp_credential_by: path:disk
      ftp_credentials: { L: ftp, W: ftp_wrong }
`, listen, max(s.sessions, 1), def(s.ftpWait, "30s"), def(s.maxUpload, "8MiB"))
	if s.amqp != "" {
		// The AMQP helper already wrote data_dir: reuse its directory.
		for _, line := range strings.Split(s.amqp, "\n") {
			if d, ok := strings.CutPrefix(line, "data_dir: "); ok {
				dir = d
			}
		}
	}
	cfgPath = filepath.Join(dir, "http-config.yaml")
	if err := os.WriteFile(cfgPath, []byte(b.String()), 0o600); err != nil {
		t.Fatal(err)
	}
	return cfgPath, filepath.Join(dir, "logs"), "http://" + listen
}

func waitHTTP(t *testing.T, base string) {
	t.Helper()
	for i := 0; i < 200; i++ {
		if c, err := net.Dial("tcp", strings.TrimPrefix(base, "http://")); err == nil {
			c.Close()
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("%s never listened", base)
}

func httpUpload(t *testing.T, base, folder, name string, body []byte, user, pass string) (*http.Response, string) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, base+"/api/upload/", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.SetBasicAuth(user, pass)
	req.Header.Set("X-Folder", folder)
	req.Header.Set("X-File-Name", name)
	return doHTTP(t, req)
}

func httpGet(t *testing.T, url, id string) (*http.Response, string) {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	req.SetBasicAuth(clientUser, clientPass)
	if id != "" {
		req.Header.Set("X-Request-Id", id)
	}
	return doHTTP(t, req)
}

func doHTTP(t *testing.T, req *http.Request) (*http.Response, string) {
	t.Helper()
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp, string(b)
}

func ftpPwd(t *testing.T, addr string) string {
	t.Helper()
	c, err := ftp.Dial(addr, ftp.DialWithTimeout(10*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Quit()
	if err := c.Login(ftpUser, ftpPass); err != nil {
		t.Fatal(err)
	}
	dir, err := c.CurrentDir()
	if err != nil {
		t.Fatal(err)
	}
	return strings.TrimSuffix(dir, "/")
}

func TestIntegrationHTTPUploadDownload(t *testing.T) {
	_, ftpAddr := env(t)
	metricsAddr := fmt.Sprintf("127.0.0.1:%d", freePort(t))
	cfg, logDir, base := writeHTTPTestConfig(t, httpSpec{ftpAddr: ftpAddr, metricsAddr: metricsAddr, sessions: 2, maxUpload: "2MiB"})
	_, stop := startBridge(t, cfg)
	waitHTTP(t, base)

	folder := "http/" + strings.ReplaceAll(uniq("ud"), ".", "-") + "/deep"
	content := bytes.Repeat([]byte("%PDF-1.7 http "), 100000) // 1.4 MB
	resp, body := httpUpload(t, base, folder, "r.pdf", content, clientUser, clientPass)
	if resp.StatusCode != 200 || body != "" {
		t.Fatalf("upload = %d %q", resp.StatusCode, body)
	}
	id := resp.Header.Get("X-Request-Id")
	if got := ftpRead(t, ftpAddr, folder+"/r.pdf"); got != string(content) {
		t.Errorf("uploaded %d bytes, want %d", len(got), len(content))
	}
	if s := steps(logDir, id); s != "received,uploaded,responded" {
		t.Errorf("steps = %s", s)
	}
	sum := sha256.Sum256(content)
	for _, r := range readAudit(logDir) {
		if r.MsgID == id && r.Step == audit.StepUploaded && (r.SHA256 != hex.EncodeToString(sum[:]) || *r.Bytes != int64(len(content)) || r.Route != "upload" || r.Queue != "") {
			t.Errorf("uploaded = %+v", r)
		}
	}

	// Overwrite.
	resp, _ = httpUpload(t, base, folder, "r.pdf", []byte("second version"), clientUser, clientPass)
	if resp.StatusCode != 200 || ftpRead(t, ftpAddr, folder+"/r.pdf") != "second version" {
		t.Errorf("overwrite = %d", resp.StatusCode)
	}

	// Download, absolute and relative to the login directory.
	abs := ftpPwd(t, ftpAddr) + "/" + folder
	resp, got := httpGet(t, base+"/api/file/L:"+abs+"/r.pdf", "dl-abs")
	if resp.StatusCode != 200 || got != "second version" || resp.ContentLength != int64(len("second version")) ||
		resp.Header.Get("Content-Type") != "application/octet-stream" || resp.Header.Get("X-Request-Id") != "dl-abs" {
		t.Errorf("download = %d %q %v", resp.StatusCode, got, resp.Header)
	}
	if s := steps(logDir, "dl-abs"); s != "received,downloaded,responded" {
		t.Errorf("download steps = %s", s)
	}
	resp, got = httpGet(t, base+"/api/file/L:"+folder+"/r.pdf", "")
	if resp.StatusCode != 200 || got != "second version" {
		t.Errorf("relative download = %d %q", resp.StatusCode, got)
	}

	// Not found, disk selection, auth and size limit.
	for _, c := range []struct {
		path string
		code int
	}{
		{"/api/file/L:" + folder + "/missing.pdf", 404},
		{"/api/file/Q:" + folder + "/r.pdf", 404},
		{"/api/file/W:" + folder + "/r.pdf", 502},
		{"/api/file/L:" + folder + "/../r.pdf", 400},
	} {
		resp, msg := httpGet(t, base+c.path, "")
		if resp.StatusCode != c.code {
			t.Errorf("%s = %d %q", c.path, resp.StatusCode, msg)
		}
		if c.code == 502 && strings.TrimSpace(msg) != "FTP login failed (530)" {
			t.Errorf("wrong disk credential: %q", msg)
		}
	}
	resp, _ = httpUpload(t, base, folder, "x.pdf", []byte("x"), clientUser, "wrong")
	if resp.StatusCode != 401 || resp.Header.Get("WWW-Authenticate") == "" {
		t.Errorf("wrong auth = %d", resp.StatusCode)
	}
	resp, msg := httpUpload(t, base, folder, "big.pdf", make([]byte, 3<<20), clientUser, clientPass)
	if resp.StatusCode != 413 {
		t.Errorf("too large = %d %q", resp.StatusCode, msg)
	}

	mresp, err := http.Get("http://" + metricsAddr + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	mb, _ := io.ReadAll(mresp.Body)
	mresp.Body.Close()
	for _, want := range []string{
		`bridge_http_requests_total{code="200",route="upload"} 2`,
		`bridge_http_requests_total{code="404",route="download"} 2`,
		`bridge_http_requests_total{code="413",route="upload"} 1`,
		"bridge_ftp_sessions_in_use 0",
	} {
		if !strings.Contains(string(mb), want) {
			t.Errorf("metrics lack %s", want)
		}
	}
	rresp, err := http.Get("http://" + metricsAddr + "/readyz")
	if err != nil {
		t.Fatal(err)
	}
	rresp.Body.Close()
	if err := stop(); err != nil {
		t.Fatal(err)
	}
	raw, _ := os.ReadFile(filepath.Join(logDir, audit.FileName(time.Now())))
	for _, secret := range []string{clientPass, ftpPass, "Authorization", "%PDF", "second version"} {
		if bytes.Contains(raw, []byte(secret)) {
			t.Errorf("audit log contains %q", secret)
		}
	}
}

// ctlProxy forwards FTP control connections and calls onStor before
// forwarding each STOR.
type ctlProxy struct {
	ln     net.Listener
	target string
	onStor func()
}

func newCtlProxy(t *testing.T, target string, onStor func()) *ctlProxy {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := &ctlProxy{ln: ln, target: target, onStor: onStor}
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go p.handle(c)
		}
	}()
	return p
}

func (p *ctlProxy) addr() string { return p.ln.Addr().String() }

type countedConn struct {
	net.Conn
	once sync.Once
	done func()
}

func (c *countedConn) Close() error {
	err := c.Conn.Close()
	c.once.Do(c.done)
	return err
}

func (p *ctlProxy) handle(client net.Conn) {
	defer client.Close()
	up, err := net.Dial("tcp", p.target)
	if err != nil {
		return
	}
	defer up.Close()
	go func() { _, _ = io.Copy(client, up); client.Close() }()
	r := bufio.NewReader(client)
	for {
		line, err := r.ReadString('\n')
		if err != nil {
			return
		}
		if p.onStor != nil && strings.HasPrefix(strings.ToUpper(line), "STOR ") {
			p.onStor()
		}
		if _, err := io.WriteString(up, line); err != nil {
			return
		}
	}
}

func TestIntegrationHTTPParallelSessions(t *testing.T) {
	_, ftpAddr := env(t)
	// The proxy slows each STOR so that requests overlap. The dialer counts
	// open control connections in process: a connection is counted from
	// before it is opened until after it is closed.
	proxy := newCtlProxy(t, ftpAddr, func() { time.Sleep(20 * time.Millisecond) })
	var mu sync.Mutex
	open, peak := 0, 0
	old := ftpDial
	t.Cleanup(func() { ftpDial = old })
	ftpDial = func(ctx context.Context, network, address string) (net.Conn, error) {
		if address != proxy.addr() {
			return (&net.Dialer{}).DialContext(ctx, network, address) // passive data connection
		}
		mu.Lock()
		open++
		peak = max(peak, open)
		mu.Unlock()
		c, err := (&net.Dialer{}).DialContext(ctx, network, address)
		if err != nil {
			mu.Lock()
			open--
			mu.Unlock()
			return nil, err
		}
		return &countedConn{Conn: c, done: func() { mu.Lock(); open--; mu.Unlock() }}, nil
	}
	metricsAddr := fmt.Sprintf("127.0.0.1:%d", freePort(t))
	cfg, _, base := writeHTTPTestConfig(t, httpSpec{ftpAddr: proxy.addr(), metricsAddr: metricsAddr, sessions: 2, ftpWait: "2m"})
	startBridge(t, cfg)
	waitHTTP(t, base)

	folder := "http/" + strings.ReplaceAll(uniq("par"), ".", "-")
	const n = 20
	codes := make(chan int, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			req, _ := http.NewRequest(http.MethodPost, base+"/api/upload/", strings.NewReader(fmt.Sprintf("file %d", i)))
			req.SetBasicAuth(clientUser, clientPass)
			req.Header.Set("X-Folder", folder)
			req.Header.Set("X-File-Name", fmt.Sprintf("f%02d.txt", i))
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				codes <- 0
				return
			}
			resp.Body.Close()
			codes <- resp.StatusCode
		}()
	}
	wg.Wait()
	close(codes)
	for c := range codes {
		if c != 200 {
			t.Errorf("upload status %d", c)
		}
	}
	mu.Lock()
	if peak > 2 || peak == 0 || open != 0 {
		t.Errorf("peak FTP sessions = %d (open %d), want at most 2", peak, open)
	}
	mu.Unlock()
	for i := 0; i < n; i++ {
		if got := ftpRead(t, ftpAddr, fmt.Sprintf("%s/f%02d.txt", folder, i)); got != fmt.Sprintf("file %d", i) {
			t.Errorf("f%02d = %q", i, got)
		}
	}
	resp, err := http.Get("http://" + metricsAddr + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	for _, want := range []string{"bridge_ftp_sessions_in_use 0", "bridge_ftp_session_wait_seconds_count 20", "bridge_ftp_session_rejected_total 0"} {
		if !strings.Contains(string(b), want) {
			t.Errorf("metrics lack %s", want)
		}
	}
}

func TestIntegrationHTTPWaitExceeded(t *testing.T) {
	_, ftpAddr := env(t)
	hold := make(chan struct{})
	stored := make(chan struct{}, 1)
	proxy := newCtlProxy(t, ftpAddr, func() {
		select {
		case stored <- struct{}{}:
		default:
		}
		<-hold
	})
	t.Cleanup(func() { close(hold) })
	cfg, logDir, base := writeHTTPTestConfig(t, httpSpec{ftpAddr: proxy.addr(), sessions: 1, ftpWait: "300ms", shutdown: "2s"})
	startBridge(t, cfg)
	waitHTTP(t, base)

	go func() {
		req, _ := http.NewRequest(http.MethodPost, base+"/api/upload/", strings.NewReader("held"))
		req.SetBasicAuth(clientUser, clientPass)
		req.Header.Set("X-Folder", "held")
		req.Header.Set("X-File-Name", "a.txt")
		if resp, err := http.DefaultClient.Do(req); err == nil {
			resp.Body.Close()
		}
	}()
	select {
	case <-stored:
	case <-time.After(60 * time.Second):
		t.Fatal("first upload never reached STOR")
	}
	start := time.Now()
	resp, msg := httpUpload(t, base, "held", "b.txt", []byte("second"), clientUser, clientPass)
	if resp.StatusCode != 503 || resp.Header.Get("Retry-After") == "" || time.Since(start) < 300*time.Millisecond {
		t.Errorf("busy = %d %q after %v", resp.StatusCode, msg, time.Since(start))
	}
	id := resp.Header.Get("X-Request-Id")
	if s := steps(logDir, id); s != "received,failed,responded" {
		t.Errorf("steps = %s", s)
	}
}

func TestIntegrationHTTPFTPDown(t *testing.T) {
	env(t)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	down := ln.Addr().String()
	ln.Close()
	metricsAddr := fmt.Sprintf("127.0.0.1:%d", freePort(t))
	cfg, logDir, base := writeHTTPTestConfig(t, httpSpec{ftpAddr: down, metricsAddr: metricsAddr})
	startBridge(t, cfg)
	waitHTTP(t, base)
	resp, msg := httpUpload(t, base, "x", "a.txt", []byte("data"), clientUser, clientPass)
	if resp.StatusCode != 502 || strings.TrimSpace(msg) != "FTP connect failed" {
		t.Errorf("upload = %d %q", resp.StatusCode, msg)
	}
	for _, r := range readAudit(logDir) {
		if r.MsgID == resp.Header.Get("X-Request-Id") && r.Step == audit.StepFailed && r.FailedStep != "upload" {
			t.Errorf("failed = %+v", r)
		}
	}
	resp, msg = httpGet(t, base+"/api/file/L:/x/a.txt", "")
	if resp.StatusCode != 502 || strings.TrimSpace(msg) != "FTP connect failed" {
		t.Errorf("download = %d %q", resp.StatusCode, msg)
	}
	r, err := http.Get("http://" + metricsAddr + "/readyz")
	if err != nil {
		t.Fatal(err)
	}
	r.Body.Close()
	if r.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("readyz = %d after FTP failures", r.StatusCode)
	}
}

func TestIntegrationHTTPGracefulShutdown(t *testing.T) {
	_, ftpAddr := env(t)
	stored := make(chan struct{}, 1)
	proxy := newCtlProxy(t, ftpAddr, func() {
		select {
		case stored <- struct{}{}:
		default:
		}
		time.Sleep(2 * time.Second)
	})
	cfg, logDir, base := writeHTTPTestConfig(t, httpSpec{ftpAddr: proxy.addr()})
	_, stop := startBridge(t, cfg)
	waitHTTP(t, base)
	folder := "http/" + strings.ReplaceAll(uniq("stop"), ".", "-")
	status := make(chan int, 1)
	go func() {
		req, _ := http.NewRequest(http.MethodPost, base+"/api/upload/", strings.NewReader("finished during shutdown"))
		req.SetBasicAuth(clientUser, clientPass)
		req.Header.Set("X-Folder", folder)
		req.Header.Set("X-File-Name", "s.txt")
		req.Header.Set("X-Request-Id", "stop-1")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			status <- 0
			return
		}
		resp.Body.Close()
		status <- resp.StatusCode
	}()
	select {
	case <-stored:
	case <-time.After(60 * time.Second):
		t.Fatal("upload never reached STOR")
	}
	if err := stop(); err != nil {
		t.Fatal(err)
	}
	if code := <-status; code != 200 {
		t.Errorf("in-flight upload = %d", code)
	}
	if got := ftpRead(t, ftpAddr, folder+"/s.txt"); got != "finished during shutdown" {
		t.Errorf("content = %q", got)
	}
	if s := steps(logDir, "stop-1"); s != "received,uploaded,responded" {
		t.Errorf("steps = %s", s)
	}
	if c, err := net.DialTimeout("tcp", strings.TrimPrefix(base, "http://"), time.Second); err == nil {
		c.Close()
		t.Error("HTTP port still open after stop")
	}
}

func TestIntegrationHTTPWithAMQP(t *testing.T) {
	rabbitAddr, ftpAddr := env(t)
	src := newStub(t, map[string]string{"F-B": "from queue"})
	q := uniq("files.both")
	conn := dial(t, rabbitAddr)
	declare(t, conn, q)
	amqpCfg, logDir := writeTestConfig(t, rabbitAddr, ftpAddr, src.URL, "", routeSpec{queue: q})
	raw, err := os.ReadFile(amqpCfg)
	if err != nil {
		t.Fatal(err)
	}
	cfg, _, base := writeHTTPTestConfig(t, httpSpec{amqp: string(raw)})
	startBridge(t, cfg)
	waitHTTP(t, base)

	publish(t, conn, q, "both-1", "upload", "F-B", "q.txt", "both")
	resp, _ := httpUpload(t, base, "both", "h.txt", []byte("from http"), clientUser, clientPass)
	if resp.StatusCode != 200 {
		t.Errorf("http upload = %d", resp.StatusCode)
	}
	waitStep(t, logDir, "both-1", audit.StepAcked)
	if ftpRead(t, ftpAddr, "both/q.txt") != "from queue" || ftpRead(t, ftpAddr, "both/h.txt") != "from http" {
		t.Error("contents differ")
	}
}
