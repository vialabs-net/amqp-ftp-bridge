//go:build integration

// Integration tests: RabbitMQ and an FTP server in containers, an HTTP stub in
// process. Run with: go test -tags integration ./internal/cli
package cli

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/jlaffaye/ftp"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/network"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/vialabs-net/amqp-ftp-bridge/internal/audit"
	"github.com/vialabs-net/amqp-ftp-bridge/internal/winsvc"
)

const (
	rabbitImage = "rabbitmq:4-alpine@sha256:7b77c3d3a996c7fcca1ca8bf53ce1557ff43c13b8ca2c132df6031781b209ef2"
	ftpImage    = "delfer/alpine-ftp-server:latest@sha256:b27013e7436cea343826ba7edb77aa3a8b2d22df14e366b82d10a3b0b5ecc880"
	rabbitUser  = "bridge"
	rabbitPass  = "bridge-test"
	ftpUser     = "uploader"
	ftpPass     = "ftp-test"
	httpUser    = "reader"
	httpPass    = "files-test"
	helperEnv   = "AFB_TEST_HELPER_CONFIG"
)

var (
	shared struct {
		once      sync.Once
		err       error
		rabbit    testcontainers.Container
		ftpC      testcontainers.Container
		rabbitAdr string
		ftpAdr    string
	}
)

// TestMain also serves as the bridge process for the kill test: with
// AFB_TEST_HELPER_CONFIG set, it runs the bridge until killed.
func TestMain(m *testing.M) {
	if cfg := os.Getenv(helperEnv); cfg != "" {
		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer stop()
		if err := serve(ctx, cfg, os.Stderr, winsvc.OpenEventLog()); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		os.Exit(0)
	}
	code := m.Run()
	ctx := context.Background()
	if shared.rabbit != nil {
		_ = shared.rabbit.Terminate(ctx)
	}
	if shared.ftpC != nil {
		_ = shared.ftpC.Terminate(ctx)
	}
	os.Exit(code)
}

func freePort(t testing.TB) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	return ln.Addr().(*net.TCPAddr).Port
}

func portFree(p int) bool {
	ln, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", p))
	if err != nil {
		return false
	}
	ln.Close()
	return true
}

// freeRange finds n consecutive free ports.
func freeRange(n int) (int, error) {
	for try := 0; try < 200; try++ {
		base := 30000 + rand.IntN(25000)
		ok := true
		for p := base; p < base+n && ok; p++ {
			ok = portFree(p)
		}
		if ok {
			return base, nil
		}
	}
	return 0, fmt.Errorf("no %d consecutive free ports", n)
}

func bind(hc *container.HostConfig, containerPort string, hostPort int) {
	if hc.PortBindings == nil {
		hc.PortBindings = network.PortMap{}
	}
	hc.PortBindings[network.MustParsePort(containerPort)] = []network.PortBinding{
		{HostIP: netip.MustParseAddr("127.0.0.1"), HostPort: fmt.Sprint(hostPort)},
	}
}

// startRabbit starts RabbitMQ. With hostPort > 0 the AMQP port is bound to
// that host port, so that it stays the same across a restart.
func startRabbit(ctx context.Context, hostPort int) (testcontainers.Container, string, error) {
	req := testcontainers.ContainerRequest{
		Image:        rabbitImage,
		ExposedPorts: []string{"5672/tcp"},
		Env:          map[string]string{"RABBITMQ_DEFAULT_USER": rabbitUser, "RABBITMQ_DEFAULT_PASS": rabbitPass},
		WaitingFor:   wait.ForLog("Server startup complete").WithStartupTimeout(3 * time.Minute),
	}
	if hostPort > 0 {
		req.HostConfigModifier = func(hc *container.HostConfig) { bind(hc, "5672/tcp", hostPort) }
	}
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{ContainerRequest: req, Started: true})
	if err != nil {
		return c, "", err
	}
	host, err := c.Host(ctx)
	if err != nil {
		return c, "", err
	}
	p, err := c.MappedPort(ctx, "5672/tcp")
	if err != nil {
		return c, "", err
	}
	return c, net.JoinHostPort(host, p.Port()), nil
}

// startFTP starts vsftpd. Passive ports are bound one to one on the host,
// so that the address in the PASV reply works from the host.
func startFTP(ctx context.Context) (testcontainers.Container, string, error) {
	const n = 6
	base, err := freeRange(n)
	if err != nil {
		return nil, "", err
	}
	exposed := []string{"21/tcp"}
	for p := base; p < base+n; p++ {
		exposed = append(exposed, fmt.Sprintf("%d/tcp", p))
	}
	req := testcontainers.ContainerRequest{
		Image:        ftpImage,
		ExposedPorts: exposed,
		Env: map[string]string{
			"USERS":    ftpUser + "|" + ftpPass,
			"ADDRESS":  "127.0.0.1",
			"MIN_PORT": fmt.Sprint(base),
			"MAX_PORT": fmt.Sprint(base + n - 1),
		},
		HostConfigModifier: func(hc *container.HostConfig) {
			for p := base; p < base+n; p++ {
				bind(hc, fmt.Sprintf("%d/tcp", p), p)
			}
		},
		WaitingFor: wait.ForListeningPort("21/tcp").WithStartupTimeout(2 * time.Minute),
	}
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{ContainerRequest: req, Started: true})
	if err != nil {
		return c, "", err
	}
	p, err := c.MappedPort(ctx, "21/tcp")
	if err != nil {
		return c, "", err
	}
	return c, net.JoinHostPort("127.0.0.1", p.Port()), nil
}

func env(t *testing.T) (rabbitAddr, ftpAddr string) {
	t.Helper()
	shared.once.Do(func() {
		ctx := context.Background()
		shared.rabbit, shared.rabbitAdr, shared.err = startRabbit(ctx, 0)
		if shared.err != nil {
			return
		}
		shared.ftpC, shared.ftpAdr, shared.err = startFTP(ctx)
	})
	if shared.err != nil {
		t.Fatalf("start containers: %v", shared.err)
	}
	t.Setenv("AFB_CRED_RABBITMQ", rabbitUser+":"+rabbitPass)
	t.Setenv("AFB_CRED_FILES_API", httpUser+":"+httpPass)
	t.Setenv("AFB_CRED_FTP", ftpUser+":"+ftpPass)
	t.Setenv("AFB_CRED_FTP_WRONG", ftpUser+":wrong-password")
	return shared.rabbitAdr, shared.ftpAdr
}

// stub is the HTTP source. Files are served under /files/<id> with Basic auth.
type stub struct {
	*httptest.Server
	mu    sync.Mutex
	files map[string]string
	// stall, when set, makes the first request for that id send half of the
	// body and block until release is closed.
	requests chan string
}

func newStub(t *testing.T, files map[string]string) *stub {
	s := &stub{files: files, requests: make(chan string, 100)}
	s.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if u, p, ok := r.BasicAuth(); !ok || u != httpUser || p != httpPass {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		id := strings.TrimPrefix(r.URL.Path, "/files/")
		s.requests <- id
		s.mu.Lock()
		body, ok := s.files[id]
		s.mu.Unlock()
		if !ok {
			http.NotFound(w, r)
			return
		}
		_, _ = io.WriteString(w, body)
	}))
	t.Cleanup(s.Close)
	return s
}

type routeSpec struct {
	queue, target, otherwise, uploadField string
}

func writeTestConfig(t *testing.T, rabbitAddr, ftpAddr, srcURL, metricsAddr string, routes ...routeSpec) (cfgPath, logDir string) {
	t.Helper()
	dir := t.TempDir()
	var b strings.Builder
	fmt.Fprintf(&b, "data_dir: %s\n", dir)
	fmt.Fprintf(&b, "shutdown_timeout: 30s\n")
	fmt.Fprintf(&b, "amqp:\n  url: amqp://%s/\n  credential: rabbitmq\n  reconnect_max: 2s\n  heartbeat: 2s\n", rabbitAddr)
	if metricsAddr != "" {
		fmt.Fprintf(&b, "metrics:\n  listen: %s\n", metricsAddr)
	}
	fmt.Fprintf(&b, "sources:\n  - name: files-api\n    url_prefix: %s/files/\n    credential: files-api\n    timeout: 30s\n", srcURL)
	fmt.Fprintf(&b, "ftp:\n  - name: main\n    address: %s\n    credential: ftp\n    timeout: 10s\n", ftpAddr)
	fmt.Fprintf(&b, "  - name: wrong\n    address: %s\n    credential: ftp_wrong\n    timeout: 10s\n", ftpAddr)
	b.WriteString("routes:\n")
	for _, r := range routes {
		target := r.target
		if target == "" {
			target = "main"
		}
		fmt.Fprintf(&b, "  - queue: %s\n    source: files-api\n    target: %s\n", r.queue, target)
		b.WriteString("    mapping:\n      source_id: data._file.id\n      folder: header:DestinationPath\n      file_name: data._file.name\n")
		if r.otherwise != "" {
			fmt.Fprintf(&b, "    match:\n      field: event\n      equals: upload\n      otherwise: %s\n", r.otherwise)
			if r.uploadField != "" {
				fmt.Fprintf(&b, "      upload_field: %s\n", r.uploadField)
			}
		}
	}
	cfgPath = filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(cfgPath, []byte(b.String()), 0o600); err != nil {
		t.Fatal(err)
	}
	return cfgPath, filepath.Join(dir, "logs")
}

// syncBuffer is a goroutine-safe buffer for the operational log.
type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

// startBridge runs the bridge in process. The returned function stops it
// and returns its error.
func startBridge(t *testing.T, cfgPath string) (*syncBuffer, func() error) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	logs := &syncBuffer{}
	done := make(chan error, 1)
	go func() { done <- serve(ctx, cfgPath, logs, winsvc.OpenEventLog()) }()
	var once sync.Once
	var err error
	stop := func() error {
		once.Do(func() {
			cancel()
			select {
			case err = <-done:
			case <-time.After(60 * time.Second):
				err = fmt.Errorf("bridge did not stop")
			}
		})
		return err
	}
	t.Cleanup(func() { _ = stop() })
	return logs, stop
}

func dial(t *testing.T, addr string) *amqp.Connection {
	t.Helper()
	var conn *amqp.Connection
	var err error
	for i := 0; i < 60; i++ {
		conn, err = amqp.Dial(fmt.Sprintf("amqp://%s:%s@%s/", rabbitUser, rabbitPass, addr))
		if err == nil {
			t.Cleanup(func() { _ = conn.Close() })
			return conn
		}
		time.Sleep(time.Second)
	}
	t.Fatalf("dial rabbitmq: %v", err)
	return nil
}

func declare(t *testing.T, conn *amqp.Connection, queues ...string) {
	t.Helper()
	ch, err := conn.Channel()
	if err != nil {
		t.Fatal(err)
	}
	defer ch.Close()
	for _, q := range queues {
		if _, err := ch.QueueDeclare(q, true, false, false, false, nil); err != nil {
			t.Fatal(err)
		}
		_, _ = ch.QueuePurge(q, false)
	}
}

func publish(t *testing.T, conn *amqp.Connection, queue, msgID, event, fileID, name, folder string) {
	t.Helper()
	ch, err := conn.Channel()
	if err != nil {
		t.Fatal(err)
	}
	defer ch.Close()
	body, _ := json.Marshal(map[string]any{
		"event": event,
		"data":  map[string]any{"_file": map[string]any{"id": fileID, "name": name}, "note": "n-" + msgID},
	})
	err = ch.PublishWithContext(context.Background(), "", queue, false, false, amqp.Publishing{
		MessageId: msgID, ContentType: "application/json", DeliveryMode: amqp.Persistent, Body: body,
		Headers: amqp.Table{"DestinationPath": folder, "x-trace": "trace-" + msgID},
	})
	if err != nil {
		t.Fatal(err)
	}
}

func readAudit(logDir string) []audit.Record {
	f, err := os.Open(filepath.Join(logDir, audit.FileName(time.Now())))
	if err != nil {
		return nil
	}
	defer f.Close()
	var out []audit.Record
	s := bufio.NewScanner(f)
	s.Buffer(nil, 1<<24)
	for s.Scan() {
		var r audit.Record
		if json.Unmarshal(s.Bytes(), &r) == nil {
			out = append(out, r)
		}
	}
	return out
}

// waitStep waits for an audit record of msgID with the given step.
func waitStep(t *testing.T, logDir, msgID, step string) audit.Record {
	t.Helper()
	deadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) {
		for _, r := range readAudit(logDir) {
			if r.MsgID == msgID && r.Step == step {
				return r
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("no %s record for %s; audit:\n%+v", step, msgID, readAudit(logDir))
	return audit.Record{}
}

func steps(logDir, msgID string) string {
	var s []string
	for _, r := range readAudit(logDir) {
		if r.MsgID == msgID {
			s = append(s, r.Step)
		}
	}
	return strings.Join(s, ",")
}

func getDeadLetter(t *testing.T, conn *amqp.Connection, queue string) amqp.Delivery {
	t.Helper()
	for i := 0; i < 300; i++ {
		// A Get on a queue that does not exist yet closes the channel: use a
		// new one each time.
		ch, err := conn.Channel()
		if err != nil {
			t.Fatal(err)
		}
		d, ok, err := ch.Get(queue, true)
		_ = ch.Close()
		if err == nil && ok {
			return d
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("no message in %s", queue)
	return amqp.Delivery{}
}

func ftpRead(t *testing.T, addr, path string) string {
	t.Helper()
	c, err := ftp.Dial(addr, ftp.DialWithTimeout(10*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Quit()
	if err := c.Login(ftpUser, ftpPass); err != nil {
		t.Fatal(err)
	}
	r, err := c.Retr(path)
	if err != nil {
		t.Fatalf("retr %s: %v", path, err)
	}
	defer r.Close()
	b, _ := io.ReadAll(r)
	return string(b)
}

func uniq(prefix string) string {
	return fmt.Sprintf("%s.%d", prefix, time.Now().UnixNano())
}

func TestIntegrationHappyPath(t *testing.T) {
	rabbitAddr, ftpAddr := env(t)
	content := strings.Repeat("%PDF-1.7 integration ", 50000)
	src := newStub(t, map[string]string{"F-100": content})
	q := uniq("files.upload")
	conn := dial(t, rabbitAddr)
	declare(t, conn, q)
	cfg, logDir := writeTestConfig(t, rabbitAddr, ftpAddr, src.URL, "", routeSpec{queue: q, otherwise: "dead_letter"})
	_, stop := startBridge(t, cfg)

	folder := "inbox/" + strings.ReplaceAll(q, ".", "-") + "/deep"
	publish(t, conn, q, "happy-1", "upload", "F-100", "report.pdf", folder)
	acked := waitStep(t, logDir, "happy-1", audit.StepAcked)
	if acked.TotalMs == nil || acked.Queue != q || acked.Hostname == "" || acked.Version == "" {
		t.Errorf("acked = %+v", acked)
	}
	if got := ftpRead(t, ftpAddr, folder+"/report.pdf"); got != content {
		t.Errorf("uploaded %d bytes, want %d", len(got), len(content))
	}
	if s := steps(logDir, "happy-1"); s != "received,downloaded,uploaded,acked" {
		t.Errorf("steps = %s", s)
	}
	recs := readAudit(logDir)
	for _, r := range recs {
		if r.Step == audit.StepDownloaded && (r.SHA256 == "" || *r.Bytes != int64(len(content)) || strings.Contains(r.Host, "@")) {
			t.Errorf("downloaded = %+v", r)
		}
	}

	// Same file again: the upload overwrites.
	src.mu.Lock()
	src.files["F-100"] = "second version"
	src.mu.Unlock()
	publish(t, conn, q, "happy-2", "upload", "F-100", "report.pdf", folder)
	waitStep(t, logDir, "happy-2", audit.StepAcked)
	if got := ftpRead(t, ftpAddr, folder+"/report.pdf"); got != "second version" {
		t.Errorf("overwrite: got %q", got)
	}
	if err := stop(); err != nil {
		t.Fatal(err)
	}
	raw, _ := os.ReadFile(filepath.Join(logDir, audit.FileName(time.Now())))
	for _, secret := range []string{httpPass, ftpPass, rabbitPass, "Authorization", "%PDF"} {
		if bytes.Contains(raw, []byte(secret)) {
			t.Errorf("audit log contains %q", secret)
		}
	}
}

func TestIntegrationDownload404(t *testing.T) {
	rabbitAddr, ftpAddr := env(t)
	src := newStub(t, map[string]string{})
	q := uniq("files.missing")
	conn := dial(t, rabbitAddr)
	declare(t, conn, q)
	cfg, logDir := writeTestConfig(t, rabbitAddr, ftpAddr, src.URL, "", routeSpec{queue: q})
	startBridge(t, cfg)

	publish(t, conn, q, "nf-1", "upload", "NOPE", "a.pdf", "inbox")
	d := getDeadLetter(t, conn, q+"_deadLetter")
	if e, _ := d.Headers["x-error"].(string); !strings.Contains(e, "HTTP 404") {
		t.Errorf("x-error = %q", e)
	}
	if st, _ := d.Headers["x-error-stacktrace"].(string); !strings.Contains(st, "step: download") || !strings.Contains(st, "http_status: 404") {
		t.Errorf("x-error-stacktrace = %q", st)
	}
	if d.Headers["x-trace"] != "trace-nf-1" || d.Headers["DestinationPath"] != "inbox" || d.MessageId != "nf-1" {
		t.Errorf("original headers lost: %+v", d.Headers)
	}
	if !strings.Contains(string(d.Body), `"id":"NOPE"`) {
		t.Errorf("body = %s", d.Body)
	}
	rec := waitStep(t, logDir, "nf-1", audit.StepDeadLettered)
	if rec.FailedStep != "download" || rec.HTTPStatus != 404 {
		t.Errorf("dead_lettered = %+v", rec)
	}
	if s := steps(logDir, "nf-1"); s != "received,dead_lettered" {
		t.Errorf("steps = %s", s)
	}
}

func TestIntegrationFTPFailure(t *testing.T) {
	rabbitAddr, ftpAddr := env(t)
	src := newStub(t, map[string]string{"F-1": "data"})
	q := uniq("files.ftpfail")
	conn := dial(t, rabbitAddr)
	declare(t, conn, q)
	metricsAddr := fmt.Sprintf("127.0.0.1:%d", freePort(t))
	cfg, logDir := writeTestConfig(t, rabbitAddr, ftpAddr, src.URL, metricsAddr, routeSpec{queue: q, target: "wrong"})
	startBridge(t, cfg)

	publish(t, conn, q, "ftp-1", "upload", "F-1", "a.txt", "inbox")
	d := getDeadLetter(t, conn, q+"_deadLetter")
	if e, _ := d.Headers["x-error"].(string); !strings.Contains(e, "530") {
		t.Errorf("x-error = %q", e)
	}
	rec := waitStep(t, logDir, "ftp-1", audit.StepDeadLettered)
	if rec.FailedStep != "upload" || rec.FTPCode != 530 {
		t.Errorf("dead_lettered = %+v", rec)
	}

	resp, err := http.Get("http://" + metricsAddr + "/readyz")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("readyz = %d after an FTP failure", resp.StatusCode)
	}
	resp, err = http.Get("http://" + metricsAddr + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	for _, want := range []string{
		fmt.Sprintf(`bridge_errors_total{code="530",queue="%s",stage="upload"} 1`, q),
		fmt.Sprintf(`bridge_messages_total{queue="%s",result="dead_lettered"} 1`, q),
		"bridge_amqp_connected 1",
	} {
		if !strings.Contains(string(b), want) {
			t.Errorf("metrics lack %s", want)
		}
	}
}

func TestIntegrationUnmatched(t *testing.T) {
	rabbitAddr, ftpAddr := env(t)
	src := newStub(t, map[string]string{})
	qDL, qField := uniq("files.unmatched-dl"), uniq("files.unmatched-field")
	conn := dial(t, rabbitAddr)
	declare(t, conn, qDL, qField)
	cfg, logDir := writeTestConfig(t, rabbitAddr, ftpAddr, src.URL, "",
		routeSpec{queue: qDL, otherwise: "dead_letter"},
		routeSpec{queue: qField, otherwise: "upload_field", uploadField: "data"})
	startBridge(t, cfg)

	publish(t, conn, qDL, "um-1", "delete", "F-1", "a.json", "legacy")
	d := getDeadLetter(t, conn, qDL+"_deadLetter")
	if st, _ := d.Headers["x-error-stacktrace"].(string); !strings.Contains(st, "step: match") {
		t.Errorf("stacktrace = %q", st)
	}
	if r := waitStep(t, logDir, "um-1", audit.StepReceived); r.Event == nil || *r.Event != "delete" || *r.Matched {
		t.Errorf("received = %+v", r)
	}
	publish(t, conn, qDL, "um-large", strings.Repeat("x", 50000), "F-1", "a.json", "legacy")
	large := getDeadLetter(t, conn, qDL+"_deadLetter")
	for _, key := range []string{"x-error", "x-error-stacktrace"} {
		if diagnostic, _ := large.Headers[key].(string); len(diagnostic) > 2048 {
			t.Errorf("oversized %s: %d bytes", key, len(diagnostic))
		}
	}
	waitStep(t, logDir, "um-large", audit.StepDeadLettered)

	publish(t, conn, qField, "um-2", "update", "F-2", "b.json", "legacy")
	waitStep(t, logDir, "um-2", audit.StepAcked)
	got := ftpRead(t, ftpAddr, "legacy/b.json")
	var data map[string]any
	if err := json.Unmarshal([]byte(got), &data); err != nil || data["note"] != "n-um-2" {
		t.Errorf("uploaded field = %q", got)
	}
	if s := steps(logDir, "um-2"); s != "received,uploaded,acked" {
		t.Errorf("steps = %s", s)
	}
	select {
	case id := <-src.requests:
		t.Errorf("unexpected download of %s", id)
	default:
	}
}

// ftpProxy forwards FTP control connections. While armed, it holds the
// first STOR command it sees and reports it, so the client is stuck in the
// middle of the upload.
type ftpProxy struct {
	ln     net.Listener
	target string
	mu     sync.Mutex
	armed  bool
	stored chan struct{}
}

func newFTPProxy(t *testing.T, target string) *ftpProxy {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := &ftpProxy{ln: ln, target: target, armed: true, stored: make(chan struct{}, 1)}
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

func (p *ftpProxy) disarm() { p.mu.Lock(); p.armed = false; p.mu.Unlock() }

func (p *ftpProxy) handle(client net.Conn) {
	defer client.Close()
	up, err := net.Dial("tcp", p.target)
	if err != nil {
		return
	}
	defer up.Close()
	go func() { _, _ = io.Copy(client, up) }()
	r := bufio.NewReader(client)
	for {
		line, err := r.ReadString('\n')
		if err != nil {
			return
		}
		if strings.HasPrefix(strings.ToUpper(line), "STOR ") {
			p.mu.Lock()
			armed := p.armed
			p.mu.Unlock()
			if armed {
				p.stored <- struct{}{}
				select {} // hold the upload until the process dies
			}
		}
		if _, err := io.WriteString(up, line); err != nil {
			return
		}
	}
}

func TestIntegrationRedeliveryAfterKill(t *testing.T) {
	rabbitAddr, ftpAddr := env(t)
	src := newStub(t, map[string]string{"F-K": "complete content"})
	proxy := newFTPProxy(t, ftpAddr)
	q := uniq("files.kill")
	conn := dial(t, rabbitAddr)
	declare(t, conn, q)
	cfg, logDir := writeTestConfig(t, rabbitAddr, proxy.ln.Addr().String(), src.URL, "", routeSpec{queue: q})

	cmd := exec.Command(os.Args[0], "-test.run=^$")
	cmd.Env = append(os.Environ(), helperEnv+"="+cfg)
	var out syncBuffer
	cmd.Stdout, cmd.Stderr = &out, &out
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cmd.Process.Kill(); _, _ = cmd.Process.Wait() })

	publish(t, conn, q, "kill-1", "upload", "F-K", "k.txt", "killed")
	select {
	case <-proxy.stored:
	case <-time.After(90 * time.Second):
		t.Fatalf("upload never started; process output:\n%s", out.String())
	}
	if s := steps(logDir, "kill-1"); s != "received,downloaded" {
		t.Errorf("steps before kill = %s", s)
	}
	if err := cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_, _ = cmd.Process.Wait()
	proxy.disarm()

	startBridge(t, cfg)
	waitStep(t, logDir, "kill-1", audit.StepAcked)
	var redelivered bool
	for _, r := range readAudit(logDir) {
		if r.MsgID == "kill-1" && r.Step == audit.StepReceived && r.Redelivered != nil && *r.Redelivered {
			redelivered = true
		}
	}
	if !redelivered {
		t.Errorf("no received record with redelivered=true: %s", steps(logDir, "kill-1"))
	}
	if got := ftpRead(t, ftpAddr, "killed/k.txt"); got != "complete content" {
		t.Errorf("content = %q", got)
	}
}

func TestIntegrationReconnect(t *testing.T) {
	_, ftpAddr := env(t)
	ctx := context.Background()
	port := freePort(t)
	rc, addr, err := startRabbit(ctx, port)
	if rc != nil {
		t.Cleanup(func() { _ = rc.Terminate(ctx) })
	}
	if err != nil {
		t.Fatal(err)
	}
	src := newStub(t, map[string]string{"F-R": "before", "F-R2": "after"})
	q := uniq("files.reconnect")
	conn := dial(t, addr)
	declare(t, conn, q)
	cfg, logDir := writeTestConfig(t, addr, ftpAddr, src.URL, "", routeSpec{queue: q})
	logs, stop := startBridge(t, cfg)

	publish(t, conn, q, "rc-1", "upload", "F-R", "r1.txt", "reconnect")
	waitStep(t, logDir, "rc-1", audit.StepAcked)

	timeout := 10 * time.Second
	if err := rc.Stop(ctx, &timeout); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(30 * time.Second)
	for !strings.Contains(logs.String(), "reconnecting") && time.Now().Before(deadline) {
		time.Sleep(100 * time.Millisecond)
	}
	if err := rc.Start(ctx); err != nil {
		t.Fatal(err)
	}
	conn2 := dial(t, addr)
	publish(t, conn2, q, "rc-2", "upload", "F-R2", "r2.txt", "reconnect")
	waitStep(t, logDir, "rc-2", audit.StepAcked)
	if got := ftpRead(t, ftpAddr, "reconnect/r2.txt"); got != "after" {
		t.Errorf("content = %q", got)
	}
	if err := stop(); err != nil {
		t.Fatal(err)
	}
	l := logs.String()
	if strings.Count(l, "amqp connected") < 2 || !strings.Contains(l, "reconnecting") {
		t.Errorf("service log:\n%s", l)
	}
	if strings.Contains(l, rabbitPass) {
		t.Error("service log contains the password")
	}
}

func TestIntegrationCheck(t *testing.T) {
	rabbitAddr, ftpAddr := env(t)
	src := newStub(t, map[string]string{})
	q := uniq("files.check")
	conn := dial(t, rabbitAddr)
	declare(t, conn, q)
	cfg, _ := writeTestConfig(t, rabbitAddr, ftpAddr, src.URL, "", routeSpec{queue: q})
	code, out, errs := run([]string{"check", "--config", cfg}, "")
	// The "wrong" FTP target fails to log in.
	if code != 1 || strings.Count(out, "FAIL") != 1 || !strings.Contains(out, "530") || strings.Count(out, "OK") != 4 {
		t.Errorf("check: %d\n%s%s", code, out, errs)
	}
	if !strings.Contains(out, "0 message(s), 0 consumer(s)") {
		t.Errorf("queue line missing:\n%s", out)
	}
}
