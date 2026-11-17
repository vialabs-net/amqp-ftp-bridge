package cli

import (
	"bytes"
	"context"
	"errors"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/vialabs-net/amqp-ftp-bridge/internal/audit"
	"github.com/vialabs-net/amqp-ftp-bridge/internal/config"
	"github.com/vialabs-net/amqp-ftp-bridge/internal/creds"
	"github.com/vialabs-net/amqp-ftp-bridge/internal/winsvc"
)

type memStore map[string]creds.Credential

func (m memStore) Get(n string) (creds.Credential, error) {
	c, ok := m[n]
	if !ok {
		return c, creds.ErrNotFound
	}
	return c, nil
}
func (m memStore) Set(n string, c creds.Credential) error { m[n] = c; return nil }

func run(args []string, stdin string) (int, string, string) {
	var out, errb bytes.Buffer
	code := Main(args, strings.NewReader(stdin), &out, &errb)
	return code, out.String(), errb.String()
}

const testConfig = `
data_dir: %DIR%
amqp:
  url: amqp://127.0.0.1:1/
  credential: rabbitmq
sources:
  - name: files-api
    url_prefix: http://127.0.0.1:1/files/
    credential: files-api
ftp:
  - name: main
    address: 127.0.0.1:1
    credential: ftp
routes:
  - queue: files.upload
    source: files-api
    target: main
    mapping:
      source_id: data.id
      folder: literal:in
      file_name: data.name
`

func writeConfig(t *testing.T) (string, string) {
	t.Helper()
	dir := t.TempDir()
	p := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(p, []byte(strings.ReplaceAll(testConfig, "%DIR%", dir)), 0o600); err != nil {
		t.Fatal(err)
	}
	return dir, p
}

func withStore(t *testing.T, s creds.Store) {
	old := newStore
	newStore = func(string) creds.Store { return s }
	t.Cleanup(func() { newStore = old })
}

func allCreds() memStore {
	return memStore{"rabbitmq": {User: "u", Password: "p"}, "files-api": {User: "u", Password: "p"}, "ftp": {User: "u", Password: "p"}}
}

func TestVersionHelpUsage(t *testing.T) {
	if code, out, _ := run([]string{"version"}, ""); code != 0 || !strings.HasPrefix(out, "amqp-ftp-bridge dev") {
		t.Errorf("version: %d %q", code, out)
	}
	if code, out, _ := run([]string{"help"}, ""); code != 0 || !strings.Contains(out, "creds set") {
		t.Errorf("help: %d", code)
	}
	if code, _, errs := run(nil, ""); code != 2 || !strings.Contains(errs, "Usage") {
		t.Errorf("no args: %d", code)
	}
	if code, _, errs := run([]string{"bogus"}, ""); code != 2 || !strings.Contains(errs, "unknown command") {
		t.Errorf("bogus: %d", code)
	}
	if code, _, _ := run([]string{"run", "--nope"}, ""); code != 2 {
		t.Errorf("bad flag: %d", code)
	}
	if code, _, _ := run([]string{"run", "-h"}, ""); code != 2 {
		t.Errorf("-h: %d", code)
	}
}

func TestServiceCommands(t *testing.T) {
	_, cfg := writeConfig(t)
	// On this system the real functions say they need Windows.
	if runtime.GOOS != "windows" {
		if code, _, errs := run([]string{"service", "start"}, ""); code != 1 || !strings.Contains(errs, "only available on Windows") {
			t.Errorf("start: %d %q", code, errs)
		}
	}
	var calls []string
	hooks := []*func() error{&svcRemove, &svcStart, &svcStop}
	saved := []func() error{svcRemove, svcStart, svcStop}
	savedInstall := svcInstall
	t.Cleanup(func() {
		for i, h := range hooks {
			*h = saved[i]
		}
		svcInstall = savedInstall
	})
	for i, name := range []string{"uninstall", "start", "stop"} {
		*hooks[i] = func() error { calls = append(calls, name); return nil }
	}
	svcInstall = func(exe, c string) error { calls = append(calls, "install "+filepath.Base(c)); return nil }
	for _, a := range [][]string{{"service", "install", "--config", cfg}, {"service", "uninstall"}, {"service", "start"}, {"service", "stop"}} {
		if code, _, errs := run(a, ""); code != 0 {
			t.Errorf("%v: %d %s", a, code, errs)
		}
	}
	if strings.Join(calls, ",") != "install config.yaml,uninstall,start,stop" {
		t.Errorf("calls = %v", calls)
	}
	svcStart = func() error { return errors.New("denied") }
	if code, _, _ := run([]string{"service", "start"}, ""); code != 1 {
		t.Error("start error")
	}
	if code, _, _ := run([]string{"service", "install", "--config", cfg + ".missing"}, ""); code != 1 {
		t.Error("install with missing config")
	}
	if code, _, _ := run([]string{"service"}, ""); code != 2 {
		t.Error("service without sub")
	}
	if code, _, _ := run([]string{"service", "pause"}, ""); code != 2 {
		t.Error("service pause")
	}
}

func TestCredsSet(t *testing.T) {
	dir, cfg := writeConfig(t)
	s := memStore{}
	var gotDir string
	old := newStore
	newStore = func(d string) creds.Store { gotDir = d; return s }
	t.Cleanup(func() { newStore = old })

	code, out, errs := run([]string{"creds", "set", "--config", cfg, "ftp"}, "uploader\nsecret\n")
	if code != 0 || s["ftp"] != (creds.Credential{User: "uploader", Password: "secret"}) || gotDir != dir {
		t.Errorf("creds set: %d %q %q %+v dir=%s", code, out, errs, s, gotDir)
	}
	if strings.Contains(out, "secret") {
		t.Error("password echoed")
	}
	if code, _, _ := run([]string{"creds", "set", "--data-dir", dir, "x"}, "u\np"); code != 0 || s["x"].Password != "p" {
		t.Error("no trailing newline")
	}
	if code, _, _ := run([]string{"creds", "set", "--data-dir", dir, "x"}, "u\n\n"); code != 1 {
		t.Error("empty password must fail")
	}
	if code, _, _ := run([]string{"creds", "set", "--data-dir", dir, "x"}, ""); code != 1 {
		t.Error("no input must fail")
	}
	if code, _, _ := run([]string{"creds", "set", "--data-dir", dir, "x"}, "u\n"); code != 1 {
		t.Error("missing password must fail")
	}
	if code, _, _ := run([]string{"creds", "set", "../bad"}, "u\np\n"); code != 1 {
		t.Error("bad name")
	}
	if code, _, _ := run([]string{"creds", "get", "x"}, ""); code != 2 {
		t.Error("creds get")
	}
	if code, _, _ := run([]string{"creds", "set"}, ""); code != 2 {
		t.Error("creds set without name")
	}
	// The environment store refuses to write on every platform.
	newStore = func(string) creds.Store { return creds.EnvStore{} }
	if code, _, errs := run([]string{"creds", "set", "--data-dir", dir, "x"}, "u\np\n"); code != 1 || !strings.Contains(errs, "AFB_CRED_X") {
		t.Errorf("env store: %d %q", code, errs)
	}
}

func TestCredsSecure(t *testing.T) {
	old := secureCreds
	t.Cleanup(func() { secureCreds = old })
	secureCreds = func(dir string) error {
		if dir != "data" {
			t.Fatalf("unexpected data directory: %q", dir)
		}
		return errors.New("ACL denied")
	}
	if code, _, errs := run([]string{"creds", "secure", "--data-dir", "data"}, ""); code != 1 || !strings.Contains(errs, "ACL denied") {
		t.Fatalf("migration error lost: %d %s", code, errs)
	}
	secureCreds = func(string) error { return nil }
	if code, _, errs := run([]string{"creds", "secure", "--data-dir", "data"}, ""); code != 0 {
		t.Fatalf("migration failed: %d %s", code, errs)
	}
	for _, args := range [][]string{{"creds", "secure"}, {"creds", "secure", "--data-dir", "data", "extra"}} {
		if code, _, _ := run(args, ""); code != 2 {
			t.Fatalf("invalid migration arguments accepted: %v", args)
		}
	}
}

func TestReadSecretTerminal(t *testing.T) {
	oldT, oldR := isTerminal, readPasswd
	t.Cleanup(func() { isTerminal, readPasswd = oldT, oldR })
	isTerminal = func(int) bool { return true }
	readPasswd = func(int) ([]byte, error) { return []byte("hidden"), nil }
	f, _ := os.Open(os.DevNull)
	defer f.Close()
	got, err := readSecret(f, nil)
	if err != nil || got != "hidden" {
		t.Errorf("%q %v", got, err)
	}
}

func TestAuditCommand(t *testing.T) {
	dir, cfg := writeConfig(t)
	logDir := filepath.Join(dir, "logs")
	w, err := audit.NewWriter(logDir, "v", "h")
	if err != nil {
		t.Fatal(err)
	}
	_ = w.Write(audit.Record{Step: audit.StepReceived, MsgID: "m1", Queue: "files.upload"})
	_ = w.Write(audit.Record{Step: audit.StepAcked, MsgID: "m1", Queue: "files.upload"})
	_ = w.Close()
	code, out, errs := run([]string{"audit", "--config", cfg, "--queue", "files.upload", "--result", "acked"}, "")
	if code != 0 || strings.Count(out, "\n") != 2 || !strings.Contains(errs, "1 message(s)") {
		t.Errorf("audit: %d %q %q", code, out, errs)
	}
	if code, _, _ := run([]string{"audit", "--log-dir", logDir, "--date", "bad"}, ""); code != 1 {
		t.Error("bad date")
	}
	if code, _, _ := run([]string{"audit", "--config", cfg + ".missing"}, ""); code != 1 {
		t.Error("missing config")
	}
	if code, _, _ := run([]string{"audit", "--bad"}, ""); code != 2 {
		t.Error("bad flag")
	}
}

func TestRunRefusesWithoutCredentials(t *testing.T) {
	_, cfg := writeConfig(t)
	withStore(t, memStore{"rabbitmq": {User: "u", Password: "p"}})
	code, _, errs := run([]string{"run", "--config", cfg}, "")
	if code != 1 || !strings.Contains(errs, `credential "files-api"`) {
		t.Errorf("run: %d %q", code, errs)
	}
	if code, _, errs := run([]string{"run", "--config", cfg + ".missing"}, ""); code != 1 || !strings.Contains(errs, "read config") {
		t.Errorf("missing config: %d %q", code, errs)
	}
}

type recLog struct{ info, errs []string }

func (r *recLog) Info(m string)  { r.info = append(r.info, m) }
func (r *recLog) Error(m string) { r.errs = append(r.errs, m) }
func (r *recLog) Close() error   { return nil }

func TestRunStartsAndStops(t *testing.T) {
	dir, cfg := writeConfig(t)
	withStore(t, allCreds())
	el := &recLog{}
	oldEL, oldCtx := openEvLog, notifyCtx
	t.Cleanup(func() { openEvLog, notifyCtx = oldEL, oldCtx })
	openEvLog = func() winsvc.EventLog { return el }
	ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
	defer cancel()
	notifyCtx = func() (context.Context, context.CancelFunc) { return ctx, cancel }

	// The broker at 127.0.0.1:1 refuses connections: the bridge retries until
	// the context ends, then stops cleanly.
	if code, _, errs := run([]string{"run", "--config", cfg}, ""); code != 0 {
		t.Fatalf("run: %d %s", code, errs)
	}
	if len(el.info) != 2 || !strings.Contains(el.info[0], "started") || !strings.Contains(el.info[1], "stopped") {
		t.Errorf("event log = %+v", el)
	}
	b, _ := os.ReadFile(filepath.Join(dir, "logs", "service-"+time.Now().Format("2006-01-02")+".log"))
	for _, want := range []string{"started", "reconnecting", "stopped"} {
		if !strings.Contains(string(b), want) {
			t.Errorf("service log lacks %q:\n%s", want, b)
		}
	}
}

func TestRunAsService(t *testing.T) {
	_, cfg := writeConfig(t)
	withStore(t, memStore{})
	el := &recLog{}
	oldIS, oldRS, oldEL := isService, runService, openEvLog
	t.Cleanup(func() { isService, runService, openEvLog = oldIS, oldRS, oldEL })
	isService = func() bool { return true }
	openEvLog = func() winsvc.EventLog { return el }
	runService = func(f func(context.Context) error) error { return f(context.Background()) }
	if code, _, _ := run([]string{"run", "--config", cfg}, ""); code != 1 {
		t.Error("want failure")
	}
	if len(el.errs) != 2 || !strings.Contains(el.errs[0], "cannot start") {
		t.Errorf("event log errors = %v", el.errs)
	}
}

func TestMetricsListenError(t *testing.T) {
	dir, cfg := writeConfig(t)
	b, _ := os.ReadFile(cfg)
	_ = os.WriteFile(cfg, append(b, []byte("metrics:\n  listen: 192.0.2.1:1\n")...), 0o600)
	_ = dir
	withStore(t, allCreds())
	if code, _, errs := run([]string{"run", "--config", cfg}, ""); code != 1 || !strings.Contains(errs, "metrics listener") {
		t.Errorf("%d %q", code, errs)
	}
}

func TestCheckFails(t *testing.T) {
	_, cfg := writeConfig(t)
	withStore(t, allCreds())
	code, out, _ := run([]string{"check", "--config", cfg, "--timeout", "2s"}, "")
	if code != 1 || strings.Count(out, "FAIL") != 3 {
		t.Errorf("check: %d\n%s", code, out)
	}
	withStore(t, memStore{})
	if code, _, _ := run([]string{"check", "--config", cfg}, ""); code != 1 {
		t.Error("missing creds")
	}
	if code, _, _ := run([]string{"check", "--config", cfg + ".x"}, ""); code != 1 {
		t.Error("missing config")
	}
	if code, _, _ := run([]string{"check", "--x"}, ""); code != 2 {
		t.Error("bad flag")
	}
}

const httpOnlyConfig = `
data_dir: %DIR%
shutdown_timeout: 2s
ftp:
  - name: main
    address: 127.0.0.1:1
    credential: ftp
http:
  listen: %LISTEN%
  credentials: [http-client]
  trusted_proxies: [192.0.2.10]
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
      ftp_credentials: { L: ftp-disk-l }
`

func writeHTTPConfig(t *testing.T, listen string) (string, string) {
	t.Helper()
	dir := t.TempDir()
	p := filepath.Join(dir, "config.yaml")
	body := strings.NewReplacer("%DIR%", dir, "%LISTEN%", listen).Replace(httpOnlyConfig)
	if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return dir, p
}

func httpCreds() memStore {
	return memStore{"ftp": {User: "u", Password: "p"}, "http-client": {User: "c", Password: "s"}, "ftp-disk-l": {User: "l", Password: "p"}}
}

func TestRunHTTPOnly(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close()
	dir, cfg := writeHTTPConfig(t, addr)
	withStore(t, httpCreds())
	el := &recLog{}
	oldEL, oldCtx := openEvLog, notifyCtx
	t.Cleanup(func() { openEvLog, notifyCtx = oldEL, oldCtx })
	openEvLog = func() winsvc.EventLog { return el }
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	notifyCtx = func() (context.Context, context.CancelFunc) { return ctx, cancel }
	done := make(chan int, 1)
	go func() { code, _, _ := run([]string{"run", "--config", cfg}, ""); done <- code }()

	var resp *http.Response
	for i := 0; i < 100; i++ {
		req, _ := http.NewRequest("POST", "http://"+addr+"/api/upload/", strings.NewReader("x"))
		req.SetBasicAuth("c", "wrong")
		if resp, err = http.DefaultClient.Do(req); err == nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 401 {
		t.Errorf("status = %d", resp.StatusCode)
	}
	cancel()
	if code := <-done; code != 0 {
		t.Errorf("run = %d", code)
	}
	b, _ := os.ReadFile(filepath.Join(dir, "logs", "service-"+time.Now().Format("2006-01-02")+".log"))
	for _, want := range []string{"http listening", "0 queue route(s) and 2 http route(s)", "stopped"} {
		if !strings.Contains(string(b), want) {
			t.Errorf("service log lacks %q:\n%s", want, b)
		}
	}
	if strings.Contains(string(b), "reconnecting") || strings.Contains(string(b), "amqp connected") {
		t.Errorf("HTTP-only service mentions amqp:\n%s", b)
	}
	a, _ := os.ReadFile(filepath.Join(dir, "logs", audit.FileName(time.Now())))
	if !strings.Contains(string(a), `"route":"upload"`) || !strings.Contains(string(a), `"http_status":401`) {
		t.Errorf("audit:\n%s", a)
	}
}

func TestBuildHTTPMissingCredential(t *testing.T) {
	cfg := &config.Config{HTTP: config.HTTP{Credentials: []string{"missing"}}}
	if _, err := buildHTTP(cfg, nil, nil, nil, nil); err == nil || !strings.Contains(err.Error(), "http credential") {
		t.Errorf("missing HTTP credential must fail closed: %v", err)
	}
}

func TestRunHTTPListenError(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	_, cfg := writeHTTPConfig(t, ln.Addr().String())
	withStore(t, httpCreds())
	if code, _, errs := run([]string{"run", "--config", cfg}, ""); code != 1 || !strings.Contains(errs, "http listener") {
		t.Errorf("%d %q", code, errs)
	}
}

func TestCheckHTTP(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	busy := ln.Addr().String()
	_, cfg := writeHTTPConfig(t, busy)
	withStore(t, httpCreds())
	code, out, _ := run([]string{"check", "--config", cfg, "--timeout", "2s"}, "")
	// Port in use, and both FTP logins fail (nothing listens on port 1).
	if code != 1 || strings.Count(out, "FAIL") != 3 || strings.Count(out, "OK   cred") != 3 ||
		!strings.Contains(out, "(ftp-disk-l)") || !strings.Contains(out, "FAIL http") || strings.Contains(out, "amqp") {
		t.Errorf("check: %d\n%s", code, out)
	}
	ln.Close()
	withStore(t, memStore{"ftp": {User: "u", Password: "p"}})
	code, out, _ = run([]string{"check", "--config", cfg, "--timeout", "2s"}, "")
	if code != 1 || !strings.Contains(out, "FAIL cred    http-client") || !strings.Contains(out, "OK   http") || strings.Contains(out, "(ftp-disk-l)") {
		t.Errorf("check: %d\n%s", code, out)
	}
}
