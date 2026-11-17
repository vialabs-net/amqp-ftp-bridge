package config

import (
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const httpOnly = `
data_dir: /tmp/afb
ftp:
  - name: main
    address: ftp.example.internal:21
    credential: ftp
http:
  listen: "127.0.0.1:8080"
  credentials: [http-client]
  trusted_proxies: [192.0.2.10, 192.0.2.0/28, "2001:db8::1"]
  max_upload_bytes: 10MiB
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
      ftp_credentials: { L: ftp-disk-l, M: ftp-disk-m }
`

func TestHTTPOnly(t *testing.T) {
	c, err := Parse([]byte(httpOnly))
	if err != nil {
		t.Fatal(err)
	}
	h := c.HTTP
	if !h.Enabled() || h.MaxFTPSessions != 8 || h.FTPWait != 30*time.Second || h.MaxUploadBytes != 10<<20 ||
		h.ReadHeaderTimeout != 10*time.Second || h.ReadTimeout != 10*time.Minute || h.WriteTimeout != 10*time.Minute || h.IdleTimeout != 2*time.Minute {
		t.Errorf("defaults: %+v", h)
	}
	if h.Routes[0].Method != "POST" || h.Routes[1].Method != "GET" {
		t.Errorf("methods: %q %q", h.Routes[0].Method, h.Routes[1].Method)
	}
	if got := strings.Join(c.Credentials(), ","); got != "ftp,http-client,ftp-disk-l,ftp-disk-m" {
		t.Errorf("credentials = %s", got)
	}
}

func TestHTTPDisabled(t *testing.T) {
	c, err := Parse([]byte(valid))
	if err != nil {
		t.Fatal(err)
	}
	if c.HTTP.Enabled() {
		t.Error("enabled without listen")
	}
}

func TestHTTPValidate(t *testing.T) {
	cases := map[string]struct{ old, new, want string }{
		"listen":         {`listen: "127.0.0.1:8080"`, "listen: nope", "http.listen"},
		"no listen":      {`listen: "127.0.0.1:8080"`, `listen: ""`, "http.listen is required"},
		"no credentials": {"credentials: [http-client]", "credentials: []", "http.credentials"},
		"proxy":          {"192.0.2.10,", "proxy.example.com,", "trusted_proxies[0]"},
		"sessions":       {"max_upload_bytes: 10MiB", "max_upload_bytes: 10MiB\n  max_ftp_sessions: -1", "max_ftp_sessions"},
		"wait":           {"max_upload_bytes: 10MiB", "max_upload_bytes: 10MiB\n  ftp_wait: -1s", "ftp_wait must be positive"},
		"size":           {"max_upload_bytes: 10MiB", "max_upload_bytes: ten", "invalid size"},
		"name":           {"name: download", "name: upload", "duplicate name"},
		"no name":        {"name: download", `name: ""`, "name is required"},
		"action":         {"action: download", "action: delete", "action must be upload or download"},
		"method":         {"action: upload", "action: upload\n      method: \"P OST\"", "invalid method"},
		"target":         {"ftp: main\n      folder: path:dir", "ftp: other\n      folder: path:dir", `unknown ftp target "other"`},
		"pattern":        {"path: /api/upload/", "path: api/upload", "must start with /"},
		"path var":       {"folder: path:dir", "folder: path:folder", "path has no {folder}"},
		"selector":       {"folder: header:X-Folder", "folder: X-Folder", "must start with header:"},
		"empty header":   {"folder: header:X-Folder", "folder: \"header:\"", "has no name"},
		"missing":        {"file_name: header:X-File-Name", `file_name: ""`, "file_name: is required"},
		"creds without":  {"      ftp_credential_by: path:disk\n", "", "ftp_credentials needs ftp_credential_by"},
		"by without":     {"ftp_credentials: { L: ftp-disk-l, M: ftp-disk-m }", "", "ftp_credentials is required"},
		"empty cred":     {"M: ftp-disk-m", `M: ""`, "empty value or credential name"},
		"unknown key":    {"  routes:\n", "  routes: []\n  unused:\n", "field unused not found"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			if !strings.Contains(httpOnly, tc.old) {
				t.Fatalf("%q not in config", tc.old)
			}
			_, err := Parse([]byte(strings.Replace(httpOnly, tc.old, tc.new, 1)))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Errorf("err = %v, want %q", err, tc.want)
			}
		})
	}
	noRoutes := "ftp:\n  - name: main\n    address: ftp.example.internal:21\n    credential: ftp\nhttp:\n  listen: 127.0.0.1:8080\n  credentials: [a]\n"
	if _, err := Parse([]byte(noRoutes)); err == nil || !strings.Contains(err.Error(), "at least one route is required when http.listen") {
		t.Errorf("no routes: %v", err)
	}
	if _, err := Parse([]byte("http:\n  routes:\n    - name: x\n")); err == nil || !strings.Contains(err.Error(), "http.listen is required") {
		t.Errorf("routes without listen: %v", err)
	}
}

func TestHTTPWithAMQP(t *testing.T) {
	both := strings.Replace(valid, "routes:\n  - queue", "http:\n"+strings.SplitN(httpOnly, "http:\n", 2)[1]+"routes:\n  - queue", 1)
	c, err := Parse([]byte(both))
	if err != nil {
		t.Fatal(err)
	}
	if len(c.Routes) != 2 || len(c.HTTP.Routes) != 2 {
		t.Errorf("routes: %d %d", len(c.Routes), len(c.HTTP.Routes))
	}
	if got := strings.Join(c.Credentials(), ","); got != "rabbitmq,files-api,ftp,http-client,ftp-disk-l,ftp-disk-m" {
		t.Errorf("credentials = %s", got)
	}
}

func TestParseByteSize(t *testing.T) {
	for in, want := range map[string]ByteSize{
		"1024": 1024, "200MiB": 200 << 20, "10 MB": 10_000_000, "1GiB": 1 << 30, "5k": 5 << 10, "7B": 7, "2gb": 2_000_000_000, "3m": 3 << 20, "1KB": 1000, "1kib": 1024, "4g": 4 << 30, "0": 0,
	} {
		got, err := ParseByteSize(in)
		if err != nil || got != want {
			t.Errorf("%q = %d %v, want %d", in, got, err, want)
		}
	}
	for _, in := range []string{"", "MiB", "-1", "1.5MiB", "99999999999GiB"} {
		if _, err := ParseByteSize(in); err == nil {
			t.Errorf("%q parsed", in)
		}
	}
}

func TestParseTrustedProxy(t *testing.T) {
	p, err := ParseTrustedProxy("192.0.2.17/28")
	if err != nil || p.String() != "192.0.2.16/28" {
		t.Errorf("prefix = %v %v", p, err)
	}
	p, err = ParseTrustedProxy("::ffff:192.0.2.1")
	if err != nil || p.String() != "192.0.2.1/32" {
		t.Errorf("mapped = %v %v", p, err)
	}
	if _, err := ParseTrustedProxy("nope"); err == nil {
		t.Error("nope parsed")
	}
}

func TestSmokeConfig(t *testing.T) {
	c, err := Load(filepath.Join("..", "..", "installer", "smoke-config.yaml"))
	if err != nil {
		t.Fatalf("installer/smoke-config.yaml: %v", err)
	}
	if !c.HTTP.Enabled() || len(c.Routes) != 1 {
		t.Error("smoke config must have queue and HTTP routes")
	}
}
