package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const valid = `
data_dir: /tmp/afb
amqp:
  url: amqps://rabbitmq.example.internal:5671/files
  credential: rabbitmq
sources:
  - name: files-api
    url_prefix: https://files.example.com/api/files/
    credential: files-api
ftp:
  - name: main
    address: ftp.example.internal:21
    credential: ftp
routes:
  - queue: files.upload
    source: files-api
    target: main
    mapping:
      source_id: data._file.id
      folder: header:DestinationPath
      file_name: data._file.name
    match:
      field: event
      equals: upload
  - queue: files.legacy
    source: files-api
    target: main
    mapping:
      source_id: data.id
      folder: literal:legacy
      file_name: data.name
    match:
      field: event
      equals: upload
      otherwise: upload_field
      upload_field: data
    dead_letter:
      exchange: legacy.dlx
      routing_key: legacy.dlq
`

func TestParseDefaults(t *testing.T) {
	c, err := Parse([]byte(valid))
	if err != nil {
		t.Fatal(err)
	}
	if c.LogDir != filepath.Join("/tmp/afb", "logs") || c.TempDir != filepath.Join("/tmp/afb", "tmp") {
		t.Errorf("dirs = %q %q", c.LogDir, c.TempDir)
	}
	if c.Audit.RetentionDays != 365 || c.AMQP.ReconnectMax != 60*time.Second || c.AMQP.Heartbeat != 10*time.Second {
		t.Errorf("defaults: %+v %+v", c.Audit, c.AMQP)
	}
	if c.DeadLetter.ErrorHeader != "x-error" || c.DeadLetter.StacktraceHeader != "x-error-stacktrace" || !*c.DeadLetter.Declare {
		t.Errorf("dead letter defaults: %+v", c.DeadLetter)
	}
	r := c.Routes[0]
	if r.DeadLetter.Exchange != "files.upload_deadLetter" || r.DeadLetter.RoutingKey != "files.upload_deadLetter" {
		t.Errorf("route dead letter = %+v", r.DeadLetter)
	}
	if r.Match.Otherwise != OtherwiseDeadLetter {
		t.Errorf("otherwise = %q", r.Match.Otherwise)
	}
	if l := c.Routes[1].DeadLetter; l.Exchange != "legacy.dlx" || l.RoutingKey != "legacy.dlq" {
		t.Errorf("override = %+v", l)
	}
	if got := strings.Join(c.Credentials(), ","); got != "rabbitmq,files-api,ftp" {
		t.Errorf("credentials = %s", got)
	}
	if _, ok := c.Source("files-api"); !ok {
		t.Error("Source")
	}
	if _, ok := c.Source("nope"); ok {
		t.Error("Source(nope)")
	}
	if _, ok := c.Target("main"); !ok {
		t.Error("Target")
	}
	if _, ok := c.Target("nope"); ok {
		t.Error("Target(nope)")
	}
	if c.Sources[0].Timeout != 5*time.Minute || c.FTP[0].Timeout != time.Minute || c.Shutdown != 5*time.Minute {
		t.Error("timeouts")
	}
}

func TestValidate(t *testing.T) {
	cases := map[string]struct{ old, new, want string }{
		"amqp userinfo":   {"amqps://rabbitmq", "amqps://u:p@rabbitmq", "must not contain credentials"},
		"amqp scheme":     {"amqps://rabbitmq", "http://rabbitmq", "amqp:// or amqps://"},
		"source userinfo": {"https://files.example.com", "https://u:p@files.example.com", "url_prefix must not contain credentials"},
		"source scheme":   {"https://files.example.com", "ftp://files.example.com", "http:// or https://"},
		"ftp address":     {"ftp.example.internal:21", "ftp.example.internal", "host:port"},
		"unknown source":  {"source: files-api\n    target: main\n    mapping:\n      source_id: data._file.id", "source: other\n    target: main\n    mapping:\n      source_id: data._file.id", `unknown source "other"`},
		"otherwise":       {"otherwise: upload_field", "otherwise: drop", "dead_letter or upload_field"},
		"upload_field":    {"upload_field: data", "upload_field: \"\"", "upload_field is required"},
		"duplicate queue": {"queue: files.legacy", "queue: files.upload", "duplicate queue"},
		"missing mapping": {"file_name: data.name", "file_name: \"\"", "mapping.file_name is required"},
		"unknown field":   {"data_dir:", "data_dirx:", "field data_dirx not found"},
		"metrics":         {"data_dir: /tmp/afb", "data_dir: /tmp/afb\nmetrics:\n  listen: nope", "metrics.listen"},
		"dup source":      {"ftp:\n", "  - name: files-api\n    url_prefix: https://files.example.com/\nftp:\n", "duplicate name"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := Parse([]byte(strings.Replace(valid, tc.old, tc.new, 1)))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Errorf("err = %v, want %q", err, tc.want)
			}
		})
	}
	if _, err := Parse([]byte("amqp: {}\n")); err == nil || !strings.Contains(err.Error(), "at least one route") {
		t.Errorf("empty config: %v", err)
	}
}

func TestLoad(t *testing.T) {
	p := filepath.Join(t.TempDir(), "c.yaml")
	if err := os.WriteFile(p, []byte(valid), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(p); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(p + ".missing"); err == nil {
		t.Error("want error")
	}
	if DefaultPath() == "" || DefaultDataDir() == "" {
		t.Error("defaults empty")
	}
}

func TestExampleConfig(t *testing.T) {
	c, err := Load(filepath.Join("..", "..", "config.example.yaml"))
	if err != nil {
		t.Fatalf("config.example.yaml: %v", err)
	}
	if len(c.Routes) == 0 {
		t.Error("no routes")
	}
}
