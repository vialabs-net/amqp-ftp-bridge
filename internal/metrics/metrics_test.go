package metrics

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func get(t *testing.T, h http.Handler, path string) (int, string) {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
	return rec.Code, rec.Body.String()
}

func TestMetricsAndHealth(t *testing.T) {
	p := New("1.2.3", "abc")
	h := p.Handler()
	if code, _ := get(t, h, "/healthz"); code != 200 {
		t.Errorf("healthz = %d", code)
	}
	if code, body := get(t, h, "/readyz"); code != 503 || !strings.Contains(body, "amqp") {
		t.Errorf("readyz before connect = %d %q", code, body)
	}
	p.AMQPConnected(true)
	if code, _ := get(t, h, "/readyz"); code != 200 {
		t.Errorf("readyz connected = %d", code)
	}
	p.FTPResult(false)
	if code, body := get(t, h, "/readyz"); code != 503 || !strings.Contains(body, "ftp") {
		t.Errorf("readyz ftp failed = %d %q", code, body)
	}
	p.FTPResult(true)

	p.Received("files.upload", true)
	p.Received("files.upload", false)
	p.InFlight("files.upload", 1)
	p.Stage("files.upload", "download", 2*time.Second, 100)
	p.Stage("files.upload", "total", 3*time.Second, 0)
	p.Error("files.upload", "download", 404)
	p.Done("files.upload", "uploaded")
	p.Done("files.upload", "dead_lettered")
	p.AMQPReconnect()
	p.AMQPConnected(false)

	_, body := get(t, h, "/metrics")
	for _, want := range []string{
		`bridge_messages_total{queue="files.upload",result="uploaded"} 1`,
		`bridge_messages_total{queue="files.upload",result="dead_lettered"} 1`,
		`bridge_redelivered_total{queue="files.upload"} 1`,
		`bridge_stage_duration_seconds_count{queue="files.upload",stage="download"} 1`,
		`bridge_bytes_total{queue="files.upload",stage="download"} 100`,
		`bridge_errors_total{code="404",queue="files.upload",stage="download"} 1`,
		`bridge_inflight{queue="files.upload"} 1`,
		`bridge_last_success_timestamp_seconds{queue="files.upload"}`,
		`bridge_amqp_connected 0`,
		`bridge_amqp_reconnects_total 1`,
		`bridge_build_info{commit="abc",version="1.2.3"} 1`,
		`go_goroutines`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("missing %s", want)
		}
	}
	if strings.Contains(body, `bridge_bytes_total{queue="files.upload",stage="total"}`) {
		t.Error("total must not count bytes")
	}
}

func TestServe(t *testing.T) {
	p := New("v", "c")
	ctx, cancel := context.WithCancel(context.Background())
	addr, errc, err := p.Serve(ctx, "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.Get("http://" + addr.String() + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if string(b) != "ok\n" {
		t.Errorf("body = %q", b)
	}
	cancel()
	select {
	case err := <-errc:
		if err != nil {
			t.Error(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("server did not stop")
	}
	if _, _, err := p.Serve(context.Background(), "256.0.0.1:bad"); err == nil {
		t.Error("want listen error")
	}
}
