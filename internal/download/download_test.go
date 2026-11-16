package download

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestFetch(t *testing.T) {
	content := strings.Repeat("payload-", 10000)
	var gotPath, gotAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath, gotAuth = r.URL.EscapedPath(), r.Header.Get("Authorization")
		switch {
		case strings.HasSuffix(r.URL.Path, "/missing"):
			http.NotFound(w, r)
		case strings.HasSuffix(r.URL.Path, "/short"):
			w.Header().Set("Content-Length", "100")
			_, _ = w.Write([]byte("abc"))
		default:
			_, _ = w.Write([]byte(content))
		}
	}))
	defer srv.Close()

	tmp := t.TempDir()
	c := New(srv.URL+"/api/files/", "reader", "secret", filepath.Join(tmp, "sub"), 5*time.Second)
	res, err := c.Fetch(context.Background(), "id 1/2")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(res.Path)
	if gotPath != "/api/files/id%201%2F2" {
		t.Errorf("path = %s", gotPath)
	}
	if !strings.HasPrefix(gotAuth, "Basic ") {
		t.Errorf("auth = %q", gotAuth)
	}
	sum := sha256.Sum256([]byte(content))
	if res.Status != 200 || res.Bytes != int64(len(content)) || res.SHA256 != hex.EncodeToString(sum[:]) || res.Host != strings.TrimPrefix(srv.URL, "http://") {
		t.Errorf("res = %+v", res)
	}
	if b, _ := os.ReadFile(res.Path); string(b) != content {
		t.Error("content mismatch")
	}

	_, err = c.Fetch(context.Background(), "missing")
	var se *StatusError
	if !errors.As(err, &se) || se.Status != 404 || !strings.Contains(err.Error(), "HTTP 404") {
		t.Errorf("missing: %v", err)
	}
	if _, err := c.Fetch(context.Background(), "short"); err == nil {
		t.Error("short body: want error")
	}
	for _, id := range []string{"", ".", ".."} {
		if _, err := c.Fetch(context.Background(), id); err == nil {
			t.Errorf("id %q: want error", id)
		}
	}
	entries, _ := os.ReadDir(filepath.Join(tmp, "sub"))
	if len(entries) != 1 {
		t.Errorf("temp files left: %d", len(entries))
	}
}

func TestFetchConnectionError(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	url := srv.URL
	srv.Close()
	c := New(url+"/", "", "", t.TempDir(), time.Second)
	_, err := c.Fetch(context.Background(), "x")
	if err == nil || strings.Contains(err.Error(), url+"/x") {
		t.Errorf("err = %v (must not hold the full URL)", err)
	}
	if _, err := c.Check(context.Background()); err == nil {
		t.Error("check: want error")
	}
}

func TestCheck(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodHead {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		if _, _, ok := r.BasicAuth(); !ok {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()
	st, err := New(srv.URL+"/", "u", "p", t.TempDir(), time.Second).Check(context.Background())
	if err != nil || st != 404 {
		t.Errorf("check = %d, %v", st, err)
	}
	st, _ = New(srv.URL+"/", "", "", t.TempDir(), time.Second).Check(context.Background())
	if st != 401 {
		t.Errorf("no auth = %d", st)
	}
	if New("::bad", "", "", "", time.Second).Host() != "" {
		t.Error("bad host")
	}
}

func TestStripURL(t *testing.T) {
	if got := stripURL(errors.New("plain")); got.Error() != "plain" {
		t.Error(got)
	}
}

func TestSourceIDCannotChangeAuthority(t *testing.T) {
	for _, prefix := range []string{"https://files.example.com", "https://files.example.com/", "https://files.example.com/api/"} {
		for _, id := range []string{"@attacker.example", ":123@attacker.example", "//attacker.example/a", "a?token=secret#fragment", "a/b"} {
			got, err := New(prefix, "reader", "secret", "", time.Second).URL(id)
			if err != nil {
				t.Fatal(err)
			}
			u, err := url.Parse(got)
			if err != nil || u.Host != "files.example.com" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
				t.Errorf("source id changed authority or query: %q, %v", got, err)
			}
		}
	}
}
