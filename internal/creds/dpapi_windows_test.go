//go:build windows

package creds

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/sys/windows"
)

func TestDPAPIStore(t *testing.T) {
	dir := t.TempDir()
	s := Default(dir)
	want := Credential{User: "reader", Password: "dpapi-test-password"}
	if err := s.Set("files", want); err != nil {
		t.Fatal(err)
	}
	got, err := s.Get("files")
	if err != nil || got != want {
		t.Fatalf("round trip: %v", err)
	}
	path := filepath.Join(dir, "credentials", "files.bin")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(data, []byte(want.User)) || bytes.Contains(data, []byte(want.Password)) || json.Valid(data) {
		t.Fatal("credential stored without encryption")
	}
	for _, path := range []string{filepath.Dir(path), path} {
		sd, err := windows.GetNamedSecurityInfo(path, windows.SE_FILE_OBJECT, windows.DACL_SECURITY_INFORMATION)
		if err != nil {
			t.Fatal(err)
		}
		if got := sd.String(); got != "D:P(A;OICI;FA;;;SY)(A;OICI;FA;;;BA)" {
			t.Errorf("unexpected credential ACL: %s", got)
		}
	}
}

func TestDPAPINoPlaintextFallback(t *testing.T) {
	dir := t.TempDir()
	s := Default(dir)
	t.Setenv(EnvVar("files"), "reader:environment-password")
	if _, err := s.Get("files"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing encrypted file must fail: %v", err)
	}
	if err := s.Set("files", Credential{User: "reader", Password: "encrypted"}); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "credentials", "files.bin")
	for _, invalid := range []string{`{"user":"reader","password":"plaintext"}`, "corrupted", ""} {
		if err := os.WriteFile(path, []byte(invalid), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := s.Get("files"); err == nil || !strings.Contains(err.Error(), "decrypt:") {
			t.Errorf("invalid encrypted file must fail closed: %v", err)
		}
	}
}
