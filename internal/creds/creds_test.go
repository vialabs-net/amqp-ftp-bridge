package creds

import (
	"errors"
	"strings"
	"testing"
)

func TestEnvStore(t *testing.T) {
	env := map[string]string{"AFB_CRED_FILES_API": "reader:s3cr:et", "AFB_CRED_BAD": "nocolon"}
	s := EnvStore{Lookup: func(k string) (string, bool) { v, ok := env[k]; return v, ok }}
	c, err := s.Get("files-api")
	if err != nil || c.User != "reader" || c.Password != "s3cr:et" {
		t.Errorf("Get = %+v, %v", c, err)
	}
	if _, err := s.Get("missing"); !errors.Is(err, ErrNotFound) || !strings.Contains(err.Error(), "AFB_CRED_MISSING") {
		t.Errorf("missing: %v", err)
	}
	if _, err := s.Get("bad"); err == nil {
		t.Error("bad: want error")
	}
	if _, err := s.Get("../x"); err == nil {
		t.Error("invalid name: want error")
	}
	if err := s.Set("x", Credential{}); err == nil {
		t.Error("Set must fail")
	}
}

func TestEnvStoreDefaultLookup(t *testing.T) {
	t.Setenv("AFB_CRED_RABBITMQ", "u:p")
	c, err := Default("ignored").Get("rabbitmq")
	if err != nil || c.User != "u" {
		t.Errorf("%+v %v", c, err)
	}
}

func TestValidName(t *testing.T) {
	for _, n := range []string{"rabbitmq", "files-api", "ftp_main.2"} {
		if err := ValidName(n); err != nil {
			t.Errorf("%s: %v", n, err)
		}
	}
	for _, n := range []string{"", ".hidden", "a/b", `a\b`, "a b", strings.Repeat("x", 65)} {
		if ValidName(n) == nil {
			t.Errorf("%q must be invalid", n)
		}
	}
}

func TestLoadAll(t *testing.T) {
	s := EnvStore{Lookup: func(k string) (string, bool) { return "u:p", k == "AFB_CRED_A" }}
	if m, err := LoadAll(s, []string{"a"}); err != nil || m["a"].User != "u" {
		t.Errorf("%v %v", m, err)
	}
	if _, err := LoadAll(s, []string{"a", "b"}); err == nil || !strings.Contains(err.Error(), `"b"`) {
		t.Errorf("err = %v", err)
	}
}
