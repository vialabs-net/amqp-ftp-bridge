// Package creds stores and reads named credentials.
//
// On Windows they are encrypted with DPAPI (machine scope) in
// <data_dir>/credentials/<name>.bin. Elsewhere, for development and tests
// only, they are read from the environment variable AFB_CRED_<NAME> with the
// value "user:password".
package creds

import (
	"errors"
	"fmt"
	"strings"
)

// Credential is a user name and password.
type Credential struct {
	User     string `json:"user"`
	Password string `json:"password"`
}

// Store reads and writes credentials by name.
type Store interface {
	Get(name string) (Credential, error)
	Set(name string, c Credential) error
}

// ErrNotFound is returned when a credential does not exist.
var ErrNotFound = errors.New("credential not found")

// ValidName reports whether name can be used as a credential name: letters,
// digits, '-', '_' and '.', not starting with '.'.
func ValidName(name string) error {
	if name == "" || len(name) > 64 || strings.HasPrefix(name, ".") {
		return fmt.Errorf("invalid credential name %q", name)
	}
	for _, r := range name {
		if !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '-' || r == '_' || r == '.') {
			return fmt.Errorf("invalid credential name %q", name)
		}
	}
	return nil
}

// LoadAll reads every named credential and fails on the first one that
// cannot be read.
func LoadAll(s Store, names []string) (map[string]Credential, error) {
	out := make(map[string]Credential, len(names))
	for _, n := range names {
		c, err := s.Get(n)
		if err != nil {
			return nil, fmt.Errorf("credential %q: %w", n, err)
		}
		out[n] = c
	}
	return out, nil
}
