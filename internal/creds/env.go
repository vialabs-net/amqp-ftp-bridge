package creds

import (
	"errors"
	"fmt"
	"os"
	"strings"
)

// EnvStore reads credentials from AFB_CRED_<NAME>="user:password". NAME is the
// credential name in upper case with every character other than a letter or
// digit replaced by '_'. It is meant for development and tests.
type EnvStore struct {
	Lookup func(string) (string, bool)
}

// EnvVar returns the environment variable that holds credential name.
func EnvVar(name string) string {
	var b strings.Builder
	b.WriteString("AFB_CRED_")
	for _, r := range strings.ToUpper(name) {
		if r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' {
			b.WriteRune(r)
		} else {
			b.WriteByte('_')
		}
	}
	return b.String()
}

// Get implements Store.
func (s EnvStore) Get(name string) (Credential, error) {
	if err := ValidName(name); err != nil {
		return Credential{}, err
	}
	lookup := s.Lookup
	if lookup == nil {
		lookup = os.LookupEnv
	}
	v, ok := lookup(EnvVar(name))
	if !ok {
		return Credential{}, fmt.Errorf("%w: set %s", ErrNotFound, EnvVar(name))
	}
	user, pass, ok := strings.Cut(v, ":")
	if !ok {
		return Credential{}, fmt.Errorf("%s must be user:password", EnvVar(name))
	}
	return Credential{User: user, Password: pass}, nil
}

// Set implements Store. The environment store is read-only.
func (s EnvStore) Set(name string, _ Credential) error {
	return errors.New("this build reads credentials from the environment: set " + EnvVar(name) + "=user:password")
}
