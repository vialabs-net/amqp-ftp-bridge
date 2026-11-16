//go:build !windows

package creds

import "errors"

// Default returns the environment store. dataDir is not used.
func Default(dataDir string) Store { return EnvStore{} }

// Secure is only needed for Windows DPAPI credentials.
func Secure(string) error { return errors.New("credential ACL migration is only available on Windows") }
