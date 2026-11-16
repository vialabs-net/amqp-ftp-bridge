//go:build !windows

package creds

// Default returns the environment store. dataDir is not used.
func Default(dataDir string) Store { return EnvStore{} }
