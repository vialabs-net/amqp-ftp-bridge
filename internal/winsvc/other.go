//go:build !windows

package winsvc

import (
	"context"
	"errors"
)

// ErrNotWindows is returned by the service commands on other systems.
var ErrNotWindows = errors.New("service commands are only available on Windows")

// IsService reports whether the process runs under the service manager.
func IsService() bool { return false }

// RunService is only available on Windows.
func RunService(func(context.Context) error) error { return ErrNotWindows }

// OpenEventLog returns a log that does nothing.
func OpenEventLog() EventLog { return nopLog{} }

// Install is only available on Windows.
func Install(exePath, configPath string) error { return ErrNotWindows }

// Uninstall is only available on Windows.
func Uninstall() error { return ErrNotWindows }

// Start is only available on Windows.
func Start() error { return ErrNotWindows }

// Stop is only available on Windows.
func Stop() error { return ErrNotWindows }

// GrantServiceLogon is only available on Windows.
func GrantServiceLogon(string) error { return ErrNotWindows }
