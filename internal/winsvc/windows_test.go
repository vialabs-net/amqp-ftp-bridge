//go:build windows

package winsvc

import (
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/sys/windows/svc/mgr"
)

func TestServiceErrorRecovery(t *testing.T) {
	if os.Getenv("AFB_TEST_SCM") != "1" {
		t.Skip("requires an isolated Windows runner with service-manager access")
	}
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	if err := Install(exe, filepath.Join(t.TempDir(), "config.yaml")); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := Uninstall(); err != nil {
			t.Error(err)
		}
	}()
	m, err := mgr.Connect()
	if err != nil {
		t.Fatal(err)
	}
	defer m.Disconnect()
	s, err := m.OpenService(Name)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if enabled, err := s.RecoveryActionsOnNonCrashFailures(); err != nil || !enabled {
		t.Errorf("service errors must trigger recovery: enabled %v, %v", enabled, err)
	}
}
