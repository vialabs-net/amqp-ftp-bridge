//go:build windows

package winsvc

import (
	"context"
	"errors"
	"golang.org/x/sys/windows/svc"
	"os"
	"path/filepath"
	"testing"
	"time"

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

func TestStopPendingProgress(t *testing.T) {
	release := make(chan struct{})
	h := handler{run: func(ctx context.Context) error { <-ctx.Done(); <-release; return nil }}
	req := make(chan svc.ChangeRequest, 1)
	statuses := make(chan svc.Status, 10)
	done := make(chan struct{})
	go func() { h.Execute(nil, req, statuses); close(done) }()
	<-statuses
	<-statuses
	req <- svc.ChangeRequest{Cmd: svc.Stop}
	first := <-statuses
	select {
	case next := <-statuses:
		if next.State != svc.StopPending || next.CheckPoint <= first.CheckPoint || next.WaitHint == 0 {
			t.Error("no stop progress")
		}
	case <-time.After(3 * time.Second):
		t.Error("no stop progress update")
	}
	close(release)
	<-done
}

type stopFake struct {
	state     svc.State
	requested bool
	err       error
}

func (f *stopFake) Query() (svc.Status, error) {
	if f.requested {
		f.state = svc.Stopped
	}
	return svc.Status{State: f.state}, nil
}
func (f *stopFake) Control(svc.Cmd) (svc.Status, error) {
	f.requested = true
	return svc.Status{State: svc.StopPending}, f.err
}
func TestStopWaitsAndPropagatesFailure(t *testing.T) {
	f := &stopFake{state: svc.Running}
	if err := stopAndWait(f); err != nil || f.state != svc.Stopped {
		t.Fatal("stop returned before stopped", err)
	}
	f = &stopFake{state: svc.Running, err: errors.New("denied")}
	if err := stopAndWait(f); !errors.Is(err, f.err) {
		t.Fatal("stop error lost", err)
	}
}
