//go:build windows

package tempfiles

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/vialabs-net/amqp-ftp-bridge/internal/winsvc"
	"golang.org/x/sys/windows"
)

func TestTemporaryFilesPrivate(t *testing.T) {
	dir := t.TempDir()
	owner, err := Acquire(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer owner.Close()
	path := filepath.Join(dir, "download-private.tmp")
	if err := os.WriteFile(path, []byte("private contents"), 0o600); err != nil {
		t.Fatal(err)
	}
	for i, path := range []string{dir, path} {
		sd, err := windows.GetNamedSecurityInfo(path, windows.SE_FILE_OBJECT, windows.DACL_SECURITY_INFORMATION)
		if err != nil {
			t.Fatal(err)
		}
		got := strings.Replace(sd.String(), "D:PAI", "D:P", 1)
		if i == 0 {
			if got != "D:P(A;OICI;FA;;;SY)(A;OICI;FA;;;BA)(A;OICI;FA;;;"+winsvc.SID+")" {
				t.Fatalf("directory ACL: %s", got)
			}
		} else {
			normalized := strings.ReplaceAll(strings.ReplaceAll(got, "D:AI", "D:"), ";ID;", ";;")
			if normalized != "D:(A;;FA;;;SY)(A;;FA;;;BA)(A;;FA;;;"+winsvc.SID+")" {
				t.Fatalf("file ACL: %s", got)
			}
		}
	}
	// An anonymous impersonation token has none of the permitted principals.
	advapi := windows.NewLazySystemDLL("advapi32.dll")
	impersonate := advapi.NewProc("ImpersonateAnonymousToken")
	revert := advapi.NewProc("RevertToSelf")
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	thread := windows.CurrentThread()
	ok, _, callErr := impersonate.Call(uintptr(thread))
	if ok == 0 {
		t.Fatal(callErr)
	}
	defer func() {
		ok, _, err := revert.Call()
		if ok == 0 {
			t.Fatal(err)
		}
	}()
	if f, err := os.Open(path); err == nil {
		f.Close()
		t.Fatal("unprivileged token read temporary contents")
	}
}
