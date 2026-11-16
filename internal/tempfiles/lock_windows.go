//go:build windows

package tempfiles

import (
	"os"

	"github.com/vialabs-net/amqp-ftp-bridge/internal/winsvc"
	"golang.org/x/sys/windows"
)

func lock(f *os.File) error {
	return windows.LockFileEx(windows.Handle(f.Fd()), windows.LOCKFILE_EXCLUSIVE_LOCK|windows.LOCKFILE_FAIL_IMMEDIATELY, 0, 1, 0, &windows.Overlapped{})
}

// A Go file mode does not restrict a Windows DACL. Protect downloads and
// upload_field contents before creating any temporary file. SYSTEM,
// Administrators and the service SID (NT SERVICE\amqp-ftp-bridge) only.
func secureDir(dir string) error {
	sd, err := windows.SecurityDescriptorFromString("D:P(A;OICI;FA;;;SY)(A;OICI;FA;;;BA)(A;OICI;FA;;;" + winsvc.SID + ")")
	if err != nil {
		return err
	}
	dacl, _, err := sd.DACL()
	if err != nil {
		return err
	}
	return windows.SetNamedSecurityInfo(dir, windows.SE_FILE_OBJECT,
		windows.DACL_SECURITY_INFORMATION|windows.PROTECTED_DACL_SECURITY_INFORMATION,
		nil, nil, dacl, nil)
}
