//go:build windows

package creds

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"unsafe"

	"github.com/vialabs-net/amqp-ftp-bridge/internal/winsvc"
	"golang.org/x/sys/windows"
)

// aclSDDL grants full control to SYSTEM and Administrators and read access
// to the service SID (NT SERVICE\amqp-ftp-bridge), with inheritance cut
// from the parent and passed down to new files. The service SID lets the
// service read credentials when it runs under a virtual or other account.
var aclSDDL = "D:P(A;OICI;FA;;;SY)(A;OICI;FA;;;BA)(A;OICI;FR;;;" + winsvc.SID + ")"

// DPAPIStore keeps credentials encrypted with DPAPI, machine scope, in
// <Dir>/<name>.bin.
type DPAPIStore struct {
	Dir string
}

// Default returns the DPAPI store in <dataDir>/credentials.
func Default(dataDir string) Store { return DPAPIStore{Dir: filepath.Join(dataDir, "credentials")} }

// Secure migrates protected credential ACLs from versions without a service
// SID. It requires an administrator and leaves encrypted contents unchanged.
func Secure(dataDir string) error {
	dir := filepath.Join(dataDir, "credentials")
	entries, err := os.ReadDir(dir)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if err := restrict(dir); err != nil {
		return err
	}
	for _, entry := range entries {
		if !strings.HasSuffix(entry.Name(), ".bin") {
			continue
		}
		if err := ValidName(strings.TrimSuffix(entry.Name(), ".bin")); err != nil {
			return err
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("credential %q is not a regular file", entry.Name())
		}
		if err := restrict(filepath.Join(dir, entry.Name())); err != nil {
			return err
		}
	}
	return nil
}

// Get implements Store.
func (s DPAPIStore) Get(name string) (Credential, error) {
	if err := ValidName(name); err != nil {
		return Credential{}, err
	}
	data, err := os.ReadFile(filepath.Join(s.Dir, name+".bin"))
	if errors.Is(err, fs.ErrNotExist) {
		return Credential{}, fmt.Errorf("%w: run 'amqp-ftp-bridge creds set %s'", ErrNotFound, name)
	}
	if err != nil {
		return Credential{}, err
	}
	plain, err := unprotect(data)
	if err != nil {
		return Credential{}, fmt.Errorf("decrypt: %w", err)
	}
	var c Credential
	if err := json.Unmarshal(plain, &c); err != nil {
		return Credential{}, fmt.Errorf("decode: %w", err)
	}
	return c, nil
}

// Set implements Store. It restricts the directory and the file to SYSTEM
// and Administrators, with read access for the service.
func (s DPAPIStore) Set(name string, c Credential) error {
	if err := ValidName(name); err != nil {
		return err
	}
	if err := os.MkdirAll(s.Dir, 0o700); err != nil {
		return err
	}
	if err := restrict(s.Dir); err != nil {
		return fmt.Errorf("set ACL on %s: %w", s.Dir, err)
	}
	plain, err := json.Marshal(c)
	if err != nil {
		return err
	}
	enc, err := protect(plain)
	if err != nil {
		return fmt.Errorf("encrypt: %w", err)
	}
	path := filepath.Join(s.Dir, name+".bin")
	if err := os.WriteFile(path, enc, 0o600); err != nil {
		return err
	}
	if err := restrict(path); err != nil {
		return fmt.Errorf("set ACL on %s: %w", path, err)
	}
	return nil
}

func restrict(path string) error {
	sd, err := windows.SecurityDescriptorFromString(aclSDDL)
	if err != nil {
		return err
	}
	dacl, _, err := sd.DACL()
	if err != nil {
		return err
	}
	return windows.SetNamedSecurityInfo(path, windows.SE_FILE_OBJECT,
		windows.DACL_SECURITY_INFORMATION|windows.PROTECTED_DACL_SECURITY_INFORMATION,
		nil, nil, dacl, nil)
}

func blob(b []byte) *windows.DataBlob {
	if len(b) == 0 {
		return &windows.DataBlob{}
	}
	return &windows.DataBlob{Size: uint32(len(b)), Data: &b[0]}
}

func takeBlob(out *windows.DataBlob) []byte {
	defer windows.LocalFree(windows.Handle(unsafe.Pointer(out.Data)))
	return append([]byte(nil), unsafe.Slice(out.Data, out.Size)...)
}

func protect(plain []byte) ([]byte, error) {
	var out windows.DataBlob
	err := windows.CryptProtectData(blob(plain), nil, nil, 0, nil,
		windows.CRYPTPROTECT_LOCAL_MACHINE|windows.CRYPTPROTECT_UI_FORBIDDEN, &out)
	if err != nil {
		return nil, err
	}
	return takeBlob(&out), nil
}

func unprotect(enc []byte) ([]byte, error) {
	var out windows.DataBlob
	err := windows.CryptUnprotectData(blob(enc), nil, nil, 0, nil, windows.CRYPTPROTECT_UI_FORBIDDEN, &out)
	if err != nil {
		return nil, err
	}
	return takeBlob(&out), nil
}
