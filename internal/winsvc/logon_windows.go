//go:build windows

package winsvc

import (
	"fmt"
	"strings"
	"unsafe"

	"golang.org/x/sys/windows"
)

// GrantServiceLogon gives an existing account the right needed by the MSI's
// service. The right is retained on uninstall because other services may use it.
func GrantServiceLogon(account string) error {
	if strings.EqualFold(account, `NT SERVICE\`+Name) || strings.EqualFold(account, "LocalSystem") {
		return nil
	}
	sid, _, _, err := windows.LookupSID("", account)
	if err != nil {
		return fmt.Errorf("look up service account: %w", err)
	}
	right, err := windows.NewNTUnicodeString("SeServiceLogonRight")
	if err != nil {
		return err
	}
	advapi := windows.NewLazySystemDLL("advapi32.dll")
	statusError := func(status uintptr) error {
		if status == 0 {
			return nil
		}
		code, _, _ := advapi.NewProc("LsaNtStatusToWinError").Call(status)
		return windows.Errno(code)
	}
	var attributes windows.OBJECT_ATTRIBUTES
	attributes.Length = uint32(unsafe.Sizeof(attributes))
	var policy windows.Handle
	// POLICY_CREATE_ACCOUNT | POLICY_LOOKUP_NAMES.
	status, _, _ := advapi.NewProc("LsaOpenPolicy").Call(0, uintptr(unsafe.Pointer(&attributes)), 0x810, uintptr(unsafe.Pointer(&policy)))
	if err := statusError(status); err != nil {
		return fmt.Errorf("open local security policy: %w", err)
	}
	defer advapi.NewProc("LsaClose").Call(uintptr(policy))
	status, _, _ = advapi.NewProc("LsaAddAccountRights").Call(uintptr(policy), uintptr(unsafe.Pointer(sid)), uintptr(unsafe.Pointer(right)), 1)
	if err := statusError(status); err != nil {
		return fmt.Errorf("grant Log on as a service: %w", err)
	}
	return nil
}
