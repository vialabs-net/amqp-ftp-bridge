package winsvc

import "testing"

func TestServiceSID(t *testing.T) {
	// Well-known SID of NT SERVICE\TrustedInstaller.
	if got := ServiceSID("TrustedInstaller"); got != "S-1-5-80-956008885-3418522649-1831038044-1853292631-2271478464" {
		t.Fatalf("TrustedInstaller: %s", got)
	}
	if ServiceSID("trustedinstaller") != ServiceSID("TrustedInstaller") {
		t.Fatal("service SID must ignore case")
	}
	if SID != ServiceSID(Name) {
		t.Fatal("SID must match Name")
	}
}
