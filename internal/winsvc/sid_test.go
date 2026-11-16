package winsvc

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

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

// The MSI grants directory access to the service SID it holds as a constant.
func TestInstallerServiceSID(t *testing.T) {
	wxs, err := os.ReadFile(filepath.Join("..", "..", "installer", "amqp-ftp-bridge.wxs"))
	if err != nil {
		t.Fatal(err)
	}
	want := `<?define ServiceSid = "` + SID + `" ?>`
	if !strings.Contains(string(wxs), want) {
		t.Fatalf("installer must define %s", want)
	}
	if !strings.Contains(string(wxs), `<?define ServiceName = "`+Name+`" ?>`) {
		t.Fatal("installer service name must match Name")
	}
}
