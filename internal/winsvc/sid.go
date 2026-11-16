package winsvc

import (
	"crypto/sha1" // #nosec G505 -- Windows defines service SIDs with SHA-1; not used for security.
	"encoding/binary"
	"fmt"
	"strings"
	"unicode/utf16"
)

// ServiceSID returns the per-service SID (NT SERVICE\<name>) in string form.
// Windows derives it from the SHA-1 of the upper-case service name in
// UTF-16LE. The service process carries it when its SID type is
// unrestricted, and it is the account of a virtual service account.
func ServiceSID(name string) string {
	u := utf16.Encode([]rune(strings.ToUpper(name)))
	b := make([]byte, 2*len(u))
	for i, c := range u {
		binary.LittleEndian.PutUint16(b[2*i:], c)
	}
	sum := sha1.Sum(b) // #nosec G401
	parts := make([]string, 5)
	for i := range parts {
		parts[i] = fmt.Sprint(binary.LittleEndian.Uint32(sum[4*i:]))
	}
	return "S-1-5-80-" + strings.Join(parts, "-")
}

// SID is the service SID of this service.
var SID = ServiceSID(Name)
