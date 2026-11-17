package httpserver

import (
	"crypto/sha256"
	"crypto/subtle"
	"net"
	"net/http"
	"net/netip"
	"strings"

	"github.com/vialabs-net/amqp-ftp-bridge/internal/creds"
)

// authenticate reports whether r carries the Basic credentials of one of
// users. Every user is compared, in constant time, whatever the result.
func authenticate(r *http.Request, users []creds.Credential) bool {
	user, pass, ok := r.BasicAuth()
	if !ok {
		return false
	}
	u, p := sha256.Sum256([]byte(user)), sha256.Sum256([]byte(pass))
	match := 0
	for _, c := range users {
		cu, cp := sha256.Sum256([]byte(c.User)), sha256.Sum256([]byte(c.Password))
		match |= subtle.ConstantTimeCompare(u[:], cu[:]) & subtle.ConstantTimeCompare(p[:], cp[:])
	}
	return match == 1
}

// clientIP returns the peer address of r. When the peer is a trusted proxy,
// it returns the right-most address of X-Forwarded-For that is not a trusted
// proxy (or the left-most one when all are trusted).
func clientIP(r *http.Request, trusted []netip.Prefix) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = r.RemoteAddr
	}
	peer, err := netip.ParseAddr(host)
	if err != nil {
		return host
	}
	peer = peer.Unmap()
	if !isTrusted(peer, trusted) {
		return peer.String()
	}
	var hops []string
	for _, v := range r.Header.Values("X-Forwarded-For") {
		hops = append(hops, strings.Split(v, ",")...)
	}
	client := peer
	for i := len(hops) - 1; i >= 0; i-- {
		a, err := netip.ParseAddr(strings.TrimSpace(hops[i]))
		if err != nil {
			break
		}
		client = a.Unmap()
		if !isTrusted(client, trusted) {
			break
		}
	}
	return client.String()
}

func isTrusted(a netip.Addr, trusted []netip.Prefix) bool {
	for _, p := range trusted {
		if p.Contains(a) {
			return true
		}
	}
	return false
}
