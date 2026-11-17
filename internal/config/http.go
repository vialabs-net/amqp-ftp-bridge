package config

import (
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/netip"
	"strconv"
	"strings"
	"time"

	"go.yaml.in/yaml/v3"

	"github.com/vialabs-net/amqp-ftp-bridge/internal/pathpattern"
)

// Values for HTTPRoute.Action.
const (
	ActionUpload   = "upload"
	ActionDownload = "download"
)

// Selector prefixes of HTTP route values.
const (
	SelectHeader  = "header:"
	SelectPath    = "path:"
	SelectQuery   = "query:"
	SelectLiteral = "literal:"
)

// HTTP configures the optional synchronous HTTP listener. It is off unless
// Listen is set.
type HTTP struct {
	Listen            string        `yaml:"listen"`
	Credentials       []string      `yaml:"credentials"`
	TrustedProxies    []string      `yaml:"trusted_proxies"`
	MaxFTPSessions    int           `yaml:"max_ftp_sessions"`
	FTPWait           time.Duration `yaml:"ftp_wait"`
	MaxUploadBytes    ByteSize      `yaml:"max_upload_bytes"`
	ReadHeaderTimeout time.Duration `yaml:"read_header_timeout"`
	ReadTimeout       time.Duration `yaml:"read_timeout"`
	WriteTimeout      time.Duration `yaml:"write_timeout"`
	IdleTimeout       time.Duration `yaml:"idle_timeout"`
	Routes            []HTTPRoute   `yaml:"routes"`
}

// HTTPRoute binds a method and a path pattern to an FTP upload or download.
type HTTPRoute struct {
	Name            string            `yaml:"name"`
	Action          string            `yaml:"action"`
	Method          string            `yaml:"method"`
	Path            string            `yaml:"path"`
	FTP             string            `yaml:"ftp"`
	Folder          string            `yaml:"folder"`
	FileName        string            `yaml:"file_name"`
	FTPCredentialBy string            `yaml:"ftp_credential_by"`
	FTPCredentials  map[string]string `yaml:"ftp_credentials"`
}

// Enabled reports whether the HTTP listener is configured.
func (h HTTP) Enabled() bool { return h.Listen != "" }

// ByteSize is a number of bytes. In YAML it is an integer or a string with a
// unit: B, KB, MB, GB (powers of 1000) or KiB, MiB, GiB (powers of 1024).
type ByteSize int64

var byteUnits = []struct {
	suffix string
	factor int64
}{
	{"KIB", 1 << 10}, {"MIB", 1 << 20}, {"GIB", 1 << 30},
	{"KB", 1000}, {"MB", 1000 * 1000}, {"GB", 1000 * 1000 * 1000},
	{"K", 1 << 10}, {"M", 1 << 20}, {"G", 1 << 30},
	{"B", 1},
}

// ParseByteSize parses "200MiB", "10 MB" or "1024".
func ParseByteSize(s string) (ByteSize, error) {
	t := strings.ToUpper(strings.TrimSpace(s))
	factor := int64(1)
	for _, u := range byteUnits {
		if strings.HasSuffix(t, u.suffix) {
			factor = u.factor
			t = strings.TrimSpace(strings.TrimSuffix(t, u.suffix))
			break
		}
	}
	n, err := strconv.ParseInt(t, 10, 64)
	if err != nil || n < 0 || n > (1<<62)/factor {
		return 0, fmt.Errorf("invalid size %q", s)
	}
	return ByteSize(n * factor), nil
}

// UnmarshalYAML implements yaml.Unmarshaler.
func (b *ByteSize) UnmarshalYAML(n *yaml.Node) error {
	v, err := ParseByteSize(n.Value)
	if err != nil {
		return err
	}
	*b = v
	return nil
}

func (h *HTTP) applyDefaults() {
	if h.MaxFTPSessions == 0 {
		h.MaxFTPSessions = 8
	}
	if h.FTPWait == 0 {
		h.FTPWait = 30 * time.Second
	}
	if h.MaxUploadBytes == 0 {
		h.MaxUploadBytes = 200 << 20
	}
	if h.ReadHeaderTimeout == 0 {
		h.ReadHeaderTimeout = 10 * time.Second
	}
	if h.ReadTimeout == 0 {
		h.ReadTimeout = 10 * time.Minute
	}
	if h.WriteTimeout == 0 {
		h.WriteTimeout = 10 * time.Minute
	}
	if h.IdleTimeout == 0 {
		h.IdleTimeout = 2 * time.Minute
	}
	for i := range h.Routes {
		r := &h.Routes[i]
		if r.Method == "" {
			switch r.Action {
			case ActionUpload:
				r.Method = http.MethodPost
			case ActionDownload:
				r.Method = http.MethodGet
			}
		}
		r.Method = strings.ToUpper(r.Method)
	}
}

// ParseTrustedProxy parses an IP address or a CIDR prefix.
func ParseTrustedProxy(s string) (netip.Prefix, error) {
	if strings.Contains(s, "/") {
		p, err := netip.ParsePrefix(s)
		return p.Masked(), err
	}
	a, err := netip.ParseAddr(s)
	if err != nil {
		return netip.Prefix{}, err
	}
	a = a.Unmap()
	return netip.PrefixFrom(a, a.BitLen()), nil
}

func (h HTTP) validate(targets map[string]bool) []error {
	var errs []error
	add := func(format string, a ...any) { errs = append(errs, fmt.Errorf(format, a...)) }
	if !h.Enabled() {
		if len(h.Routes) > 0 {
			add("http.listen is required when http.routes are set")
		}
		return errs
	}
	if _, _, err := net.SplitHostPort(h.Listen); err != nil {
		add("http.listen: %v", err)
	}
	if len(h.Routes) == 0 {
		add("http.routes: at least one route is required when http.listen is set")
	}
	if len(h.Credentials) == 0 {
		add("http.credentials: at least one credential is required")
	}
	for i, name := range h.Credentials {
		if name == "" {
			add("http.credentials[%d]: credential name is required", i)
		}
	}
	for i, p := range h.TrustedProxies {
		if _, err := ParseTrustedProxy(p); err != nil {
			add("http.trusted_proxies[%d]: %q is not an IP address or CIDR prefix", i, p)
		}
	}
	if h.MaxFTPSessions < 0 {
		add("http.max_ftp_sessions must be positive")
	}
	for name, d := range map[string]time.Duration{
		"ftp_wait": h.FTPWait, "read_header_timeout": h.ReadHeaderTimeout, "read_timeout": h.ReadTimeout,
		"write_timeout": h.WriteTimeout, "idle_timeout": h.IdleTimeout,
	} {
		if d < 0 {
			add("http.%s must be positive", name)
		}
	}
	if h.MaxUploadBytes < 0 {
		add("http.max_upload_bytes must be positive")
	}
	names := map[string]bool{}
	for i, r := range h.Routes {
		errs = append(errs, r.validate(i, names, targets)...)
	}
	return errs
}

func (r HTTPRoute) validate(i int, names, targets map[string]bool) []error {
	var errs []error
	add := func(format string, a ...any) {
		errs = append(errs, fmt.Errorf("http.routes[%d]: "+format, append([]any{i}, a...)...))
	}
	switch {
	case r.Name == "":
		add("name is required")
	case names[r.Name]:
		add("duplicate name %q", r.Name)
	}
	names[r.Name] = true
	if r.Action != ActionUpload && r.Action != ActionDownload {
		add("action must be upload or download")
	}
	if r.Method == "" || strings.ContainsAny(r.Method, " \t/") {
		add("invalid method %q", r.Method)
	}
	if !targets[r.FTP] {
		add("unknown ftp target %q", r.FTP)
	}
	pattern, err := pathpattern.Compile(r.Path)
	if err != nil {
		add("path: %v", err)
	}
	selectors := map[string]string{"folder": r.Folder, "file_name": r.FileName}
	if r.FTPCredentialBy != "" {
		selectors["ftp_credential_by"] = r.FTPCredentialBy
		if len(r.FTPCredentials) == 0 {
			add("ftp_credentials is required with ftp_credential_by")
		}
	} else if len(r.FTPCredentials) > 0 {
		add("ftp_credentials needs ftp_credential_by")
	}
	var credentialKeys []string
	for key, name := range r.FTPCredentials {
		if key == "" || name == "" {
			add("ftp_credentials: empty value or credential name")
		}
		for _, previous := range credentialKeys {
			if strings.EqualFold(previous, key) {
				add("ftp_credentials: ambiguous keys %q and %q", previous, key)
			}
		}
		credentialKeys = append(credentialKeys, key)
	}
	for field, sel := range selectors {
		if err := validSelector(sel, pattern); err != nil {
			add("%s: %v", field, err)
		}
	}
	return errs
}

func validSelector(sel string, pattern *pathpattern.Pattern) error {
	switch {
	case strings.TrimSpace(sel) == "":
		return errors.New("is required")
	case strings.HasPrefix(sel, SelectLiteral):
		return nil
	case strings.HasPrefix(sel, SelectHeader), strings.HasPrefix(sel, SelectQuery):
		if _, name, _ := strings.Cut(sel, ":"); name == "" {
			return fmt.Errorf("%q has no name", sel)
		}
		return nil
	case strings.HasPrefix(sel, SelectPath):
		name := strings.TrimPrefix(sel, SelectPath)
		if pattern != nil && !pattern.Has(name) {
			return fmt.Errorf("path has no {%s}", name)
		}
		return nil
	}
	return fmt.Errorf("%q must start with one of header:, path:, query:, literal: (selector prefixes)", sel)
}
