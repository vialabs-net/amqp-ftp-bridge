// Package pathpattern matches URL paths against patterns with named values.
//
// A pattern is a path that starts with "/". It may hold:
//
//	{name}     text inside one segment, at least one character; it may share
//	           the segment with literal text ("{disk}:")
//	{name...}  any text, across "/"; it may be empty or start with "/"
//
// Two values must be separated by literal text. Matching works on the
// escaped path, so an encoded "/" (%2F) never splits a segment. Values are
// URL-decoded.
package pathpattern

import (
	"fmt"
	"net/url"
	"regexp"
	"strings"
)

// Pattern is a compiled path pattern.
type Pattern struct {
	raw   string
	re    *regexp.Regexp
	names []string
}

var nameRE = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// Compile parses a pattern.
func Compile(pattern string) (*Pattern, error) {
	if !strings.HasPrefix(pattern, "/") {
		return nil, fmt.Errorf("path pattern %q must start with /", pattern)
	}
	var re strings.Builder
	re.WriteString("^")
	p := &Pattern{raw: pattern}
	seen := map[string]bool{}
	lastWasVar := false
	rest := pattern
	for rest != "" {
		open := strings.IndexAny(rest, "{}")
		if open < 0 {
			re.WriteString(regexp.QuoteMeta(normalize(rest)))
			break
		}
		if rest[open] == '}' {
			return nil, fmt.Errorf("path pattern %q: unexpected }", pattern)
		}
		if open > 0 {
			re.WriteString(regexp.QuoteMeta(normalize(rest[:open])))
			lastWasVar = false
		}
		end := strings.IndexByte(rest[open:], '}')
		if end < 0 {
			return nil, fmt.Errorf("path pattern %q: missing }", pattern)
		}
		name := rest[open+1 : open+end]
		multi := strings.HasSuffix(name, "...")
		name = strings.TrimSuffix(name, "...")
		switch {
		case !nameRE.MatchString(name):
			return nil, fmt.Errorf("path pattern %q: invalid name %q", pattern, name)
		case seen[name]:
			return nil, fmt.Errorf("path pattern %q: duplicate name %q", pattern, name)
		case lastWasVar:
			return nil, fmt.Errorf("path pattern %q: {%s} must be separated from the previous value by literal text", pattern, name)
		}
		seen[name] = true
		p.names = append(p.names, name)
		if multi {
			re.WriteString("(.*)")
		} else {
			re.WriteString("([^/]+?)")
		}
		lastWasVar = true
		rest = rest[open+end+1:]
	}
	re.WriteString("$")
	compiled, err := regexp.Compile(re.String())
	if err != nil {
		return nil, fmt.Errorf("path pattern %q: %w", pattern, err)
	}
	p.re = compiled
	return p, nil
}

// MustCompile is Compile that panics on error, for tests and constants.
func MustCompile(pattern string) *Pattern {
	p, err := Compile(pattern)
	if err != nil {
		panic(err)
	}
	return p
}

// String returns the pattern source.
func (p *Pattern) String() string { return p.raw }

// Names returns the value names in pattern order.
func (p *Pattern) Names() []string { return append([]string(nil), p.names...) }

// Has reports whether the pattern defines name.
func (p *Pattern) Has(name string) bool {
	for _, n := range p.names {
		if n == name {
			return true
		}
	}
	return false
}

// Match matches an escaped URL path (url.URL.EscapedPath). It returns the
// decoded values, or ok=false when the path does not match or a value cannot
// be decoded.
func (p *Pattern) Match(escapedPath string) (map[string]string, bool) {
	m := p.re.FindStringSubmatch(normalize(escapedPath))
	if m == nil {
		return nil, false
	}
	values := make(map[string]string, len(p.names))
	for i, name := range p.names {
		v, err := url.PathUnescape(m[i+1])
		if err != nil {
			return nil, false
		}
		values[name] = v
	}
	return values, true
}

// normalize decodes every percent escape except %2F and %25, so that literal
// text matches whether or not the client encoded it, while an encoded "/"
// stays inside its segment. Invalid escapes are left as they are.
func normalize(s string) string {
	if !strings.Contains(s, "%") {
		return s
	}
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		if s[i] == '%' && i+2 < len(s) && isHex(s[i+1]) && isHex(s[i+2]) {
			c := unhex(s[i+1])<<4 | unhex(s[i+2])
			if c == '/' || c == '%' {
				b.WriteByte('%')
				b.WriteString(strings.ToUpper(s[i+1 : i+3]))
			} else {
				b.WriteByte(c)
			}
			i += 2
			continue
		}
		b.WriteByte(s[i])
	}
	return b.String()
}

func isHex(c byte) bool {
	return '0' <= c && c <= '9' || 'a' <= c && c <= 'f' || 'A' <= c && c <= 'F'
}

func unhex(c byte) byte {
	switch {
	case '0' <= c && c <= '9':
		return c - '0'
	case 'a' <= c && c <= 'f':
		return c - 'a' + 10
	}
	return c - 'A' + 10
}
