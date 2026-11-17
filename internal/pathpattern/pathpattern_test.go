package pathpattern

import (
	"reflect"
	"testing"
)

func TestMatch(t *testing.T) {
	download := MustCompile("/api/file/{disk}:{dir...}/{name}")
	cases := []struct {
		p    *Pattern
		path string
		want map[string]string
	}{
		{download, "/api/file/L:/a/b/c.pdf", map[string]string{"disk": "L", "dir": "/a/b", "name": "c.pdf"}},
		{download, "/api/file/L:a/c.pdf", map[string]string{"disk": "L", "dir": "a", "name": "c.pdf"}},
		{download, "/api/file/L:/c.pdf", map[string]string{"disk": "L", "dir": "", "name": "c.pdf"}},
		{download, "/api/file/L:/a%20b/r%C3%A9sum%C3%A9.pdf", map[string]string{"disk": "L", "dir": "/a b", "name": "résumé.pdf"}},
		{download, "/api/file/L%3A/a/c.pdf", map[string]string{"disk": "L", "dir": "/a", "name": "c.pdf"}},
		{download, "/api/file/L:/a/x%2Fy", map[string]string{"disk": "L", "dir": "/a", "name": "x/y"}},
		{download, "/api/file/L:/a/100%25.txt", map[string]string{"disk": "L", "dir": "/a", "name": "100%.txt"}},
		{download, "/api/file/L:/a/../c.pdf", map[string]string{"disk": "L", "dir": "/a/..", "name": "c.pdf"}},
		{download, "/api/file/L:c.pdf", nil},
		{download, "/api/file/:/a/c.pdf", nil},
		{download, "/api/file/L:/a/", nil},
		{download, "/api/files/L:/a/c.pdf", nil},
		{MustCompile("/api/upload/"), "/api/upload/", map[string]string{}},
		{MustCompile("/api/upload/"), "/api/upload", nil},
		{MustCompile("/api/upload/"), "/api/upload/x", nil},
		{MustCompile("/x/{all...}"), "/x/", map[string]string{"all": ""}},
		{MustCompile("/x/{all...}"), "/x//a/b", map[string]string{"all": "/a/b"}},
		{MustCompile("/v/{id}"), "/v/a/b", nil},
		{MustCompile("/v/{id}.{ext}"), "/v/a.b.c", map[string]string{"id": "a", "ext": "b.c"}},
	}
	for _, c := range cases {
		got, ok := c.p.Match(c.path)
		if c.want == nil {
			if ok {
				t.Errorf("%s matched %s: %v", c.p, c.path, got)
			}
			continue
		}
		if !ok || !reflect.DeepEqual(got, c.want) {
			t.Errorf("%s on %s = %v %v, want %v", c.p, c.path, got, ok, c.want)
		}
	}
}

func TestCompileErrors(t *testing.T) {
	for _, p := range []string{
		"api/x", "/x/{a", "/x/a}", "/x/{}", "/x/{1a}", "/x/{a}/{a}", "/x/{a}{b}", "/x/{a...}{b}", "/x/{a b}",
	} {
		if _, err := Compile(p); err == nil {
			t.Errorf("%q compiled", p)
		}
	}
	defer func() {
		if recover() == nil {
			t.Error("MustCompile did not panic")
		}
	}()
	MustCompile("bad")
}

func TestNames(t *testing.T) {
	p := MustCompile("/a/{x}/{y...}")
	if !reflect.DeepEqual(p.Names(), []string{"x", "y"}) || !p.Has("y") || p.Has("z") || p.String() != "/a/{x}/{y...}" {
		t.Errorf("names = %v", p.Names())
	}
}

func TestInvalidEscapeIsKept(t *testing.T) {
	if got := normalize("/a%zz%4"); got != "/a%zz%4" {
		t.Errorf("normalize = %q", got)
	}
	if _, ok := MustCompile("/a/{x}").Match("/a/%zz"); ok {
		t.Error("invalid escape matched")
	}
}
