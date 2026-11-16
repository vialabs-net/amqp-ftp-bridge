package mapping

import (
	"errors"
	"testing"
)

const body = `{"event":"upload","n":42,"ok":true,"nil":null,"data":{"_file":{"id":"abc-1","name":"a.pdf"},"items":[{"id":"x"},{"id":"y"}]}}`

func TestString(t *testing.T) {
	m, err := Parse([]byte(body), map[string]any{"DestinationPath": "inbox/2026", "Count": int32(7), "Raw": []byte("bytes")})
	if err != nil {
		t.Fatal(err)
	}
	cases := map[string]string{
		"data._file.id":          "abc-1",
		"data._file.name":        "a.pdf",
		"data.items.1.id":        "y",
		"event":                  "upload",
		"n":                      "42",
		"ok":                     "true",
		"header:DestinationPath": "inbox/2026",
		"header:destinationpath": "inbox/2026",
		"header:Count":           "7",
		"header:Raw":             "bytes",
		"literal:/fixed/folder":  "/fixed/folder",
	}
	for sel, want := range cases {
		got, err := m.String(sel)
		if err != nil || got != want {
			t.Errorf("%s = %q, %v; want %q", sel, got, err, want)
		}
	}
}

func TestStringErrors(t *testing.T) {
	m, _ := Parse([]byte(body), nil)
	for _, sel := range []string{"missing", "data._file.missing", "data.items.5.id", "data.items.x", "event.deeper", "header:Nope"} {
		if _, err := m.String(sel); !errors.Is(err, ErrNotFound) {
			t.Errorf("%s: err = %v, want ErrNotFound", sel, err)
		}
	}
	for _, sel := range []string{"nil", "data", "data.items"} {
		if _, err := m.String(sel); err == nil {
			t.Errorf("%s: want error for non-scalar", sel)
		}
	}
}

func TestNotJSON(t *testing.T) {
	m, err := Parse([]byte("plain text"), map[string]any{"H": "v"})
	if err == nil {
		t.Fatal("want parse error")
	}
	if _, err := m.String("a.b"); err == nil {
		t.Error("json path on non-JSON body must fail")
	}
	if v, err := m.String("header:H"); err != nil || v != "v" {
		t.Errorf("header on non-JSON body = %q, %v", v, err)
	}
}

func TestJSON(t *testing.T) {
	m, _ := Parse([]byte(body), map[string]any{"B": []byte("raw")})
	got, err := m.JSON("data._file")
	if err != nil || string(got) != `{"id":"abc-1","name":"a.pdf"}` {
		t.Errorf("JSON(data._file) = %s, %v", got, err)
	}
	if got, _ := m.JSON("event"); string(got) != "upload" {
		t.Errorf("JSON(event) = %s", got)
	}
	if got, _ := m.JSON("header:B"); string(got) != "raw" {
		t.Errorf("JSON(header:B) = %s", got)
	}
	if _, err := m.JSON("missing"); err == nil {
		t.Error("want error")
	}
}

func TestMatches(t *testing.T) {
	m, _ := Parse([]byte(body), map[string]any{"Kind": "upload"})
	if ok, v := m.Matches("event", "upload"); !ok || v != "upload" {
		t.Errorf("Matches(event) = %v, %q", ok, v)
	}
	if ok, v := m.Matches("event", "delete"); ok || v != "upload" {
		t.Errorf("Matches(event, delete) = %v, %q", ok, v)
	}
	if ok, v := m.Matches("missing", "x"); ok || v != "" {
		t.Errorf("Matches(missing) = %v, %q", ok, v)
	}
	if ok, _ := m.Matches("header:Kind", "upload"); !ok {
		t.Error("Matches(header:Kind)")
	}
}
