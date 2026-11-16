// Package mapping extracts values from a message body and its AMQP headers.
//
// A selector is one of:
//
//	data._file.id     a dot-separated JSON path in the body; numeric parts index arrays
//	header:Name       an AMQP header (exact name first, then case-insensitive)
//	literal:text      a fixed value
package mapping

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
)

const (
	headerPrefix  = "header:"
	literalPrefix = "literal:"
)

// ErrNotFound is returned when a selector points to nothing.
var ErrNotFound = errors.New("not found")

// Message is a parsed message body with its headers.
type Message struct {
	body    any
	parsed  bool
	headers map[string]any
}

// Parse decodes the body as JSON. A body that is not JSON is accepted, but
// every JSON path selector on it then fails.
func Parse(body []byte, headers map[string]any) (*Message, error) {
	m := &Message{headers: headers}
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.UseNumber()
	if err := dec.Decode(&m.body); err != nil {
		return m, fmt.Errorf("body is not valid JSON: %w", err)
	}
	if err := dec.Decode(new(any)); err != io.EOF {
		return m, errors.New("body must contain exactly one JSON value")
	}
	m.parsed = true
	return m, nil
}

// String resolves selector to a string. Objects, arrays and null are errors.
func (m *Message) String(selector string) (string, error) {
	v, err := m.Value(selector)
	if err != nil {
		return "", err
	}
	return scalar(selector, v)
}

// JSON resolves selector and returns its JSON encoding. A string value is
// returned as is, without quotes.
func (m *Message) JSON(selector string) ([]byte, error) {
	v, err := m.Value(selector)
	if err != nil {
		return nil, err
	}
	switch t := v.(type) {
	case string:
		return []byte(t), nil
	case []byte:
		return t, nil
	}
	return json.Marshal(v)
}

// Value resolves selector to its raw value.
func (m *Message) Value(selector string) (any, error) {
	switch {
	case strings.HasPrefix(selector, literalPrefix):
		return strings.TrimPrefix(selector, literalPrefix), nil
	case strings.HasPrefix(selector, headerPrefix):
		name := strings.TrimPrefix(selector, headerPrefix)
		if v, ok := m.headers[name]; ok {
			return v, nil
		}
		for k, v := range m.headers {
			if strings.EqualFold(k, name) {
				return v, nil
			}
		}
		return nil, fmt.Errorf("header %q: %w", name, ErrNotFound)
	}
	if !m.parsed {
		return nil, fmt.Errorf("%s: body is not JSON", selector)
	}
	cur := m.body
	for _, part := range strings.Split(selector, ".") {
		switch node := cur.(type) {
		case map[string]any:
			v, ok := node[part]
			if !ok {
				return nil, fmt.Errorf("%s: %w", selector, ErrNotFound)
			}
			cur = v
		case []any:
			idx, err := strconv.Atoi(part)
			if err != nil || idx < 0 || idx >= len(node) {
				return nil, fmt.Errorf("%s: %w", selector, ErrNotFound)
			}
			cur = node[idx]
		default:
			return nil, fmt.Errorf("%s: %w", selector, ErrNotFound)
		}
	}
	return cur, nil
}

func scalar(selector string, v any) (string, error) {
	switch t := v.(type) {
	case string:
		return t, nil
	case []byte:
		return string(t), nil
	case json.Number:
		return t.String(), nil
	case bool:
		return strconv.FormatBool(t), nil
	case int, int8, int16, int32, int64, uint8, uint16, uint32, uint64, float32, float64:
		return fmt.Sprint(t), nil
	case nil:
		return "", fmt.Errorf("%s is null", selector)
	}
	return "", fmt.Errorf("%s is not a scalar value", selector)
}

// Matches reports whether the value at field equals want, and returns the
// value found ("" when absent).
func (m *Message) Matches(field, want string) (bool, string) {
	got, err := m.String(field)
	if err != nil {
		return false, ""
	}
	return got == want, got
}
