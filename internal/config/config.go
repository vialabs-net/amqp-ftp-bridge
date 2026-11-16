// Package config loads and validates the YAML configuration.
package config

import (
	"bytes"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"go.yaml.in/yaml/v3"
)

// Values for Match.Otherwise.
const (
	OtherwiseDeadLetter  = "dead_letter"
	OtherwiseUploadField = "upload_field"
)

// Config is the whole configuration file.
type Config struct {
	DataDir    string        `yaml:"data_dir"`
	LogDir     string        `yaml:"log_dir"`
	TempDir    string        `yaml:"temp_dir"`
	Audit      Audit         `yaml:"audit"`
	AMQP       AMQP          `yaml:"amqp"`
	DeadLetter DeadLetter    `yaml:"dead_letter"`
	Metrics    Metrics       `yaml:"metrics"`
	Sources    []Source      `yaml:"sources"`
	FTP        []FTP         `yaml:"ftp"`
	Routes     []Route       `yaml:"routes"`
	Shutdown   time.Duration `yaml:"shutdown_timeout"`
}

// Audit configures the audit log.
type Audit struct {
	RetentionDays int `yaml:"retention_days"`
}

// AMQP configures the RabbitMQ connection.
type AMQP struct {
	URL          string        `yaml:"url"`
	Credential   string        `yaml:"credential"`
	Heartbeat    time.Duration `yaml:"heartbeat"`
	ReconnectMax time.Duration `yaml:"reconnect_max"`
}

// DeadLetter holds the defaults for dead-letter publishing.
type DeadLetter struct {
	ExchangeSuffix   string `yaml:"exchange_suffix"`
	RoutingKeySuffix string `yaml:"routing_key_suffix"`
	ErrorHeader      string `yaml:"error_header"`
	StacktraceHeader string `yaml:"stacktrace_header"`
	Declare          *bool  `yaml:"declare"`
}

// Metrics configures the Prometheus endpoint.
type Metrics struct {
	Listen string `yaml:"listen"`
}

// Source is an HTTP download source.
type Source struct {
	Name       string        `yaml:"name"`
	URLPrefix  string        `yaml:"url_prefix"`
	Credential string        `yaml:"credential"`
	Timeout    time.Duration `yaml:"timeout"`
}

// FTP is an FTP upload target.
type FTP struct {
	Name       string        `yaml:"name"`
	Address    string        `yaml:"address"`
	Credential string        `yaml:"credential"`
	Timeout    time.Duration `yaml:"timeout"`
}

// Route binds one queue to one source and one FTP target.
type Route struct {
	Queue      string          `yaml:"queue"`
	Source     string          `yaml:"source"`
	Target     string          `yaml:"target"`
	Mapping    Mapping         `yaml:"mapping"`
	Match      *Match          `yaml:"match"`
	DeadLetter RouteDeadLetter `yaml:"dead_letter"`
}

// Mapping says where each value comes from.
type Mapping struct {
	SourceID string `yaml:"source_id"`
	Folder   string `yaml:"folder"`
	FileName string `yaml:"file_name"`
}

// Match is an optional filter on one field of the message.
type Match struct {
	Field       string `yaml:"field"`
	Equals      string `yaml:"equals"`
	Otherwise   string `yaml:"otherwise"`
	UploadField string `yaml:"upload_field"`
}

// RouteDeadLetter overrides the dead-letter exchange and routing key of a route.
type RouteDeadLetter struct {
	Exchange   string `yaml:"exchange"`
	RoutingKey string `yaml:"routing_key"`
}

// DefaultPath returns the default configuration file path.
func DefaultPath() string {
	return filepath.Join(DefaultDataDir(), "config.yaml")
}

// DefaultDataDir returns %ProgramData%\amqp-ftp-bridge on Windows and
// /var/lib/amqp-ftp-bridge elsewhere.
func DefaultDataDir() string {
	if runtime.GOOS == "windows" {
		pd := os.Getenv("ProgramData")
		if pd == "" {
			pd = `C:\ProgramData`
		}
		return filepath.Join(pd, "amqp-ftp-bridge")
	}
	return "/var/lib/amqp-ftp-bridge"
}

// Load reads, defaults and validates the file at path.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	return Parse(data)
}

// Parse decodes, defaults and validates YAML data.
func Parse(data []byte) (*Config, error) {
	var c Config
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	if err := dec.Decode(&c); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	c.applyDefaults()
	if err := c.Validate(); err != nil {
		return nil, err
	}
	return &c, nil
}

func (c *Config) applyDefaults() {
	if c.DataDir == "" {
		c.DataDir = DefaultDataDir()
	}
	if c.LogDir == "" {
		c.LogDir = filepath.Join(c.DataDir, "logs")
	}
	if c.TempDir == "" {
		c.TempDir = filepath.Join(c.DataDir, "tmp")
	}
	if c.Audit.RetentionDays == 0 {
		c.Audit.RetentionDays = 365
	}
	if c.AMQP.Heartbeat == 0 {
		c.AMQP.Heartbeat = 10 * time.Second
	}
	if c.AMQP.ReconnectMax == 0 {
		c.AMQP.ReconnectMax = 60 * time.Second
	}
	if c.DeadLetter.ExchangeSuffix == "" {
		c.DeadLetter.ExchangeSuffix = "_deadLetter"
	}
	if c.DeadLetter.RoutingKeySuffix == "" {
		c.DeadLetter.RoutingKeySuffix = "_deadLetter"
	}
	if c.DeadLetter.ErrorHeader == "" {
		c.DeadLetter.ErrorHeader = "x-error"
	}
	if c.DeadLetter.StacktraceHeader == "" {
		c.DeadLetter.StacktraceHeader = "x-error-stacktrace"
	}
	if c.DeadLetter.Declare == nil {
		t := true
		c.DeadLetter.Declare = &t
	}
	if c.Shutdown == 0 {
		c.Shutdown = 5 * time.Minute
	}
	for i := range c.Sources {
		if c.Sources[i].Timeout == 0 {
			c.Sources[i].Timeout = 5 * time.Minute
		}
	}
	for i := range c.FTP {
		if c.FTP[i].Timeout == 0 {
			c.FTP[i].Timeout = time.Minute
		}
	}
	for i := range c.Routes {
		r := &c.Routes[i]
		if r.Match != nil && r.Match.Otherwise == "" {
			r.Match.Otherwise = OtherwiseDeadLetter
		}
		if r.DeadLetter.Exchange == "" {
			r.DeadLetter.Exchange = r.Queue + c.DeadLetter.ExchangeSuffix
		}
		if r.DeadLetter.RoutingKey == "" {
			r.DeadLetter.RoutingKey = r.Queue + c.DeadLetter.RoutingKeySuffix
		}
	}
}

// Validate checks the configuration and returns all problems found.
func (c *Config) Validate() error {
	var errs []error
	add := func(format string, a ...any) { errs = append(errs, fmt.Errorf(format, a...)) }

	if c.AMQP.URL == "" {
		add("amqp.url is required")
	} else if u, err := url.Parse(c.AMQP.URL); err != nil || (u.Scheme != "amqp" && u.Scheme != "amqps") {
		add("amqp.url must be an amqp:// or amqps:// URL")
	} else if u.User != nil {
		add("amqp.url must not contain credentials: use amqp.credential")
	}
	if c.AMQP.Credential == "" {
		add("amqp.credential is required")
	}
	if c.Audit.RetentionDays < 0 {
		add("audit.retention_days must be positive")
	}
	if c.Metrics.Listen != "" {
		if _, _, err := net.SplitHostPort(c.Metrics.Listen); err != nil {
			add("metrics.listen: %v", err)
		}
	}

	sources := map[string]bool{}
	for i, s := range c.Sources {
		switch {
		case s.Name == "":
			add("sources[%d].name is required", i)
		case sources[s.Name]:
			add("sources[%d]: duplicate name %q", i, s.Name)
		}
		sources[s.Name] = true
		u, err := url.Parse(s.URLPrefix)
		switch {
		case s.URLPrefix == "" || err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "":
			add("sources[%d].url_prefix must be an http:// or https:// URL", i)
		case u.User != nil:
			add("sources[%d].url_prefix must not contain credentials: use credential", i)
		}
	}

	targets := map[string]bool{}
	for i, f := range c.FTP {
		switch {
		case f.Name == "":
			add("ftp[%d].name is required", i)
		case targets[f.Name]:
			add("ftp[%d]: duplicate name %q", i, f.Name)
		}
		targets[f.Name] = true
		if _, _, err := net.SplitHostPort(f.Address); err != nil {
			add("ftp[%d].address must be host:port", i)
		}
		if f.Credential == "" {
			add("ftp[%d].credential is required", i)
		}
	}

	if len(c.Routes) == 0 {
		add("at least one route is required")
	}
	queues := map[string]bool{}
	for i, r := range c.Routes {
		errs = append(errs, r.validate(i, queues, sources, targets)...)
	}
	return errors.Join(errs...)
}

func (r Route) validate(i int, queues, sources, targets map[string]bool) []error {
	var errs []error
	add := func(format string, a ...any) {
		errs = append(errs, fmt.Errorf("routes[%d]: "+format, append([]any{i}, a...)...))
	}
	switch {
	case r.Queue == "":
		add("queue is required")
	case queues[r.Queue]:
		add("duplicate queue %q", r.Queue)
	}
	queues[r.Queue] = true
	if !sources[r.Source] {
		add("unknown source %q", r.Source)
	}
	if !targets[r.Target] {
		add("unknown ftp target %q", r.Target)
	}
	for name, v := range map[string]string{"source_id": r.Mapping.SourceID, "folder": r.Mapping.Folder, "file_name": r.Mapping.FileName} {
		if strings.TrimSpace(v) == "" {
			add("mapping.%s is required", name)
		}
	}
	if m := r.Match; m != nil {
		if m.Field == "" {
			add("match.field is required")
		}
		switch m.Otherwise {
		case OtherwiseDeadLetter:
		case OtherwiseUploadField:
			if m.UploadField == "" {
				add("match.upload_field is required when otherwise is upload_field")
			}
		default:
			add("match.otherwise must be dead_letter or upload_field")
		}
	}
	return errs
}

// Credentials returns the names of every credential the configuration uses.
func (c *Config) Credentials() []string {
	seen := map[string]bool{}
	var out []string
	add := func(n string) {
		if n != "" && !seen[n] {
			seen[n] = true
			out = append(out, n)
		}
	}
	add(c.AMQP.Credential)
	for _, s := range c.Sources {
		add(s.Credential)
	}
	for _, f := range c.FTP {
		add(f.Credential)
	}
	return out
}

// Source returns the source with the given name.
func (c *Config) Source(name string) (Source, bool) {
	for _, s := range c.Sources {
		if s.Name == name {
			return s, true
		}
	}
	return Source{}, false
}

// Target returns the FTP target with the given name.
func (c *Config) Target(name string) (FTP, bool) {
	for _, f := range c.FTP {
		if f.Name == name {
			return f, true
		}
	}
	return FTP{}, false
}
